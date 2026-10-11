import { randomUUID } from "node:crypto";
import { afterAll, describe, expect, it } from "vitest";
import { Pool } from "pg";
import { grain, persistentState } from "@thresh/core/decorators";
import { Grain } from "@thresh/core/grain";
import { defineGrainInterface } from "@thresh/core/grain-interface";
import { JournaledGrain } from "@thresh/core/journaled-grain";
import type { GrainKey } from "@thresh/core/key-kinds";
import type { PersistentState } from "@thresh/core/persistent-state";
import type { Remindable, TickStatus } from "@thresh/core/reminder";
import { SiloAddress } from "@thresh/core/silo-address";
import { InProcessNetwork } from "@thresh/messaging/in-process-transport";
import { createSilo } from "@thresh/hosting/silo-builder";
import {
  STORAGE_ALLOW_IN_MEMORY_CLUSTER_ENV,
  STORAGE_URL_ENV,
  storageBackendFromEnvironment,
  type StorageBackend,
} from "@thresh/hosting/storage-backend";

const PG_URL = process.env.PG_URL ?? "postgres://localhost:5432/postgres";

async function reachable(connectionString: string): Promise<Pool | undefined> {
  const probe = new Pool({ connectionString });
  probe.on("error", () => {});
  try {
    await probe.query("SELECT 1");
    return probe;
  } catch {
    await probe.end().catch(() => {});
    return undefined;
  }
}

const admin = await reachable(PG_URL);

afterAll(async () => {
  await admin?.end();
});

interface IAccount extends GrainKey<string> {
  deposit(cents: number): Promise<number>;
  getBalance(): Promise<number>;
}
const IAccount = defineGrainInterface<IAccount>("IAccount.storage-backend", {
  options: { getBalance: { readOnly: true } },
});

@grain()
class AccountGrain extends Grain implements IAccount {
  // A provider name nothing registers explicitly: the backend must satisfy it.
  @persistentState("balance", {
    provider: "ledger-store",
    defaultValue: (): { cents: number } => ({ cents: 0 }),
  })
  private balance!: PersistentState<{ cents: number }>;

  async deposit(cents: number): Promise<number> {
    this.balance.value.cents += cents;
    await this.balance.write();
    return this.balance.value.cents;
  }

  async getBalance(): Promise<number> {
    return this.balance.value.cents;
  }
}

interface ILedger extends GrainKey<string> {
  add(n: number): Promise<number>;
  total(): Promise<number>;
}
const ILedger = defineGrainInterface<ILedger>("ILedger.storage-backend", {
  options: { total: { readOnly: true } },
});

@grain()
class LedgerGrain extends JournaledGrain<number, number> implements ILedger {
  initialState(): number {
    return 0;
  }

  transitionState(state: number, event: number): number {
    return state + event;
  }

  async add(n: number): Promise<number> {
    this.raiseEvent(n);
    await this.confirmEvents();
    return this.state;
  }

  async total(): Promise<number> {
    return this.state;
  }
}

const ticks: string[] = [];

interface IAlarm extends GrainKey<string> {
  arm(): Promise<void>;
}
const IAlarm = defineGrainInterface<IAlarm>("IAlarm.storage-backend");

@grain()
class AlarmGrain extends Grain implements IAlarm, Remindable {
  async arm(): Promise<void> {
    await this.runtime.registerReminder("ring", { seconds: 60 }, { seconds: 60 });
  }

  async receiveReminder(name: string, _status: TickStatus): Promise<void> {
    ticks.push(name);
  }
}

const local = new SiloAddress("silo-sb", "uid-sb", "silo-sb:11111");
const peer = new SiloAddress("silo-sb2", "uid-sb2", "silo-sb2:11111");

function buildSilo(backend: StorageBackend, silos: readonly SiloAddress[] = [local]) {
  return createSilo({ clusterId: "c-storage-backend", local })
    .useStaticMembership(silos)
    .useInProcessTransport(new InProcessNetwork())
    .useStorageBackend(backend)
    .useReminders()
    .registerGrain(AccountGrain, { interfaces: [IAccount] })
    .registerGrain(LedgerGrain, { interfaces: [ILedger] })
    .registerGrain(AlarmGrain, { interfaces: [IAlarm] });
}

describe("storageBackendFromEnvironment", () => {
  it("is in-memory when the URL is not set", () => {
    expect(storageBackendFromEnvironment({})).toEqual({ kind: "memory" });
  });

  it("carries the in-memory cluster opt-in", () => {
    expect(
      storageBackendFromEnvironment({ [STORAGE_ALLOW_IN_MEMORY_CLUSTER_ENV]: "true" }),
    ).toEqual({ kind: "memory", allowInCluster: true });
    expect(
      storageBackendFromEnvironment({ [STORAGE_ALLOW_IN_MEMORY_CLUSTER_ENV]: "FALSE" }),
    ).toEqual({ kind: "memory" });
  });

  it("rejects an opt-in that is not a boolean, naming the variable", () => {
    expect(() =>
      storageBackendFromEnvironment({ [STORAGE_ALLOW_IN_MEMORY_CLUSTER_ENV]: "yes" }),
    ).toThrow(STORAGE_ALLOW_IN_MEMORY_CLUSTER_ENV);
  });

  it("refuses a URL that is present but blank rather than degrading to memory", () => {
    expect(() => storageBackendFromEnvironment({ [STORAGE_URL_ENV]: "  " })).toThrow(
      STORAGE_URL_ENV,
    );
  });

  it("selects Postgres from a postgres or postgresql URL", () => {
    expect(storageBackendFromEnvironment({ [STORAGE_URL_ENV]: "postgres://h/db" })).toEqual({
      kind: "postgres",
      connectionString: "postgres://h/db",
    });
    expect(storageBackendFromEnvironment({ [STORAGE_URL_ENV]: "postgresql://h/db" })).toEqual({
      kind: "postgres",
      connectionString: "postgresql://h/db",
    });
  });

  it("selects Redis from a redis or rediss URL", () => {
    expect(storageBackendFromEnvironment({ [STORAGE_URL_ENV]: "redis://h:6379" })).toEqual({
      kind: "redis",
      url: "redis://h:6379",
    });
    expect(storageBackendFromEnvironment({ [STORAGE_URL_ENV]: "rediss://h:6380" })).toEqual({
      kind: "redis",
      url: "rediss://h:6380",
    });
  });

  it("rejects an unknown scheme, naming the variable", () => {
    expect(() => storageBackendFromEnvironment({ [STORAGE_URL_ENV]: "mysql://h/db" })).toThrow(
      STORAGE_URL_ENV,
    );
  });
});

describe("SiloBuilder.useStorageBackend (memory)", () => {
  it("satisfies any storage provider name, journaled grains and reminders", async () => {
    const silo = buildSilo({ kind: "memory" }).build();
    await silo.start();
    try {
      expect(await silo.getGrain(IAccount, "a-1").deposit(5)).toBe(5);
      expect(await silo.getGrain(ILedger, "l-1").add(7)).toBe(7);
      await silo.getGrain(IAlarm, "al-1").arm();
    } finally {
      await silo.stop();
    }
  });

  it("reads the backend's provider back under any name", () => {
    const builder = buildSilo({ kind: "memory" });
    expect(builder.storageProvider("ledger-store")).toBeDefined();
    expect(builder.storageProvider("ledger-store")).toBe(builder.storageProvider("other"));
  });

  it("keeps an explicitly registered provider ahead of the backend", async () => {
    const builder = buildSilo({ kind: "memory" });
    const before = builder.storageProvider("ledger-store");
    builder.useMemoryStorage();
    expect(builder.storageProvider("default")).not.toBe(before);
    expect(builder.storageProvider("ledger-store")).toBe(before);
  });

  it("refuses in-memory storage for a multi-silo view, naming the variable", () => {
    expect(() => buildSilo({ kind: "memory" }, [local, peer]).build()).toThrow(STORAGE_URL_ENV);
  });

  it("accepts in-memory storage for a multi-silo view when opted in", () => {
    expect(() =>
      buildSilo({ kind: "memory", allowInCluster: true }, [local, peer]).build(),
    ).not.toThrow();
  });
});

describe.skipIf(admin === undefined)("SiloBuilder.useStorageBackend (postgres)", () => {
  const prefix = `thresh_test_backend_${randomUUID().replace(/-/g, "")}`;

  afterAll(async () => {
    for (const table of [
      `${prefix}_grain_state`,
      `${prefix}_journal_logs`,
      `${prefix}_journal_entries`,
      `${prefix}_reminders`,
    ]) {
      await admin!.query(`DROP TABLE IF EXISTS ${table}`);
    }
  });

  const backend = (): StorageBackend => ({
    kind: "postgres",
    connectionString: PG_URL,
    tablePrefix: prefix,
  });

  it("keeps grain state, journals and reminders in Postgres across a restart", async () => {
    const first = buildSilo(backend()).build();
    await first.start();
    await first.getGrain(IAccount, "a-1").deposit(100);
    await first.getGrain(ILedger, "l-1").add(3);
    await first.getGrain(ILedger, "l-1").add(4);
    await first.getGrain(IAlarm, "al-1").arm();
    await first.stop();

    const restarted = buildSilo(backend()).build();
    await restarted.start();
    try {
      expect(await restarted.getGrain(IAccount, "a-1").getBalance()).toBe(100);
      expect(await restarted.getGrain(ILedger, "l-1").total()).toBe(7);
    } finally {
      await restarted.stop();
    }

    const reminders = await admin!.query(`SELECT 1 FROM ${prefix}_reminders`);
    expect(reminders.rowCount).toBeGreaterThan(0);
  });

  it("allows a multi-silo view", () => {
    expect(() => buildSilo(backend(), [local, peer]).build()).not.toThrow();
  });
});
