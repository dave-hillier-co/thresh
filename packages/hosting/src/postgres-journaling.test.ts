import { randomUUID } from "node:crypto";
import { afterAll, describe, expect, it } from "vitest";
import { Pool } from "pg";
import { durableList, grain } from "@thresh/core/decorators";
import { Grain } from "@thresh/core/grain";
import { defineGrainInterface } from "@thresh/core/grain-interface";
import { JournaledGrain } from "@thresh/core/journaled-grain";
import type { GrainKey } from "@thresh/core/key-kinds";
import type { DurableList } from "@thresh/core/durable-state";
import { SiloAddress } from "@thresh/core/silo-address";
import { InProcessNetwork } from "@thresh/messaging/in-process-transport";
import { createSilo } from "@thresh/hosting/silo-builder";

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
const tablePrefix = `thresh_test_pgjournal_${randomUUID().replace(/-/g, "")}`;

afterAll(async () => {
  if (admin === undefined) return;
  await admin.query(`DROP TABLE IF EXISTS ${tablePrefix}_entries`);
  await admin.query(`DROP TABLE IF EXISTS ${tablePrefix}_logs`);
  await admin.end();
});

interface ICart extends GrainKey<string> {
  add(item: string): Promise<number>;
  list(): Promise<readonly string[]>;
}
const ICart = defineGrainInterface<ICart>("ICart.postgres-journal", {
  options: { list: { readOnly: true } },
});

@grain()
class CartGrain extends Grain implements ICart {
  @durableList("items")
  private items!: DurableList<string>;

  async add(item: string): Promise<number> {
    await this.items.add(item);
    return this.items.length;
  }

  async list(): Promise<readonly string[]> {
    return this.items.toArray();
  }
}

interface ILedger extends GrainKey<string> {
  add(n: number): Promise<[number, number]>;
  read(): Promise<[number, number]>;
}
const ILedger = defineGrainInterface<ILedger>("ILedger.postgres-journal", {
  options: { read: { readOnly: true } },
});

@grain()
class LedgerGrain extends JournaledGrain<number, number> implements ILedger {
  initialState(): number {
    return 0;
  }

  transitionState(state: number, event: number): number {
    return state + event;
  }

  async add(n: number): Promise<[number, number]> {
    this.raiseEvent(n);
    await this.confirmEvents();
    return [this.state, this.version];
  }

  async read(): Promise<[number, number]> {
    return [this.state, this.version];
  }
}

function buildSilo(serviceId?: string) {
  const local = new SiloAddress("silo-pg", "uid-pg", "silo-pg:11111");
  return createSilo({
    clusterId: "c1",
    ...(serviceId !== undefined ? { serviceId } : {}),
    local,
  })
    .useStaticMembership([local])
    .useInProcessTransport(new InProcessNetwork())
    .addPostgresJournaling("default", { connectionString: PG_URL, tablePrefix })
    .registerGrain(CartGrain, { interfaces: [ICart] })
    .registerGrain(LedgerGrain, { interfaces: [ILedger] })
    .build();
}

describe.skipIf(admin === undefined)("Postgres journaling end-to-end", () => {
  it("replays a journalled list across a silo restart via Postgres", async () => {
    const first = buildSilo();
    await first.start();
    await first.getGrain(ICart, "c-1").add("apple");
    await first.getGrain(ICart, "c-1").add("pear");
    await first.stop();

    const restarted = buildSilo();
    await restarted.start();
    try {
      expect(await restarted.getGrain(ICart, "c-1").list()).toEqual(["apple", "pear"]);
    } finally {
      await restarted.stop();
    }
  });

  it("replays a JournaledGrain's event log across a silo restart via Postgres", async () => {
    const first = buildSilo();
    await first.start();
    for (let i = 1; i <= 150; i++) await first.getGrain(ILedger, "l-1").add(i);
    await first.stop();

    const restarted = buildSilo();
    await restarted.start();
    try {
      // 150 events crosses the default compaction threshold, so this also
      // proves the snapshot frame round-trips with its version.
      expect(await restarted.getGrain(ILedger, "l-1").read()).toEqual([(150 * 151) / 2, 150]);
    } finally {
      await restarted.stop();
    }
  });

  it("threads the silo's serviceId into the journal rows", async () => {
    const silo = buildSilo("svc-a");
    await silo.start();
    try {
      await silo.getGrain(ICart, "c-svc").add("thing");
      const rows = await admin!.query(
        `SELECT 1 FROM ${tablePrefix}_logs WHERE service_id = 'svc-a'`,
      );
      expect(rows.rowCount).toBeGreaterThan(0);
    } finally {
      await silo.stop();
    }
  });
});
