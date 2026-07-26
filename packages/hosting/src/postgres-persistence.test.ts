import { randomUUID } from "node:crypto";
import { afterAll, describe, expect, it } from "vitest";
import { Pool } from "pg";
import { grain, persistentState } from "@thresh/core/decorators";
import { Grain } from "@thresh/core/grain";
import { defineGrainInterface } from "@thresh/core/grain-interface";
import type { GrainWithStringKey } from "@thresh/core/key-kinds";
import type { PersistentState } from "@thresh/core/persistent-state";
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
const tableName = `thresh_test_${randomUUID().replace(/-/g, "")}`;

afterAll(async () => {
  if (admin === undefined) return;
  await admin.query(`DROP TABLE IF EXISTS ${tableName}`);
  await admin.end();
});

interface BalanceState {
  cents: number;
}

interface IAccount extends GrainWithStringKey {
  deposit(cents: number): Promise<number>;
  getBalance(): Promise<number>;
}
const IAccount = defineGrainInterface<IAccount>("IAccount.postgres", {
  options: { getBalance: { readOnly: true } },
});

@grain()
class AccountGrain extends Grain implements IAccount {
  @persistentState("balance", { defaultValue: (): BalanceState => ({ cents: 0 }) })
  private balance!: PersistentState<BalanceState>;

  async deposit(cents: number): Promise<number> {
    this.balance.value.cents += cents;
    await this.balance.write();
    return this.balance.value.cents;
  }

  async getBalance(): Promise<number> {
    return this.balance.value.cents;
  }
}

const local = new SiloAddress("silo-0", "uid-0", "silo-0:11111");

function buildSilo() {
  return createSilo({ clusterId: "c1", local })
    .useStaticMembership([local])
    .useInProcessTransport(new InProcessNetwork())
    .addPostgresStorage("default", { connectionString: PG_URL, tableName })
    .registerGrain(AccountGrain, { interfaces: [IAccount] })
    .build();
}

describe.skipIf(admin === undefined)("Postgres persistence end-to-end", () => {
  it("survives a silo restart via the durable Postgres store", async () => {
    const first = buildSilo();
    await first.start();
    expect(await first.getGrain(IAccount, "acc-1").deposit(100)).toBe(100);
    expect(await first.getGrain(IAccount, "acc-1").deposit(50)).toBe(150);
    await first.stop(); // pod dies, Postgres pool closes

    const restarted = buildSilo(); // new pod, same durable store
    await restarted.start();
    try {
      // Read-on-activate repopulates the grain from Postgres, not local memory.
      expect(await restarted.getGrain(IAccount, "acc-1").getBalance()).toBe(150);
    } finally {
      await restarted.stop();
    }
  });
});
