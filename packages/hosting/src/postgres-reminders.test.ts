import { randomUUID } from "node:crypto";
import { afterAll, describe, expect, it } from "vitest";
import { Pool } from "pg";
import { grain } from "@thresh/core/decorators";
import { Grain } from "@thresh/core/grain";
import { defineGrainInterface } from "@thresh/core/grain-interface";
import type { GrainKey } from "@thresh/core/key-kinds";
import type { Remindable, TickStatus } from "@thresh/core/reminder";
import { SiloAddress } from "@thresh/core/silo-address";
import { FakeTimeProvider } from "@thresh/core/test-support/fake-time-provider";
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
const checks = new Map<string, number>();

afterAll(async () => {
  if (admin === undefined) return;
  await admin.query(`DROP TABLE IF EXISTS ${tableName}`);
  await admin.end();
});

interface IBilling extends GrainKey<string> {
  scheduleSelfCheck(): Promise<void>;
  checkCount(): Promise<number>;
}
const IBilling = defineGrainInterface<IBilling>("IBilling.postgres");

@grain()
class BillingGrain extends Grain implements IBilling, Remindable {
  async scheduleSelfCheck(): Promise<void> {
    await this.runtime.registerReminder("self-check", { seconds: 60 }, { seconds: 60 });
  }
  async checkCount(): Promise<number> {
    return checks.get(String(this.id.key)) ?? 0;
  }
  async receiveReminder(name: string, _status: TickStatus): Promise<void> {
    if (name === "self-check") {
      const key = String(this.id.key);
      checks.set(key, (checks.get(key) ?? 0) + 1);
    }
  }
}

const local = new SiloAddress("silo-0", "uid-0", "silo-0:11111");

describe.skipIf(admin === undefined)("Postgres reminders end-to-end", () => {
  it("fires a reminder durably recorded in Postgres", async () => {
    checks.clear();
    const time = new FakeTimeProvider();
    const silo = createSilo({ clusterId: "c1", local, time })
      .useStaticMembership([local])
      .useInProcessTransport(new InProcessNetwork())
      .usePostgresReminders({ connectionString: PG_URL, tableName })
      .registerGrain(BillingGrain, { interfaces: [IBilling] })
      .build();
    await silo.start();
    try {
      await silo.getGrain(IBilling, "acct").scheduleSelfCheck();
      time.advance(180_000); // three 60s periods
      await new Promise((r) => setTimeout(r, 0));
      expect(await silo.getGrain(IBilling, "acct").checkCount()).toBe(3);
    } finally {
      await silo.stop();
    }
  });
});
