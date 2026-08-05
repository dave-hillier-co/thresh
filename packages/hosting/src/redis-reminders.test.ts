import { randomUUID } from "node:crypto";
import { afterAll, describe, expect, it } from "vitest";
import { createClient } from "redis";
import { grain } from "@thresh/core/decorators";
import { Grain } from "@thresh/core/grain";
import { defineGrainInterface } from "@thresh/core/grain-interface";
import type { GrainKey } from "@thresh/core/key-kinds";
import type { Remindable, TickStatus } from "@thresh/core/reminder";
import { SiloAddress } from "@thresh/core/silo-address";
import { FakeTimeProvider } from "@thresh/core/test-support/fake-time-provider";
import { InProcessNetwork } from "@thresh/messaging/in-process-transport";
import { createSilo } from "@thresh/hosting/silo-builder";

const REDIS_URL = process.env.REDIS_URL ?? "redis://localhost:6379";
type Client = ReturnType<typeof createClient>;

async function reachable(url: string): Promise<Client | undefined> {
  const probe = createClient({ url });
  probe.on("error", () => {});
  try {
    await probe.connect();
    await probe.ping();
    return probe;
  } catch {
    try {
      await probe.destroy();
    } catch {
      /* never connected */
    }
    return undefined;
  }
}

const admin = await reachable(REDIS_URL);
const keyPrefix = `thresh-test:rem-e2e:${randomUUID()}`;
const checks = new Map<string, number>();

afterAll(async () => {
  if (admin === undefined) return;
  for await (const keys of admin.scanIterator({ MATCH: `${keyPrefix}*` })) {
    if (keys.length > 0) await admin.del(keys);
  }
  await admin.destroy();
});

interface IBilling extends GrainKey<string> {
  scheduleSelfCheck(): Promise<void>;
  checkCount(): Promise<number>;
}
const IBilling = defineGrainInterface<IBilling>("IBilling.redis");

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

describe.skipIf(admin === undefined)("Redis reminders end-to-end", () => {
  it("fires a reminder durably recorded in Redis", async () => {
    checks.clear();
    const time = new FakeTimeProvider();
    const silo = createSilo({ clusterId: "c1", local, time })
      .useStaticMembership([local])
      .useInProcessTransport(new InProcessNetwork())
      .useRedisReminders({ url: REDIS_URL, keyPrefix })
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
