import { randomUUID } from "node:crypto";
import { afterAll, describe, expect, it } from "vitest";
import { createClient } from "redis";
import { grain, persistentState } from "@thresh/core/decorators";
import { Grain } from "@thresh/core/grain";
import { defineGrainInterface } from "@thresh/core/grain-interface";
import type { GrainKey } from "@thresh/core/key-kinds";
import type { PersistentState } from "@thresh/core/persistent-state";
import { SiloAddress } from "@thresh/core/silo-address";
import { InProcessNetwork } from "@thresh/messaging/in-process-transport";
import { createSilo } from "@thresh/hosting/silo-builder";

const REDIS_URL = process.env.REDIS_URL ?? "redis://localhost:6379";
type Client = ReturnType<typeof createClient>;

async function reachable(url: string): Promise<Client | undefined> {
  // `reconnectStrategy: false` is load-bearing, not tidiness: node-redis retries a refused
  // connection forever by default, so `connect()` below never settles when no Redis is
  // listening and this module-load probe hangs the whole suite instead of skipping it.
  const probe = createClient({ url, socket: { reconnectStrategy: false, connectTimeout: 500 } });
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
const keyPrefix = `thresh-test:e2e:${randomUUID()}`;

afterAll(async () => {
  if (admin === undefined) return;
  for await (const keys of admin.scanIterator({ MATCH: `${keyPrefix}*` })) {
    if (keys.length > 0) await admin.del(keys);
  }
  await admin.destroy();
});

interface BalanceState {
  cents: number;
}

interface IAccount extends GrainKey<string> {
  deposit(cents: number): Promise<number>;
  getBalance(): Promise<number>;
}
const IAccount = defineGrainInterface<IAccount>("IAccount.redis", {
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
    .addRedisStorage("default", { url: REDIS_URL, keyPrefix })
    .registerGrain(AccountGrain, { interfaces: [IAccount] })
    .build();
}

describe.skipIf(admin === undefined)("Redis persistence end-to-end", () => {
  it("survives a silo restart via the durable Redis store", async () => {
    const first = buildSilo();
    await first.start();
    expect(await first.getGrain(IAccount, "acc-1").deposit(100)).toBe(100);
    expect(await first.getGrain(IAccount, "acc-1").deposit(50)).toBe(150);
    await first.stop(); // pod dies, Redis connection closes

    const restarted = buildSilo(); // new pod, same durable store
    await restarted.start();
    try {
      // Read-on-activate repopulates the grain from Redis, not local memory.
      expect(await restarted.getGrain(IAccount, "acc-1").getBalance()).toBe(150);
    } finally {
      await restarted.stop();
    }
  });
});
