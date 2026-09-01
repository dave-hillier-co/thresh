import { randomUUID } from "node:crypto";
import { afterAll, describe, expect, it } from "vitest";
import { createClient } from "redis";
import { durableList, grain } from "@thresh/core/decorators";
import { Grain } from "@thresh/core/grain";
import { defineGrainInterface } from "@thresh/core/grain-interface";
import type { GrainKey } from "@thresh/core/key-kinds";
import type { DurableList } from "@thresh/core/durable-state";
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
const keyPrefix = `thresh-test:journal-e2e:${randomUUID()}`;

afterAll(async () => {
  if (admin === undefined) return;
  for await (const keys of admin.scanIterator({ MATCH: `${keyPrefix}*` })) {
    if (keys.length > 0) await admin.del(keys);
  }
  await admin.destroy();
});

interface ICart extends GrainKey<string> {
  add(item: string): Promise<number>;
  list(): Promise<readonly string[]>;
}
const ICart = defineGrainInterface<ICart>("ICart.redis-journal", {
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

const local = new SiloAddress("silo-0", "uid-0", "silo-0:11111");

function buildSilo(serviceId?: string, address: SiloAddress = local) {
  return createSilo({
    clusterId: "c1",
    ...(serviceId !== undefined ? { serviceId } : {}),
    local: address,
  })
    .useStaticMembership([address])
    .useInProcessTransport(new InProcessNetwork())
    .addRedisJournaling("default", { url: REDIS_URL, keyPrefix })
    .registerGrain(CartGrain, { interfaces: [ICart] })
    .build();
}

describe.skipIf(admin === undefined)("Redis journaling end-to-end", () => {
  it("replays a journalled list across a silo restart via Redis", async () => {
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

  // Issue #64: `addRedisJournaling` must thread the silo's `serviceId` into
  // `RedisJournalStorage`, not leave every silo on `DEFAULT_SERVICE_ID`.
  it("threads the silo's serviceId into the journal keys", async () => {
    const address = new SiloAddress("silo-svc", "uid-svc", "silo-svc:11111");
    const silo = buildSilo("svc-a", address);
    await silo.start();
    try {
      await silo.getGrain(ICart, "c-svc").add("thing");
      const keys: string[] = [];
      for await (const batch of admin!.scanIterator({ MATCH: `${keyPrefix}:svc-a:journal:*` })) {
        keys.push(...(Array.isArray(batch) ? batch : [batch]));
      }
      expect(keys.length).toBeGreaterThan(0);
    } finally {
      await silo.stop();
    }
  });
});
