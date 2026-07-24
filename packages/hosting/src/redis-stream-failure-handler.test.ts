import { randomUUID } from "node:crypto";
import { afterAll, describe, expect, it } from "vitest";
import { createClient } from "redis";
import { grain, implicitStreamSubscription } from "@tsva/core/decorators";
import { Grain } from "@tsva/core/grain";
import { defineGrainInterface } from "@tsva/core/grain-interface";
import type { GrainWithStringKey } from "@tsva/core/key-kinds";
import { SiloAddress } from "@tsva/core/silo-address";
import { STREAM_SUBSCRIPTION_OBSERVER, type StreamHandler } from "@tsva/core/stream";
import { InProcessNetwork } from "@tsva/messaging/in-process-transport";
import { StaticMembershipService } from "@tsva/runtime/static-membership";
import { MemoryStreamFailureStore } from "@tsva/streams/stream-failure-store";
import { createSilo } from "@tsva/hosting/silo-builder";

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

async function waitFor(
  predicate: () => boolean | Promise<boolean>,
  timeoutMs = 5000,
): Promise<void> {
  const start = Date.now();
  while (!(await predicate())) {
    if (Date.now() - start > timeoutMs) throw new Error("waitFor: timed out");
    await new Promise((r) => setTimeout(r, 15));
  }
}

const admin = await reachable(REDIS_URL);
const keyPrefix = `tsva-test:stream-failure-handler:${randomUUID()}`;

afterAll(async () => {
  if (admin === undefined) return;
  for await (const keys of admin.scanIterator({ MATCH: `${keyPrefix}*` })) {
    if (keys.length > 0) await admin.del(keys);
  }
  await admin.destroy();
});

interface IPoisonRoom extends GrainWithStringKey {
  say(text: string): Promise<void>;
}
const IPoisonRoom = defineGrainInterface<IPoisonRoom>("IPoisonRoom.failureHandler");

type IAlwaysFailingConsumerGrain = GrainWithStringKey;
const IAlwaysFailingConsumerGrain = defineGrainInterface<IAlwaysFailingConsumerGrain>(
  "IAlwaysFailingConsumerGrain.failureHandler",
);

@grain()
class PoisonRoomGrain extends Grain implements IPoisonRoom {
  async say(text: string): Promise<void> {
    await this.runtime.getStreamProvider().getStream<string>("room", this.id.key).publish(text);
  }
}

/** Every event this implicit subscriber receives fails delivery, exercising the failure store. */
@grain()
@implicitStreamSubscription("room")
class AlwaysFailingConsumerGrain extends Grain {
  [STREAM_SUBSCRIPTION_OBSERVER](): StreamHandler<string> {
    return {
      onNext: async () => {
        throw new Error("consumer always fails");
      },
    };
  }
}

describe.skipIf(admin === undefined)("useDurableStreamFailureStore + addRedisStreams", () => {
  it("records a permanently-failed delivery in the configured durable failure store", async () => {
    const local = new SiloAddress("silo-0", "uid-0", "silo-0:11111");
    const membership = new StaticMembershipService(local, [local]);
    const net = new InProcessNetwork();
    const store = new MemoryStreamFailureStore();

    const silo = createSilo({ clusterId: "c", local, random: () => 0 })
      .useMembership(membership)
      .useInProcessTransport(net)
      .useDurableStreamFailureStore(store)
      .addRedisStreams("default", { url: REDIS_URL, keyPrefix })
      .registerGrain(PoisonRoomGrain, { interfaces: [IPoisonRoom] })
      .registerGrain(AlwaysFailingConsumerGrain, { interfaces: [IAlwaysFailingConsumerGrain] })
      .build();

    await silo.start();
    try {
      await silo.getGrain(IPoisonRoom, "room-1").say("poison");
      await waitFor(async () => (await store.listFailures()).length === 1);
      const [failure] = await store.listFailures();
      expect(failure).toMatchObject({ streamKey: "room/room-1", event: "poison", attempts: 3 });
    } finally {
      await silo.stop();
    }
  }, 15_000);

  it("falls back to no failure handler when useDurableStreamFailureStore was never called", () => {
    const local = new SiloAddress("silo-1", "uid-1", "silo-1:11111");
    const membership = new StaticMembershipService(local, [local]);
    const net = new InProcessNetwork();

    expect(() =>
      createSilo({ clusterId: "c", local, random: () => 0 })
        .useMembership(membership)
        .useInProcessTransport(net)
        .addRedisStreams("default", { url: REDIS_URL, keyPrefix })
        .build(),
    ).not.toThrow();
  });
});
