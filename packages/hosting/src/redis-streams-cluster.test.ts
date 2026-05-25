import { randomUUID } from "node:crypto";
import { afterAll, describe, expect, it } from "vitest";
import { createClient } from "redis";
import { grain } from "@tsva/core/decorators";
import { Grain } from "@tsva/core/grain";
import { defineGrainInterface } from "@tsva/core/grain-interface";
import { stableHash32 } from "@tsva/core/hash";
import type { GrainWithStringKey } from "@tsva/core/key-kinds";
import { SiloAddress } from "@tsva/core/silo-address";
import type { StreamHandler } from "@tsva/core/stream";
import { ConsistentHashRing } from "@tsva/directory/consistent-hash-ring";
import { InProcessNetwork } from "@tsva/messaging/in-process-transport";
import { StaticMembershipService } from "@tsva/runtime/static-membership";
import { queueRingHash } from "@tsva/streams/queue-ownership";
import { createSilo } from "@tsva/hosting/silo-builder";
import type { SiloHost } from "@tsva/hosting/silo-host";

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

async function waitFor(predicate: () => boolean, timeoutMs = 4000): Promise<void> {
  const start = Date.now();
  while (!predicate()) {
    if (Date.now() - start > timeoutMs) throw new Error("waitFor: timed out");
    await new Promise((r) => setTimeout(r, 15));
  }
}

const admin = await reachable(REDIS_URL);
const keyPrefix = `tsva-test:stream-cluster:${randomUUID()}`;
const QUEUE_COUNT = 8; // RedisPullingStreamProvider default

// Cross-activation sinks (a reactivated consumer keeps accumulating here).
const received = new Map<string, string[]>();
const joinedRoom = new Map<string, string>();

const addrs = [0, 1].map((n) => new SiloAddress(`silo-${n}`, `uid-${n}`, `silo-${n}:11111`));
const inRanges = (h: number, ranges: ReadonlyArray<readonly [number, number]>) =>
  ranges.some(([b, e]) => (b <= e ? h >= b && h < e : h >= b || h < e));

/** A room whose physical queue the ring assigns to silo `index`. */
function roomOwnedBy(index: number): string {
  const ranges = new ConsistentHashRing(addrs).rangesFor(addrs[index]!);
  for (let i = 0; ; i++) {
    const room = `room-${i}`;
    const queue = stableHash32(`chat/${room}`) % QUEUE_COUNT;
    if (inRanges(queueRingHash("default", queue), ranges)) return room;
  }
}

afterAll(async () => {
  if (admin === undefined) return;
  for await (const keys of admin.scanIterator({ MATCH: `${keyPrefix}*` })) {
    if (keys.length > 0) await admin.del(keys);
  }
  await admin.destroy();
});

interface IChatRoom extends GrainWithStringKey {
  say(text: string): Promise<void>;
}
const IChatRoom = defineGrainInterface<IChatRoom>("IChatRoom.cluster", { methods: ["say"] });

interface IChatUser extends GrainWithStringKey {
  join(room: string): Promise<void>;
  count(): Promise<number>;
}
const IChatUser = defineGrainInterface<IChatUser>("IChatUser.cluster", {
  methods: ["join", "count"],
  options: { count: { readOnly: true } },
});

@grain()
class ChatRoomGrain extends Grain implements IChatRoom {
  async say(text: string): Promise<void> {
    await this.runtime.getStreamProvider().getStream<string>("chat", this.id.key).publish(text);
  }
}

@grain()
class ChatUserGrain extends Grain implements IChatUser {
  override async onActivate(): Promise<void> {
    // Re-bind the subscription if this is a fresh activation after reactivation.
    const room = joinedRoom.get(String(this.id.key));
    if (room === undefined) return;
    const stream = this.runtime.getStreamProvider().getStream<string>("chat", room);
    const existing = await stream.getSubscriptions();
    if (existing.length > 0) await existing[0]!.resume(this.handler(room));
  }

  async join(room: string): Promise<void> {
    joinedRoom.set(String(this.id.key), room);
    received.set(String(this.id.key), received.get(String(this.id.key)) ?? []);
    await this.runtime
      .getStreamProvider()
      .getStream<string>("chat", room)
      .subscribe(this.handler(room));
  }

  async count(): Promise<number> {
    return received.get(String(this.id.key))?.length ?? 0;
  }

  private handler(_room: string): StreamHandler<string> {
    const key = String(this.id.key);
    return { onNext: async (text) => void received.get(key)!.push(text) };
  }
}

function buildSilo(local: SiloAddress, membership: StaticMembershipService, net: InProcessNetwork) {
  return createSilo({ clusterId: "c", local, random: () => 0 })
    .useMembership(membership)
    .useInProcessTransport(net)
    .addRedisStreams("default", { url: REDIS_URL, keyPrefix })
    .registerGrain(ChatRoomGrain, { interfaces: [IChatRoom] })
    .registerGrain(ChatUserGrain, { interfaces: [IChatUser] })
    .build();
}

describe.skipIf(admin === undefined)("Redis streams across a cluster", () => {
  it("hands a queue to a surviving silo, which resumes from the committed cursor", async () => {
    received.clear();
    joinedRoom.clear();
    const net = new InProcessNetwork();
    const membership = new StaticMembershipService(addrs[0]!, addrs);
    const silos: SiloHost[] = addrs.map((local) => buildSilo(local, membership, net));
    for (const s of silos) await s.start();

    // The room's queue is owned by silo-1; the consumer activates on silo-0
    // (random -> first candidate), so delivery crosses silos and killing silo-1
    // exercises a genuine queue handoff without disturbing the consumer.
    const room = roomOwnedBy(1);

    try {
      await silos[0]!.getGrain(IChatUser, "alice").join(room);
      await silos[0]!.getGrain(IChatRoom, room).say("m1");
      await silos[0]!.getGrain(IChatRoom, room).say("m2");
      await waitFor(() => (received.get("alice")?.length ?? 0) === 2);

      // silo-1 (the queue owner) dies and leaves the view.
      await silos[1]!.stop();
      membership.removeSilo(addrs[1]!);

      // More events arrive; silo-0 takes the queue over and resumes from cursor 2.
      await silos[0]!.getGrain(IChatRoom, room).say("m3");
      await silos[0]!.getGrain(IChatRoom, room).say("m4");
      await waitFor(() => (received.get("alice")?.length ?? 0) === 4);

      // No gaps, no duplicate redelivery of the pre-handoff events.
      expect(received.get("alice")).toEqual(["m1", "m2", "m3", "m4"]);
    } finally {
      await Promise.all(silos.map((s) => s.stop().catch(() => undefined)));
    }
  }, 30_000);
});
