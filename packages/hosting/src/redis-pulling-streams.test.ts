import { randomUUID } from "node:crypto";
import { afterAll, describe, expect, it } from "vitest";
import { createClient } from "redis";
import { grain, implicitStreamSubscription } from "@thresh/core/decorators";
import { Grain } from "@thresh/core/grain";
import { defineGrainInterface } from "@thresh/core/grain-interface";
import type { GrainWithStringKey } from "@thresh/core/key-kinds";
import { SiloAddress } from "@thresh/core/silo-address";
import { STREAM_SUBSCRIPTION_OBSERVER, type StreamHandler } from "@thresh/core/stream";
import { InProcessNetwork } from "@thresh/messaging/in-process-transport";
import { createSilo } from "@thresh/hosting/silo-builder";
import type { SiloHost } from "@thresh/hosting/silo-host";

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

async function waitFor(predicate: () => Promise<boolean>, timeoutMs = 3000): Promise<void> {
  const start = Date.now();
  while (!(await predicate())) {
    if (Date.now() - start > timeoutMs) throw new Error("waitFor: timed out");
    await new Promise((r) => setTimeout(r, 10));
  }
}

const admin = await reachable(REDIS_URL);
const keyPrefix = `thresh-test:pull-e2e:${randomUUID()}`;

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
const IChatRoom = defineGrainInterface<IChatRoom>("IChatRoom.pull");

interface IChatUser extends GrainWithStringKey {
  join(room: string): Promise<void>;
  history(): Promise<string[]>;
}
const IChatUser = defineGrainInterface<IChatUser>("IChatUser.pull", {
  options: { history: { readOnly: true } },
});

@grain()
class ChatRoomGrain extends Grain implements IChatRoom {
  async say(text: string): Promise<void> {
    await this.runtime.getStreamProvider().getStream<string>("chat", this.id.key).publish(text);
  }
}

@grain()
class ChatUserGrain extends Grain implements IChatUser {
  private received: string[] = [];

  async join(room: string): Promise<void> {
    const stream = this.runtime.getStreamProvider().getStream<string>("chat", room);
    const existing = await stream.getSubscriptions();
    if (existing.length > 0) await existing[0]!.resume(this.handler());
    else await stream.subscribe(this.handler());
  }

  async history(): Promise<string[]> {
    return [...this.received];
  }

  private handler(): StreamHandler<string> {
    return { onNext: async (text) => void this.received.push(text) };
  }
}

// An implicitly-subscribed grain: no `subscribe`/`join` call. A `Watcher` with
// key K is auto-subscribed to the stream ("chat", K) and records what it sees in
// a module sink (the grain may be collected between deliveries).
const watched: Record<string, string[]> = {};

interface IWatcher extends GrainWithStringKey {
  seen(): Promise<string[]>;
}
const IWatcher = defineGrainInterface<IWatcher>("IWatcher.pull", {
  options: { seen: { readOnly: true } },
});

@grain()
@implicitStreamSubscription("chat")
class WatcherGrain extends Grain implements IWatcher {
  async seen(): Promise<string[]> {
    return [...(watched[String(this.id.key)] ?? [])];
  }
  [STREAM_SUBSCRIPTION_OBSERVER](_namespace: string, key: string): StreamHandler<string> {
    return { onNext: async (text) => void (watched[key] ??= []).push(text) };
  }
}

const local = new SiloAddress("silo-0", "uid-0", "silo-0:11111");

function buildSilo(): SiloHost {
  return createSilo({ clusterId: "c1", local })
    .useStaticMembership([local])
    .useInProcessTransport(new InProcessNetwork())
    .addRedisStreams("default", { url: REDIS_URL, keyPrefix })
    .registerGrain(ChatRoomGrain, { interfaces: [IChatRoom] })
    .registerGrain(ChatUserGrain, { interfaces: [IChatUser] })
    .registerGrain(WatcherGrain, { interfaces: [IWatcher] })
    .build();
}

describe.skipIf(admin === undefined)("Redis pulling-agent streams end-to-end", () => {
  it("fans out a room's messages to every subscribed user grain, in order", async () => {
    const silo = buildSilo();
    await silo.start();
    try {
      // Two distinct consumer grains subscribe to one room stream; a third joins
      // a different room and must see none of it (subscription isolation).
      await silo.getGrain(IChatUser, "alice").join("general");
      await silo.getGrain(IChatUser, "bob").join("general");
      await silo.getGrain(IChatUser, "carol").join("random");

      await silo.getGrain(IChatRoom, "general").say("hello");
      await silo.getGrain(IChatRoom, "general").say("world");

      await waitFor(async () => (await silo.getGrain(IChatUser, "alice").history()).length === 2);
      await waitFor(async () => (await silo.getGrain(IChatUser, "bob").history()).length === 2);

      expect(await silo.getGrain(IChatUser, "alice").history()).toEqual(["hello", "world"]);
      expect(await silo.getGrain(IChatUser, "bob").history()).toEqual(["hello", "world"]);
      expect(await silo.getGrain(IChatUser, "carol").history()).toEqual([]);
    } finally {
      await silo.stop();
    }
  });

  it("auto-delivers to an implicitly-subscribed grain by key, with no subscribe call", async () => {
    const silo = buildSilo();
    await silo.start();
    try {
      // No join/subscribe: the Watcher's key matches the room's stream key, so it
      // is implicitly subscribed to ("chat", "watched"). A Watcher on a different
      // key sees nothing (subscription is by key).
      await silo.getGrain(IChatRoom, "watched").say("ping");
      await silo.getGrain(IChatRoom, "watched").say("pong");

      await waitFor(async () => (await silo.getGrain(IWatcher, "watched").seen()).length === 2);
      expect(await silo.getGrain(IWatcher, "watched").seen()).toEqual(["ping", "pong"]);
      expect(await silo.getGrain(IWatcher, "other").seen()).toEqual([]);
    } finally {
      await silo.stop();
    }
  });
});
