import { describe, expect, it } from "vitest";
import { GrainId } from "@tsva/core/grain-id";
import { SiloAddress } from "@tsva/core/silo-address";
import { FakeTimeProvider } from "@tsva/core/test-support/fake-time-provider";
import { InProcessNetwork } from "@tsva/messaging/in-process-transport";
import { createSilo } from "@tsva/hosting/silo-builder";
import type { SiloHost } from "@tsva/hosting/silo-host";
import { ChatRoomGrain } from "@tsva/example-chat/chat-room-grain";
import { ChatUserGrain } from "@tsva/example-chat/chat-user-grain";
import { runChatDemo } from "@tsva/example-chat/demo";
import { IChatRoom, IChatUser } from "@tsva/example-chat/interfaces";

const local = new SiloAddress("silo-0", "uid-0", "silo-0:11111");
const flush = () => new Promise((r) => setTimeout(r, 0));
const texts = (msgs: { text: string }[]) => msgs.map((m) => m.text);

function buildChatSilo(time?: FakeTimeProvider): SiloHost {
  return createSilo({
    clusterId: "chat",
    local,
    ...(time !== undefined
      ? { time, collectionAgeSeconds: 30, collectionIntervalSeconds: 10 }
      : {}),
  })
    .useStaticMembership([local])
    .useInProcessTransport(new InProcessNetwork())
    .useMemoryStreams()
    .registerGrain(ChatRoomGrain, { interfaces: [IChatRoom] })
    .registerGrain(ChatUserGrain, { interfaces: [IChatUser] })
    .build();
}

describe("chat room (stream fan-out acceptance)", () => {
  it("fans a message out to every member in publish order", async () => {
    const silo = buildChatSilo();
    await silo.start();
    try {
      for (const name of ["alice", "bob", "carol"]) {
        await silo.getGrain(IChatUser, name).join("general");
      }
      await silo.getGrain(IChatRoom, "general").say("alice", "hi all");
      await silo.getGrain(IChatRoom, "general").say("bob", "hey");
      await flush();

      for (const name of ["alice", "bob", "carol"]) {
        expect(texts(await silo.getGrain(IChatUser, name).history())).toEqual(["hi all", "hey"]);
      }
    } finally {
      await silo.stop();
    }
  });

  it("isolates rooms by key — a member hears only its own room", async () => {
    const silo = buildChatSilo();
    await silo.start();
    try {
      await silo.getGrain(IChatUser, "amy").join("red");
      await silo.getGrain(IChatUser, "ben").join("blue");
      await silo.getGrain(IChatRoom, "red").say("amy", "red-only");
      await flush();

      expect(texts(await silo.getGrain(IChatUser, "amy").history())).toEqual(["red-only"]);
      expect(await silo.getGrain(IChatUser, "ben").history()).toEqual([]);
    } finally {
      await silo.stop();
    }
  });

  it("resumes a member's own subscription after it deactivated while idle", async () => {
    const time = new FakeTimeProvider();
    const silo = buildChatSilo(time);
    await silo.start();
    try {
      await silo.getGrain(IChatUser, "alice").join("general");
      await silo.getGrain(IChatUser, "bob").join("general");
      await silo.getGrain(IChatRoom, "general").say("alice", "m1");
      await flush();

      // Keep alice active past one sweep; let bob fall idle and be collected.
      time.advance(25_000);
      await silo.getGrain(IChatUser, "alice").history();
      time.advance(20_000);
      await flush();
      expect(silo.isActive(new GrainId("ChatUser", "bob"))).toBe(false);

      // Messages bob misses while away; alice (still active) receives them live,
      // advancing alice's cursor past bob's.
      await silo.getGrain(IChatRoom, "general").say("alice", "m2");
      await silo.getGrain(IChatRoom, "general").say("alice", "m3");
      await flush();
      expect(texts(await silo.getGrain(IChatUser, "alice").history())).toEqual(["m1", "m2", "m3"]);

      // Bob rejoins: it must resume ITS OWN subscription (cursor at m1), receiving
      // only the two it missed — not alice's already-consumed position (which would
      // yield nothing).
      await silo.getGrain(IChatUser, "bob").join("general");
      await flush();
      expect(texts(await silo.getGrain(IChatUser, "bob").history())).toEqual(["m2", "m3"]);
    } finally {
      await silo.stop();
    }
  });

  it("runs the runnable demo end-to-end", async () => {
    const result = await runChatDemo();
    for (const name of ["alice", "bob", "carol"]) {
      expect(texts(result.fanOut[name] ?? [])).toEqual(["hello everyone"]);
    }
    expect(texts(result.bobResumedWhileAway)).toEqual(["you around?", "guess not"]);
  });
});
