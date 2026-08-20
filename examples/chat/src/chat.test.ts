import { describe, expect, it } from "vitest";
import { GrainId } from "@thresh/core/grain-id";
import { SiloAddress } from "@thresh/core/silo-address";
import { FakeTimeProvider } from "@thresh/core/test-support/fake-time-provider";
import { InProcessNetwork } from "@thresh/messaging/in-process-transport";
import { createSilo } from "@thresh/hosting/silo-builder";
import type { SiloHost } from "@thresh/hosting/silo-host";
import { ChatRoomGrain } from "@thresh/example-chat/chat-room-grain";
import { ChatUserGrain } from "@thresh/example-chat/chat-user-grain";
import { runChatDemo } from "@thresh/example-chat/demo";
import { chatRoom, chatUser } from "@thresh/example-chat/interfaces";

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
    .registerGrain(ChatRoomGrain, { interfaces: [chatRoom] })
    .registerGrain(ChatUserGrain, { interfaces: [chatUser] })
    .build();
}

describe("chat room (stream fan-out acceptance)", () => {
  it("fans a message out to every member in publish order", async () => {
    const silo = buildChatSilo();
    await silo.start();
    try {
      for (const name of ["alice", "bob", "carol"]) {
        await silo.getGrain(chatUser, name).join("general");
      }
      await silo.getGrain(chatRoom, "general").say("alice", "hi all");
      await silo.getGrain(chatRoom, "general").say("bob", "hey");
      await flush();

      for (const name of ["alice", "bob", "carol"]) {
        expect(texts(await silo.getGrain(chatUser, name).history())).toEqual(["hi all", "hey"]);
      }
    } finally {
      await silo.stop();
    }
  });

  it("isolates rooms by key — a member hears only its own room", async () => {
    const silo = buildChatSilo();
    await silo.start();
    try {
      await silo.getGrain(chatUser, "amy").join("red");
      await silo.getGrain(chatUser, "ben").join("blue");
      await silo.getGrain(chatRoom, "red").say("amy", "red-only");
      await flush();

      expect(texts(await silo.getGrain(chatUser, "amy").history())).toEqual(["red-only"]);
      expect(await silo.getGrain(chatUser, "ben").history()).toEqual([]);
    } finally {
      await silo.stop();
    }
  });

  it("resumes a member's own subscription after it deactivated while idle", async () => {
    const time = new FakeTimeProvider();
    const silo = buildChatSilo(time);
    await silo.start();
    try {
      await silo.getGrain(chatUser, "alice").join("general");
      await silo.getGrain(chatUser, "bob").join("general");
      await silo.getGrain(chatRoom, "general").say("alice", "m1");
      await flush();

      // Keep alice active past one sweep; let bob fall idle and be collected.
      time.advance(25_000);
      await silo.getGrain(chatUser, "alice").history();
      time.advance(20_000);
      await flush();
      expect(silo.isActive(new GrainId("ChatUser", "bob"))).toBe(false);

      // Messages bob misses while away; alice (still active) receives them live,
      // advancing alice's cursor past bob's.
      await silo.getGrain(chatRoom, "general").say("alice", "m2");
      await silo.getGrain(chatRoom, "general").say("alice", "m3");
      await flush();
      expect(texts(await silo.getGrain(chatUser, "alice").history())).toEqual(["m1", "m2", "m3"]);

      // Bob rejoins: it must resume ITS OWN subscription (cursor at m1), receiving
      // only the two it missed — not alice's already-consumed position (which would
      // yield nothing).
      await silo.getGrain(chatUser, "bob").join("general");
      await flush();
      expect(texts(await silo.getGrain(chatUser, "bob").history())).toEqual(["m2", "m3"]);
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
