import { GrainId } from "@thresh/core/grain-id";
import { SiloAddress } from "@thresh/core/silo-address";
import { FakeTimeProvider } from "@thresh/core/test-support/fake-time-provider";
import { InProcessNetwork } from "@thresh/messaging/in-process-transport";
import { createSilo } from "@thresh/hosting/silo-builder";
import { ChatRoomGrain } from "@thresh/example-chat/chat-room-grain";
import { ChatUserGrain } from "@thresh/example-chat/chat-user-grain";
import { IChatRoom, IChatUser, type ChatMessage } from "@thresh/example-chat/interfaces";

export interface ChatDemoResult {
  /** Messages each member received during the fan-out phase. */
  fanOut: Record<string, ChatMessage[]>;
  /** Messages bob missed while deactivated and recovered by resuming its own subscription. */
  bobResumedWhileAway: ChatMessage[];
}

const tick = () => new Promise((r) => setTimeout(r, 0));

/**
 * Runs the chat room end-to-end against in-memory streams + the in-process
 * transport. Shows (1) one message fanning out to every member, and (2) a member
 * that deactivated while idle resuming its own subscription on reactivation and
 * recovering exactly the messages it missed — using a fake clock to force the
 * idle collection deterministically.
 */
export async function runChatDemo(): Promise<ChatDemoResult> {
  const time = new FakeTimeProvider();
  const local = new SiloAddress("silo-0", "uid-0", "silo-0:11111");

  const silo = createSilo({
    clusterId: "chat",
    local,
    time,
    collectionAgeSeconds: 30,
    collectionIntervalSeconds: 10,
  })
    .useStaticMembership([local])
    .useInProcessTransport(new InProcessNetwork())
    .useMemoryStreams()
    .registerGrain(ChatRoomGrain, { interfaces: [IChatRoom] })
    .registerGrain(ChatUserGrain, { interfaces: [IChatUser] })
    .build();

  await silo.start();
  try {
    const members = ["alice", "bob", "carol"];
    for (const name of members) await silo.getGrain(IChatUser, name).join("general");

    await silo.getGrain(IChatRoom, "general").say("alice", "hello everyone");
    await tick();

    const fanOut: Record<string, ChatMessage[]> = {};
    for (const name of members) fanOut[name] = await silo.getGrain(IChatUser, name).history();

    // Keep alice active across a sweep; let bob fall idle and be collected.
    time.advance(25_000);
    await silo.getGrain(IChatUser, "alice").history();
    time.advance(20_000);
    await tick();
    const bobCollected = !silo.isActive(new GrainId("ChatUser", "bob"));

    // Two messages land while bob is away; alice still receives them live.
    await silo.getGrain(IChatRoom, "general").say("alice", "you around?");
    await silo.getGrain(IChatRoom, "general").say("carol", "guess not");
    await tick();

    // Bob comes back and resumes its own subscription from where it left off.
    await silo.getGrain(IChatUser, "bob").join("general");
    await tick();
    const bobHistory = await silo.getGrain(IChatUser, "bob").history();

    return {
      fanOut,
      bobResumedWhileAway: bobCollected ? bobHistory : [],
    };
  } finally {
    await silo.stop();
  }
}
