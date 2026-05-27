import { defineGrain } from "@tsva/core/define-grain";
import type { StreamHandler } from "@tsva/core/stream";
import { CHAT, type ChatMessage, type IChatUser } from "@tsva/example-chat/interfaces";

/**
 * A chat member. `join` subscribes to the room stream — or, if this activation
 * already has a durable subscription (e.g. after deactivating while idle and
 * being reactivated), resumes it from the saved cursor so no messages are lost.
 *
 * `getSubscriptions()` is scoped to *this* grain by the runtime, so when many
 * members share one room stream each resumes its own subscription, not a
 * neighbour's. `onNext` mutates `received` with no lock — proof it runs as a
 * turn on this activation.
 */
export const ChatUserGrain = defineGrain<IChatUser>("ChatUser", (ctx) => {
  const received: ChatMessage[] = [];

  const handler = (): StreamHandler<ChatMessage> => ({
    onNext: async (message) => void received.push(message),
  });

  return {
    join: async (room: string): Promise<void> => {
      const stream = ctx.runtime.getStreamProvider().getStream<ChatMessage>(CHAT, room);
      const mine = await stream.getSubscriptions();
      if (mine.length > 0) await mine[0]!.resume(handler());
      else await stream.subscribe(handler());
    },

    history: async (): Promise<ChatMessage[]> => [...received], // a snapshot — never leak the live internal array
  };
});
