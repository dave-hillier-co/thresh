import { defineGrain } from "@thresh/core/define-grain";
import { CHAT, type ChatMessage } from "@thresh/example-chat/interfaces";

/**
 * A chat room is a pure fan-out point: `say` publishes to the room's telemetry
 * stream and every member that subscribed receives it. The room holds no member
 * list — the stream is the membership.
 */
export const ChatRoomGrain = defineGrain("ChatRoom", (ctx) => ({
  say: async (from: string, text: string): Promise<void> => {
    await ctx.runtime
      .getStreamProvider()
      .getStream<ChatMessage>(CHAT, ctx.id.key)
      .publish({ from, text });
  },
}));
