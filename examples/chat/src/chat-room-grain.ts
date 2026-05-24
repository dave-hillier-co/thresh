import { grain } from "@tsva/core/decorators";
import { Grain } from "@tsva/core/grain";
import { CHAT, type ChatMessage, type IChatRoom } from "@tsva/example-chat/interfaces";

/**
 * A chat room is a pure fan-out point: `say` publishes to the room's telemetry
 * stream and every member that subscribed receives it. The room holds no member
 * list — the stream is the membership.
 */
@grain()
export class ChatRoomGrain extends Grain implements IChatRoom {
  async say(from: string, text: string): Promise<void> {
    await this.runtime
      .getStreamProvider()
      .getStream<ChatMessage>(CHAT, this.id.key)
      .publish({ from, text });
  }
}
