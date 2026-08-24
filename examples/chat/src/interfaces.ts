export interface ChatMessage {
  from: string;
  text: string;
}

/** Stream namespace messages are published to; the stream key is the room. */
export const CHAT = "chat";
