import { defineGrainInterface } from "@thresh/core/grain-interface";
import type { GrainKey } from "@thresh/core/key-kinds";

export interface ChatMessage {
  from: string;
  text: string;
}

/** A room: anyone can say something; the text fans out to every member. */
export type ChatRoom = GrainKey<string> & {
  say(from: string, text: string): Promise<void>;
};

/** A member: joins a room and accumulates the messages it receives. */
export type ChatUser = GrainKey<string> & {
  join(room: string): Promise<void>;
  history(): Promise<ChatMessage[]>;
};

export const chatRoom = defineGrainInterface<ChatRoom>("example.chatRoom");

export const chatUser = defineGrainInterface<ChatUser>("example.chatUser", {
  options: { history: { readOnly: true } },
});

/** Stream namespace messages are published to; the stream key is the room. */
export const CHAT = "chat";
