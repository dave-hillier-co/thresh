import { defineGrainInterface } from "@tsva/core/grain-interface";
import type { GrainWithStringKey } from "@tsva/core/key-kinds";

export interface ChatMessage {
  from: string;
  text: string;
}

/** A room: anyone can say something; the text fans out to every member. */
export interface IChatRoom extends GrainWithStringKey {
  say(from: string, text: string): Promise<void>;
}

/** A member: joins a room and accumulates the messages it receives. */
export interface IChatUser extends GrainWithStringKey {
  join(room: string): Promise<void>;
  history(): Promise<ChatMessage[]>;
}

export const IChatRoom = defineGrainInterface<IChatRoom>("example.IChatRoom");

export const IChatUser = defineGrainInterface<IChatUser>("example.IChatUser", {
  options: { history: { readOnly: true } },
});

/** Stream namespace messages are published to; the stream key is the room. */
export const CHAT = "chat";
