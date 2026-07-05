// Ported from dotnet/orleans test/Grains/TestGrains/EventSourcing/{ChatGrain,ChatFormat,ChatEvents}.cs @ v10.1.0 (MIT).
// Upstream is a `JournaledGrain<XDocument, IChatEvent>`: every mutation is a
// replayable event applied to an in-memory `XDocument`, and `GetChat` returns
// `TentativeState`. This framework has no journaled-grain machinery
// (GAP-EVENT-SOURCING); the tests only assert on the rendered document's
// content though, so this keeps the same document shape and truncation rule
// (`MaxNumPosts`) with plain in-memory state instead of a real event log.
import { grain } from "@tsva/core/decorators";
import { Grain } from "@tsva/core/grain";
import type { Guid } from "@tsva/core/guid";
import { IChatGrain } from "@tsva/parity/grains/interfaces/chat-grain-interfaces";

export { IChatGrain };

export const MAX_NUM_POSTS = 100;

interface Post {
  guid: string;
  user: string;
  timestamp: string;
  text: string;
}

function renderPost(post: Post): string {
  return (
    `    <post id="${post.guid}">\n` +
    `      <user>${post.user}</user>\n` +
    `      <timestamp>${post.timestamp}</timestamp>\n` +
    `      <text>${post.text}</text>\n` +
    `    </post>`
  );
}

function renderDocument(createdIso: string, posts: readonly Post[]): string {
  const postsElement =
    posts.length === 0
      ? "  <posts />"
      : `  <posts>\n${posts.map(renderPost).join("\n")}\n  </posts>`;
  return (
    `<!--This chat room was created by TestGrains.ChatGrain-->\n` +
    `<root>\n` +
    `  <created>${createdIso}</created>\n` +
    `${postsElement}\n` +
    `</root>`
  );
}

@grain({ name: "TestGrains.ChatGrain" })
export class ChatGrain extends Grain implements IChatGrain {
  private created: string | undefined;
  private posts: Post[] = [];

  override async onActivate(): Promise<void> {
    // Upstream raises a conditional `CreatedEvent` the first time the grain is
    // used, so the created timestamp is fixed for the grain's lifetime.
    this.created = new Date().toISOString();
  }

  async getChat(): Promise<string> {
    return renderDocument(this.created ?? new Date().toISOString(), this.posts);
  }

  async post(guid: Guid, user: string, text: string): Promise<void> {
    this.posts.push({ guid: guid.toString(), user, timestamp: new Date().toISOString(), text });
    if (this.posts.length > MAX_NUM_POSTS) this.posts.shift();
  }

  async delete(guid: Guid): Promise<void> {
    const key = guid.toString();
    this.posts = this.posts.filter((p) => p.guid !== key);
  }

  async edit(guid: Guid, text: string): Promise<void> {
    const key = guid.toString();
    const post = this.posts.find((p) => p.guid === key);
    if (post !== undefined) post.text = text;
  }
}
