// Ported from dotnet/orleans test/Orleans.EventSourcing.Tests/EventSourcingTests/ChatGrainTests.cs @ v10.1.0 (MIT).
import { afterAll, beforeAll, describe, expect } from "vitest";
import { orleansTest } from "@tsva/testing/orleans-test";
import { TestCluster } from "@tsva/testing/test-cluster";
import { ChatGrain, IChatGrain, MAX_NUM_POSTS } from "@tsva/parity/grains/impl/chat-grain";
import { Guid } from "@tsva/core/guid";

function postCount(content: string): number {
  return (content.match(/<post id="/g) ?? []).length;
}

function findPostText(content: string, guid: string): string | undefined {
  const match = new RegExp(`<post id="${guid}">[\\s\\S]*?<text>(.*?)<\\/text>`).exec(content);
  return match?.[1];
}

describe("Tester.EventSourcingTests.ChatGrainTests", () => {
  let cluster: TestCluster;

  beforeAll(async () => {
    cluster = await TestCluster.start({
      initialSilos: 2,
      grains: [{ ctor: ChatGrain, interfaces: [IChatGrain] }],
    });
  });

  afterAll(async () => {
    await cluster.dispose();
  });

  const getChatGrain = () => cluster.getGrain(IChatGrain, `Chatroom-${Guid.newGuid().toString()}`);

  orleansTest("Tester.EventSourcingTests.ChatGrainTests.Init", async () => {
    const chat = getChatGrain();

    const content = await chat.getChat();

    const expectedPrefix = `<!--This chat room was created by TestGrains.ChatGrain-->\n<root>\n  <created>`;
    const expectedSuffix = `</created>\n  <posts />\n</root>`;

    expect(content.startsWith(expectedPrefix)).toBe(true);
    expect(content.endsWith(expectedSuffix)).toBe(true);
  });

  orleansTest("Tester.EventSourcingTests.ChatGrainTests.PostThenDelete", async () => {
    const chat = getChatGrain();
    const guid = Guid.newGuid();

    await chat.post(guid, "Famous Athlete", "I am retiring");

    expect(postCount(await chat.getChat())).toBe(1);

    await chat.delete(guid);

    expect(postCount(await chat.getChat())).toBe(0);
  });

  orleansTest("Tester.EventSourcingTests.ChatGrainTests.PostThenEdit", async () => {
    const chat = getChatGrain();
    const guid = Guid.newGuid();

    await chat.post(Guid.newGuid(), "asdf", "asdf");
    await chat.post(guid, "Famous Athlete", "I am retiring");
    await chat.post(Guid.newGuid(), "456", "456");

    expect(postCount(await chat.getChat())).toBe(3);

    await chat.edit(guid, "I am not retiring");

    const content = await chat.getChat();
    expect(postCount(content)).toBe(3);
    expect(findPostText(content, guid.toString())).toBe("I am not retiring");
  });

  orleansTest("Tester.EventSourcingTests.ChatGrainTests.Truncate", async () => {
    const chat = getChatGrain();

    for (let i = 0; i < MAX_NUM_POSTS + 10; i += 1) {
      await chat.post(Guid.newGuid(), String(i), String(i));
    }

    expect(postCount(await chat.getChat())).toBe(MAX_NUM_POSTS);
  });
});
