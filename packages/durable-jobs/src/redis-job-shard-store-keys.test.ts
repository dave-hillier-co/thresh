import { describe, expect, it } from "vitest";
import { GrainId } from "@thresh/core/grain-id";
import { RedisJobShardStore, type RedisClient } from "@thresh/durable-jobs/redis-job-shard-store";

/** A client that records every key it is addressed with, and answers nothing. */
function recordingClient(): { client: RedisClient; keys: string[] } {
  const keys: string[] = [];
  const chain = {
    hSet(key: string) {
      keys.push(key);
      return chain;
    },
    sAdd(key: string) {
      keys.push(key);
      return chain;
    },
    exec: async () => [],
  };
  const client = {
    multi: () => chain,
    hGetAll: async (key: string) => {
      keys.push(key);
      return {};
    },
    hDel: async (key: string) => {
      keys.push(key);
      return 0;
    },
    eval: async (_script: string, options: { keys: string[] }) => {
      keys.push(...options.keys);
      return null;
    },
  } as unknown as RedisClient;
  return { client, keys };
}

async function touchEveryKey(store: RedisJobShardStore): Promise<void> {
  await store.persistAdd({
    id: "j",
    name: "job",
    dueTime: new Date(0),
    target: new GrainId("t", "g"),
    shardKey: 5,
    metadata: {},
  });
  await store.readJobs(5);
  await store.persistRemove(5, "j");
  await store.claimShard(5, "o", { maxAdoptedCount: 3 });
  await store.releaseShard(5, "o");
}

describe("RedisJobShardStore keys", () => {
  it("carry the service id after the prefix", async () => {
    const { client, keys } = recordingClient();
    await touchEveryKey(new RedisJobShardStore(client, { keyPrefix: "p", serviceId: "svc" }));
    expect(new Set(keys)).toEqual(new Set(["p:svc:job:s:5", "p:svc:job:shards", "p:svc:job:m:5"]));
  });

  it('default the service id to "default", and partition two services', async () => {
    const a = recordingClient();
    await touchEveryKey(new RedisJobShardStore(a.client));
    expect(a.keys.every((k) => k.startsWith("thresh:default:job:"))).toBe(true);
    const b = recordingClient();
    await touchEveryKey(new RedisJobShardStore(b.client, { serviceId: "other" }));
    expect(b.keys.some((k) => a.keys.includes(k))).toBe(false);
  });
});
