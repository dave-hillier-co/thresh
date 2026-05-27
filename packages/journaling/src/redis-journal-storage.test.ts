import { randomUUID } from "node:crypto";
import { afterAll, beforeEach, describe, expect, it } from "vitest";
import { createClient } from "redis";
import { InconsistentStateError } from "@tsva/core/errors";
import { GrainId } from "@tsva/core/grain-id";
import { DurableValueImpl } from "@tsva/journaling/durable-value-impl";
import { RedisJournalStorage } from "@tsva/journaling/redis-journal-storage";
import { StateMachineManagerImpl } from "@tsva/journaling/state-machine-manager-impl";

const REDIS_URL = process.env.REDIS_URL ?? "redis://localhost:6379";

type Client = ReturnType<typeof createClient>;

/** Probe Redis once at load time so the suite skips cleanly without it. */
async function reachable(url: string): Promise<Client | undefined> {
  const probe = createClient({ url });
  probe.on("error", () => {});
  try {
    await probe.connect();
    await probe.ping();
    return probe;
  } catch {
    try {
      await probe.destroy();
    } catch {
      /* never connected */
    }
    return undefined;
  }
}

const client = await reachable(REDIS_URL);
const prefix = `tsva-test:journal:${randomUUID()}`;

const id = new GrainId("Agg", "g1");
const makeStorage = () => new RedisJournalStorage(client!, { keyPrefix: prefix });

async function deleteAll(c: Client, match: string): Promise<void> {
  for await (const keys of c.scanIterator({ MATCH: match })) {
    if (keys.length > 0) await c.del(keys);
  }
}

afterAll(async () => {
  if (client === undefined) return;
  await deleteAll(client, `${prefix}*`);
  await client.destroy();
});

describe.skipIf(client === undefined)("RedisJournalStorage", () => {
  beforeEach(async () => {
    await deleteAll(client!, `${prefix}*`);
  });

  it("reads an empty log as no version", async () => {
    const segment = await makeStorage().read("journal", id);
    expect(segment.entries).toEqual([]);
    expect(segment.version).toBeUndefined();
  });

  it("appends and round-trips through a fresh client", async () => {
    const v1 = await makeStorage().append("journal", id, ["a", "b"], undefined);
    expect(v1).toBe(1);
    const v2 = await makeStorage().append("journal", id, ["c"], 1);
    expect(v2).toBe(2);

    const segment = await makeStorage().read("journal", id);
    expect(segment.entries).toEqual(["a", "b", "c"]);
    expect(segment.version).toBe(2);
  });

  it("raises InconsistentStateError on a stale append", async () => {
    await makeStorage().append("journal", id, ["a"], undefined);
    await expect(makeStorage().append("journal", id, ["b"], undefined)).rejects.toBeInstanceOf(
      InconsistentStateError,
    );
    await expect(makeStorage().append("journal", id, ["b"], 5)).rejects.toBeInstanceOf(
      InconsistentStateError,
    );
  });

  it("replace truncates the log and bumps the version", async () => {
    await makeStorage().append("journal", id, ["a", "b", "c"], undefined);
    const v = await makeStorage().replace("journal", id, ["snap"], 1);
    expect(v).toBe(2);
    const segment = await makeStorage().read("journal", id);
    expect(segment.entries).toEqual(["snap"]);
    expect(segment.version).toBe(2);
  });

  it("clears both keys", async () => {
    await makeStorage().append("journal", id, ["a"], undefined);
    await makeStorage().clear("journal", id);
    const segment = await makeStorage().read("journal", id);
    expect(segment.entries).toEqual([]);
    expect(segment.version).toBeUndefined();
  });

  it("round-trips a full manager + structure through Redis", async () => {
    const a = new StateMachineManagerImpl("journal", id, makeStorage());
    const av = new DurableValueImpl<number>("v", a);
    a.register(av);
    await a.replay();
    await av.set(1);
    await av.set(2);

    const b = new StateMachineManagerImpl("journal", id, makeStorage());
    const bv = new DurableValueImpl<number>("v", b);
    b.register(bv);
    await b.replay();
    expect(bv.value).toBe(2);
  });
});
