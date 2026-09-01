import { randomUUID } from "node:crypto";
import { afterAll, beforeEach, describe, expect, it } from "vitest";
import { createClient } from "redis";
import { InconsistentStateError } from "@thresh/core/errors";
import { GrainId } from "@thresh/core/grain-id";
import { DurableValueImpl } from "@thresh/journaling/durable-value-impl";
import { RedisJournalStorage } from "@thresh/journaling/redis-journal-storage";
import { StateMachineManagerImpl } from "@thresh/journaling/state-machine-manager-impl";

const REDIS_URL = process.env.REDIS_URL ?? "redis://localhost:6379";

type Client = ReturnType<typeof createClient>;

/** Probe Redis once at load time so the suite skips cleanly without it. */
async function reachable(url: string): Promise<Client | undefined> {
  // `reconnectStrategy: false` is load-bearing, not tidiness: node-redis retries a refused
  // connection forever by default, so `connect()` below never settles when no Redis is
  // listening and this module-load probe hangs the whole suite instead of skipping it.
  const probe = createClient({ url, socket: { reconnectStrategy: false, connectTimeout: 500 } });
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
const prefix = `thresh-test:journal:${randomUUID()}`;

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

  // GAP-CANCELLATION-STORAGE (issue #18): an ambient signal threads through to
  // node-redis's own `withAbortSignal`, so an already-aborted signal cancels
  // the call for real rather than merely abandoning the wait for it.
  describe("ambient AbortSignal (issue #18)", () => {
    it("rejects read() when the signal is already aborted", async () => {
      const controller = new AbortController();
      controller.abort();
      await expect(makeStorage().read("journal", id, controller.signal)).rejects.toBeDefined();
    });

    it("rejects append() when the signal is already aborted", async () => {
      const controller = new AbortController();
      controller.abort();
      await expect(
        makeStorage().append("journal", id, ["a"], undefined, controller.signal),
      ).rejects.toBeDefined();
    });

    it("still succeeds when the signal never fires", async () => {
      const controller = new AbortController();
      await makeStorage().append("journal", id, ["a"], undefined, controller.signal);
      const segment = await makeStorage().read("journal", id, controller.signal);
      expect(segment.entries).toEqual(["a"]);
    });
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

// Issue #64: RedisJournalStorage partitions only by keyPrefix, so two
// services sharing one Redis with default prefixes silently share journal
// logs — the same collision #59 fixed for grain state. Redis has no ALTER, so
// this is a deliberate upgrade break (mirrors #59's RedisGrainStorage call):
// a log written under the old key shape orphans on upgrade.
describe.skipIf(client === undefined)(
  "RedisJournalStorage service partitioning (issue #64)",
  () => {
    beforeEach(async () => {
      await deleteAll(client!, `${prefix}*`);
    });

    it("keeps two service ids' logs independent for the same grain and log name", async () => {
      const alpha = new RedisJournalStorage(client!, { keyPrefix: prefix, serviceId: "alpha" });
      await alpha.append("journal", id, ["a"], undefined);

      const beta = new RedisJournalStorage(client!, { keyPrefix: prefix, serviceId: "beta" });
      const segment = await beta.read("journal", id);
      expect(segment).toEqual({ entries: [], version: undefined });

      // No cross-service version conflict: beta's blind append (expectedVersion
      // undefined) must succeed even though alpha's log already has a version.
      const v = await beta.append("journal", id, ["b"], undefined);
      expect(v).toBe(1);

      const alphaSegment = await alpha.read("journal", id);
      expect(alphaSegment.entries).toEqual(["a"]);
    });

    it("orphans a log written under the pre-service-dimension key shape (deliberate upgrade break)", async () => {
      // Old shape had no {serviceId} segment.
      await client!.rPush(`${prefix}:journal:${id.toString()}/journal`, ["a", "b"]);
      await client!.set(`${prefix}:journalver:${id.toString()}/journal`, "2");

      const defaultConfigured = new RedisJournalStorage(client!, { keyPrefix: prefix });
      const segment = await defaultConfigured.read("journal", id);
      expect(segment).toEqual({ entries: [], version: undefined });
    });
  },
);
