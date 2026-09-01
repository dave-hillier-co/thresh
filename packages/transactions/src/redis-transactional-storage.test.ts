import { randomUUID } from "node:crypto";
import { afterAll, beforeEach, describe, expect, it } from "vitest";
import { createClient } from "redis";
import { InconsistentStateError } from "@thresh/core/errors";
import { GrainId } from "@thresh/core/grain-id";
import { serializeValue } from "@thresh/core/value-codec";
import { RedisTransactionalStorage } from "@thresh/transactions/redis-transactional-storage";
import { EMPTY_METADATA } from "@thresh/transactions/transactional-storage-apply";

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

async function deleteAll(c: Client, match: string): Promise<void> {
  for await (const keys of c.scanIterator({ MATCH: match })) {
    if (keys.length > 0) await c.del(keys);
  }
}

const client = await reachable(REDIS_URL);
const prefix = `thresh-test:tx:${randomUUID()}`;
const id = new GrainId("Account", "a1");
const makeStorage = () => new RedisTransactionalStorage(client!, { keyPrefix: prefix });

afterAll(async () => {
  if (client === undefined) return;
  await deleteAll(client, `${prefix}*`);
  await client.destroy();
});

describe.skipIf(client === undefined)("RedisTransactionalStorage", () => {
  beforeEach(async () => {
    await deleteAll(client!, `${prefix}*`);
  });

  it("loads an empty response for a never-written record", async () => {
    const response = await makeStorage().load("balance", id);
    expect(response).toEqual({
      etag: undefined,
      committedState: undefined,
      committedSequenceId: 0,
      metadata: EMPTY_METADATA,
      pendingStates: [],
    });
  });

  it("stores and reloads a committed state through a fresh instance", async () => {
    const storage = makeStorage();
    const etag = await storage.store(
      "balance",
      id,
      undefined,
      { timeStamp: 1, commitRecords: {} },
      [{ sequenceId: 1, transactionId: "t1", timeStamp: 1, state: { cents: 5 } }],
      1,
      undefined,
    );
    const response = await new RedisTransactionalStorage(client!, { keyPrefix: prefix }).load(
      "balance",
      id,
    );
    expect(response.etag).toBe(etag);
    expect(response.committedState).toEqual({ cents: 5 });
    expect(response.committedSequenceId).toBe(1);
  });

  it("rejects a stale store with a mismatched etag", async () => {
    const storage = makeStorage();
    await storage.store(
      "balance",
      id,
      undefined,
      { timeStamp: 1, commitRecords: {} },
      [],
      undefined,
      undefined,
    );
    await expect(
      storage.store(
        "balance",
        id,
        "wrong-etag",
        { timeStamp: 2, commitRecords: {} },
        [],
        undefined,
        undefined,
      ),
    ).rejects.toBeInstanceOf(InconsistentStateError);
  });
});

// Issue #64: RedisTransactionalStorage partitions only by keyPrefix, so two
// services sharing one Redis with default prefixes silently share
// transactional records — the same collision #59 fixed for grain state.
// Redis has no ALTER, so this is a deliberate upgrade break (mirrors #59's
// RedisGrainStorage call): a record written under the old key shape orphans
// on upgrade. A Redis-backed transactional deployment should drain in-flight
// transactions before upgrading (todo.md / docs/deviations.md).
describe.skipIf(client === undefined)(
  "RedisTransactionalStorage service partitioning (issue #64)",
  () => {
    beforeEach(async () => {
      await deleteAll(client!, `${prefix}*`);
    });

    it("keeps two service ids' records independent for the same grain and state", async () => {
      const alpha = new RedisTransactionalStorage(client!, {
        keyPrefix: prefix,
        serviceId: "alpha",
      });
      await alpha.store(
        "balance",
        id,
        undefined,
        { timeStamp: 1, commitRecords: {} },
        [{ sequenceId: 1, transactionId: "t1", timeStamp: 1, state: { cents: 1 } }],
        1,
        undefined,
      );

      const beta = new RedisTransactionalStorage(client!, { keyPrefix: prefix, serviceId: "beta" });
      const betaLoad = await beta.load("balance", id);
      expect(betaLoad.etag).toBeUndefined();
      expect(betaLoad.committedSequenceId).toBe(0);

      // No cross-service etag conflict: beta's blind store must succeed.
      await expect(
        beta.store(
          "balance",
          id,
          undefined,
          { timeStamp: 1, commitRecords: {} },
          [{ sequenceId: 1, transactionId: "t2", timeStamp: 1, state: { cents: 2 } }],
          1,
          undefined,
        ),
      ).resolves.toBeDefined();

      const alphaLoad = await alpha.load("balance", id);
      expect(alphaLoad.committedState).toEqual({ cents: 1 });
      const betaReload = await beta.load("balance", id);
      expect(betaReload.committedState).toEqual({ cents: 2 });
    });

    it("orphans a record written under the pre-service-dimension key shape (deliberate upgrade break)", async () => {
      // Old shape had no {serviceId} segment.
      await client!.hSet(`${prefix}:tx:${id.toString()}/balance`, {
        etag: "old-etag",
        data: serializeValue({
          committedState: { cents: 9 },
          committedSequenceId: 1,
          metadata: EMPTY_METADATA,
          pendingStates: [],
        }),
      });

      const defaultConfigured = new RedisTransactionalStorage(client!, { keyPrefix: prefix });
      const response = await defaultConfigured.load("balance", id);
      expect(response.etag).toBeUndefined();
      expect(response.committedSequenceId).toBe(0);
    });
  },
);
