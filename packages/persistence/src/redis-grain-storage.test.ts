import { randomUUID } from "node:crypto";
import { afterAll, beforeEach, describe, expect, it } from "vitest";
import { createClient } from "redis";
import { InconsistentStateError } from "@thresh/core/errors";
import { GrainId } from "@thresh/core/grain-id";
import type { GrainStorage } from "@thresh/core/grain-storage";
import { PersistentStateImpl } from "@thresh/persistence/persistent-state-impl";
import { RedisGrainStorage } from "@thresh/persistence/redis-grain-storage";

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
// A unique prefix isolates this run from anything else in the shared Redis.
const prefix = `thresh-test:storage:${randomUUID()}`;

interface Balance {
  cents: number;
}

const id = new GrainId("Account", "a1");
const makeStorage = (): GrainStorage => new RedisGrainStorage(client!, { keyPrefix: prefix });
const makeState = (storage: GrainStorage, name = "balance") =>
  new PersistentStateImpl<Balance>(name, id, storage, () => ({ cents: 0 }));

// node-redis v5 scanIterator yields batches of keys, not individual keys.
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

describe.skipIf(client === undefined)("RedisGrainStorage", () => {
  beforeEach(async () => {
    await deleteAll(client!, `${prefix}*`);
  });

  it("starts empty with a default value", async () => {
    const state = makeState(makeStorage());
    await state.read();
    expect(state.exists).toBe(false);
    expect(state.value.cents).toBe(0);
    expect(state.etag).toBeUndefined();
  });

  it("persists a write and reloads it on a fresh activation", async () => {
    const storage = makeStorage();
    const first = makeState(storage);
    first.value.cents = 100;
    await first.write();
    expect(first.exists).toBe(true);
    expect(first.etag).toBeDefined();

    // A fresh storage instance proves it round-trips through Redis, not memory.
    const reactivated = makeState(makeStorage());
    await reactivated.read();
    expect(reactivated.value.cents).toBe(100);
  });

  it("raises InconsistentStateError when a stale writer loses the race", async () => {
    const storage = makeStorage();
    const a = makeState(storage);
    await a.read();
    a.value.cents = 1;
    await a.write();

    const b = makeState(makeStorage());
    await b.read(); // reads a's etag

    a.value.cents = 2;
    await a.write(); // bumps the etag

    b.value.cents = 3;
    await expect(b.write()).rejects.toBeInstanceOf(InconsistentStateError);
  });

  it("rejects a blind write over an existing record", async () => {
    const a = makeState(makeStorage());
    a.value.cents = 1;
    await a.write();

    const blind = makeState(makeStorage()); // never read -> no etag
    blind.value.cents = 9;
    await expect(blind.write()).rejects.toBeInstanceOf(InconsistentStateError);
  });

  it("clears the record and resets to the default", async () => {
    const storage = makeStorage();
    const state = makeState(storage);
    state.value.cents = 50;
    await state.write();
    await state.clear();
    expect(state.exists).toBe(false);
    expect(state.value.cents).toBe(0);

    const reloaded = makeState(makeStorage());
    await reloaded.read();
    expect(reloaded.exists).toBe(false);
  });

  it("round-trips runtime value types (Date) through the codec", async () => {
    interface WithDate {
      at: Date;
    }
    const when = new Date("2026-05-25T09:00:00.000Z");
    const a = new PersistentStateImpl<WithDate>("dated", id, makeStorage(), () => ({
      at: new Date(0),
    }));
    a.value.at = when;
    await a.write();

    const b = new PersistentStateImpl<WithDate>("dated", id, makeStorage(), () => ({
      at: new Date(0),
    }));
    await b.read();
    expect(b.value.at).toBeInstanceOf(Date);
    expect(b.value.at.getTime()).toBe(when.getTime());
  });

  it("keeps named states independent", async () => {
    const balance = makeState(makeStorage(), "balance");
    balance.value.cents = 5;
    await balance.write();

    const limit = makeState(makeStorage(), "limit");
    await limit.read();
    expect(limit.exists).toBe(false);
  });

  // GAP-CANCELLATION-STORAGE (issue #18): an ambient signal threads through to
  // node-redis's own `withAbortSignal`, so an already-aborted signal cancels
  // the call for real rather than merely abandoning the wait for it.
  describe("ambient AbortSignal (issue #18)", () => {
    it("rejects read() when the signal is already aborted", async () => {
      const state = makeState(makeStorage());
      const controller = new AbortController();
      controller.abort();
      await expect(state.read(controller.signal)).rejects.toBeDefined();
    });

    it("still succeeds when the signal never fires", async () => {
      const state = makeState(makeStorage());
      const controller = new AbortController();
      state.value.cents = 42;
      await state.write(controller.signal);
      expect(state.exists).toBe(true);

      const reloaded = makeState(makeStorage());
      await reloaded.read(controller.signal);
      expect(reloaded.value.cents).toBe(42);
    });
  });
});

// Issue #59: Orleans' Redis provider prefixes every key with the ServiceId
// (`{ServiceId}/state/`, `RedisGrainStorage.cs:56`), so several clusters can
// share one Redis and stay partitioned. Thresh's `keyPrefix` is a deployment
// namespace, not a service identity, and nothing partitioned by service.
describe.skipIf(client === undefined)("RedisGrainStorage service partitioning (issue #59)", () => {
  beforeEach(async () => {
    await deleteAll(client!, `${prefix}*`);
  });

  it("keeps two service ids on separate keys for the same grain and state", async () => {
    const alpha = new RedisGrainStorage(client!, { keyPrefix: prefix, serviceId: "alpha" });
    const a = makeState(alpha);
    a.value.cents = 1;
    await a.write();

    // A second cluster has never seen this grain: its read must find nothing,
    // and its blind write must succeed, because alpha's key is not its key.
    const beta = new RedisGrainStorage(client!, { keyPrefix: prefix, serviceId: "beta" });
    const b = makeState(beta);
    await b.read();
    expect(b.exists).toBe(false);
    b.value.cents = 2;
    await b.write();

    const reAlpha = makeState(
      new RedisGrainStorage(client!, { keyPrefix: prefix, serviceId: "alpha" }),
    );
    await reAlpha.read();
    expect(reAlpha.value.cents).toBe(1);
    const reBeta = makeState(
      new RedisGrainStorage(client!, { keyPrefix: prefix, serviceId: "beta" }),
    );
    await reBeta.read();
    expect(reBeta.value.cents).toBe(2);
  });
});
