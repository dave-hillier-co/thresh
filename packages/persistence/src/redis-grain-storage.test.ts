import { randomUUID } from "node:crypto";
import { afterAll, beforeEach, describe, expect, it } from "vitest";
import { createClient } from "redis";
import { InconsistentStateError } from "@tsva/core/errors";
import { GrainId } from "@tsva/core/grain-id";
import type { GrainStorage } from "@tsva/core/grain-storage";
import { PersistentStateImpl } from "@tsva/persistence/persistent-state-impl";
import { RedisGrainStorage } from "@tsva/persistence/redis-grain-storage";

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
// A unique prefix isolates this run from anything else in the shared Redis.
const prefix = `tsva-test:storage:${randomUUID()}`;

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
});
