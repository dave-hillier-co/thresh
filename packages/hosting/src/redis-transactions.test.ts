import { randomUUID } from "node:crypto";
import { afterAll, describe, expect, it } from "vitest";
import { createClient } from "redis";
import { grain, transactionalState } from "@thresh/core/decorators";
import { Grain } from "@thresh/core/grain";
import { defineGrainInterface } from "@thresh/core/grain-interface";
import { GrainId } from "@thresh/core/grain-id";
import type { GrainType } from "@thresh/core/grain-type";
import type { GrainKey } from "@thresh/core/key-kinds";
import { SiloAddress } from "@thresh/core/silo-address";
import type { TransactionalState } from "@thresh/core/transactional-state";
import { InProcessNetwork } from "@thresh/messaging/in-process-transport";
import { RedisTransactionalStorage } from "@thresh/transactions/redis-transactional-storage";
import { createSilo } from "@thresh/hosting/silo-builder";

const REDIS_URL = process.env.REDIS_URL ?? "redis://localhost:6379";
type Client = ReturnType<typeof createClient>;

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

const admin = await reachable(REDIS_URL);
const keyPrefix = `thresh-test:tx:${randomUUID()}`;

afterAll(async () => {
  if (admin === undefined) return;
  for await (const keys of admin.scanIterator({ MATCH: `${keyPrefix}*` })) {
    if (keys.length > 0) await admin.del(keys);
  }
  await admin.destroy();
});

interface Balance {
  cents: number;
}

interface Account extends GrainKey<string> {
  credit(cents: number): Promise<void>;
  balance(): Promise<number>;
}

const Account = defineGrainInterface<Account>("RedisTxAccount", {
  options: { credit: { transaction: "create" }, balance: { transaction: "createOrJoin" } },
});

@grain()
class AccountGrain extends Grain implements Account {
  @transactionalState("balance", { initial: (): Balance => ({ cents: 0 }) })
  private bal!: TransactionalState<Balance>;

  async credit(cents: number): Promise<void> {
    await this.bal.performUpdate((s) => {
      s.cents += cents;
    });
  }

  async balance(): Promise<number> {
    return this.bal.performRead((s) => s.cents);
  }
}

const local = new SiloAddress("silo-0", "uid-0", "silo-0:11111");

function buildSilo() {
  return createSilo({ clusterId: "redis-tx", local })
    .useStaticMembership([local])
    .useInProcessTransport(new InProcessNetwork())
    .addRedisTransactionalStorage("default", { url: REDIS_URL, keyPrefix })
    .registerGrain(AccountGrain, { interfaces: [Account] })
    .build();
}

describe.skipIf(admin === undefined)("Redis transactional storage", () => {
  it("prepares, commits, and loads the committed version", async () => {
    const storage = new RedisTransactionalStorage(admin!, { keyPrefix });
    const grainId = new GrainId("Acct" as GrainType, randomUUID());
    const meta = { timeStamp: 1, commitRecords: {} };

    const etag1 = await storage.store(
      "s",
      grainId,
      undefined,
      meta,
      [{ sequenceId: 1, transactionId: "t1", timeStamp: 1, state: { cents: 500 } }],
      undefined,
      undefined,
    );
    // Prepared but not committed: committed version is still empty.
    let loaded = await storage.load("s", grainId);
    expect(loaded.committedSequenceId).toBe(0);
    expect(loaded.pendingStates).toHaveLength(1);

    const etag2 = await storage.store("s", grainId, etag1, meta, [], 1, undefined);
    expect(etag2).not.toBe(etag1);
    loaded = await storage.load("s", grainId);
    expect(loaded.committedState).toEqual({ cents: 500 });
    expect(loaded.committedSequenceId).toBe(1);
    expect(loaded.pendingStates).toHaveLength(0);
  });

  it("rejects a store with a stale etag", async () => {
    const storage = new RedisTransactionalStorage(admin!, { keyPrefix });
    const grainId = new GrainId("Acct" as GrainType, randomUUID());
    const meta = { timeStamp: 1, commitRecords: {} };
    const etag = await storage.store("s", grainId, undefined, meta, [], undefined, undefined);
    await storage.store("s", grainId, etag, meta, [], undefined, undefined);
    // Reusing the first etag is stale.
    await expect(storage.store("s", grainId, etag, meta, [], undefined, undefined)).rejects.toThrow(
      /etag/i,
    );
  });

  // GAP-CANCELLATION-STORAGE (issue #18): an ambient signal threads through to
  // node-redis's own `withAbortSignal`, so an already-aborted signal cancels
  // the call for real rather than merely abandoning the wait for it.
  describe("ambient AbortSignal (issue #18)", () => {
    it("rejects load() when the signal is already aborted", async () => {
      const storage = new RedisTransactionalStorage(admin!, { keyPrefix });
      const grainId = new GrainId("Acct" as GrainType, randomUUID());
      const controller = new AbortController();
      controller.abort();
      await expect(storage.load("s", grainId, controller.signal)).rejects.toBeDefined();
    });

    it("rejects store() when the signal is already aborted", async () => {
      const storage = new RedisTransactionalStorage(admin!, { keyPrefix });
      const grainId = new GrainId("Acct" as GrainType, randomUUID());
      const meta = { timeStamp: 1, commitRecords: {} };
      const controller = new AbortController();
      controller.abort();
      await expect(
        storage.store("s", grainId, undefined, meta, [], undefined, undefined, controller.signal),
      ).rejects.toBeDefined();
    });

    it("still succeeds when the signal never fires", async () => {
      const storage = new RedisTransactionalStorage(admin!, { keyPrefix });
      const grainId = new GrainId("Acct" as GrainType, randomUUID());
      const meta = { timeStamp: 1, commitRecords: {} };
      const controller = new AbortController();
      const etag = await storage.store(
        "s",
        grainId,
        undefined,
        meta,
        [],
        undefined,
        undefined,
        controller.signal,
      );
      const loaded = await storage.load("s", grainId, controller.signal);
      expect(loaded.etag).toBe(etag);
    });
  });

  it("keeps committed transactional state across a silo restart via Redis", async () => {
    const first = buildSilo();
    await first.start();
    await first.getGrain(Account, "acc-1").credit(100);
    await first.getGrain(Account, "acc-1").credit(50);
    await first.stop();

    const restarted = buildSilo();
    await restarted.start();
    try {
      expect(await restarted.getGrain(Account, "acc-1").balance()).toBe(150);
    } finally {
      await restarted.stop();
    }
  });
});
