import { describe, expect, it } from "vitest";
import { transactionalState } from "@thresh/core/decorators";
import { invocationContext } from "@thresh/runtime/invocation-context";
import { TransactionAgent } from "@thresh/runtime/transaction-agent";
import { GrainId } from "@thresh/core/grain-id";
import type { GrainType } from "@thresh/core/grain-type";
import type { TimeProvider, TimerHandle } from "@thresh/core/time-provider";
import type { TransactionalState } from "@thresh/core/transactional-state";
import type { TransactionOutcome } from "@thresh/core/transaction-info";
import { MemoryTransactionalStorage } from "@thresh/transactions/memory-transactional-storage";
import {
  bindTransactionalStates,
  unbindTransactionalStates,
} from "@thresh/transactions/transactional-state-activator";
import { TransactionalStorageRegistry } from "@thresh/transactions/transactional-storage-registry";

interface Balance {
  cents: number;
}

class AccountInstance {
  @transactionalState("balance", { initial: (): Balance => ({ cents: 100 }) })
  bal!: TransactionalState<Balance>;
}

const grainId = (key: string) => new GrainId("Account" as GrainType, key);

/** Deterministic clock: time only moves when `advance` is called. */
function fakeClock(): TimeProvider & { advance(ms: number): void } {
  let current = 0;
  const timers = new Map<number, { at: number; fire: () => void }>();
  let nextId = 1;
  return {
    now: () => current,
    setTimer: (handler, delayMs): TimerHandle => {
      const id = nextId++;
      timers.set(id, { at: current + delayMs, fire: handler });
      return id;
    },
    clearTimer: (handle) => {
      timers.delete(handle as number);
    },
    advance: (ms: number) => {
      current += ms;
      for (const [id, t] of [...timers]) {
        if (t.at <= current) {
          timers.delete(id);
          t.fire();
        }
      }
    },
  };
}

const flushMicrotasks = async (): Promise<void> => {
  for (let i = 0; i < 5; i++) await Promise.resolve();
};

describe("transactional-state-activator", () => {
  it("binds a facet whose TM is unreachable and schedules its confirmation worker", async () => {
    const clock = fakeClock();
    const storage = new MemoryTransactionalStorage();
    // Seed a prepared-but-uncommitted record naming a remote TM (a crash right
    // after prepare, before commit).
    await storage.store(
      "balance",
      grainId("a"),
      undefined,
      { timeStamp: 1, commitRecords: {} },
      [
        {
          sequenceId: 1,
          transactionId: "T1",
          timeStamp: 1,
          transactionManager: { grainId: grainId("tm"), stateName: "ledger" },
          state: { cents: 250 },
        },
      ],
      undefined,
      undefined,
    );

    let calls = 0;
    const resolveStatus = async (): Promise<TransactionOutcome> => {
      calls += 1;
      throw new Error("TM unreachable");
    };

    const instance = new AccountInstance();
    const registry = new TransactionalStorageRegistry().add("default", storage);
    await bindTransactionalStates(instance, grainId("a"), registry, resolveStatus, clock);

    // One recovery attempt during load(), failed: still in-doubt.
    expect(calls).toBe(1);

    // Left running, the confirmation worker retries on its own.
    clock.advance(1_000);
    await flushMicrotasks();
    expect(calls).toBe(2);
  });

  it("unbindTransactionalStates stops the confirmation worker so it never retries again", async () => {
    const clock = fakeClock();
    const storage = new MemoryTransactionalStorage();
    await storage.store(
      "balance",
      grainId("b"),
      undefined,
      { timeStamp: 1, commitRecords: {} },
      [
        {
          sequenceId: 1,
          transactionId: "T2",
          timeStamp: 1,
          transactionManager: { grainId: grainId("tm"), stateName: "ledger" },
          state: { cents: 250 },
        },
      ],
      undefined,
      undefined,
    );

    let calls = 0;
    const resolveStatus = async (): Promise<TransactionOutcome> => {
      calls += 1;
      throw new Error("TM unreachable");
    };

    const instance = new AccountInstance();
    const registry = new TransactionalStorageRegistry().add("default", storage);
    await bindTransactionalStates(instance, grainId("b"), registry, resolveStatus, clock);
    expect(calls).toBe(1);

    // Deactivate: this must stop the facet's confirmation worker.
    unbindTransactionalStates(instance);

    // Without the unbind, this advance would fire the worker again.
    clock.advance(60_000);
    await flushMicrotasks();
    expect(calls).toBe(1);
  });
});

class LedgerInstance {
  @transactionalState("balance", {
    initial: (): Balance => ({ cents: 0 }),
    provider: "ledger",
  })
  bal!: TransactionalState<Balance>;
}

describe("a transactional state that names a storage provider (S7)", () => {
  it("reads and writes through that provider rather than the default one", async () => {
    const defaultStorage = new MemoryTransactionalStorage();
    const ledgerStorage = new MemoryTransactionalStorage();
    const id = grainId("ledger-1");
    await ledgerStorage.store(
      "balance",
      id,
      undefined,
      { timeStamp: 1, commitRecords: {} },
      [
        {
          sequenceId: 1,
          transactionId: "T0",
          timeStamp: 1,
          transactionManager: { grainId: id, stateName: "balance" },
          state: { cents: 777 },
        },
      ],
      1,
      undefined,
    );
    const registry = new TransactionalStorageRegistry()
      .add("default", defaultStorage)
      .add("ledger", ledgerStorage);
    const instance = new LedgerInstance();
    await bindTransactionalStates(instance, id, registry);

    const agent = new TransactionAgent(fakeClock());
    const tx = agent.startTransaction();
    await invocationContext.run({ senderId: undefined, ownerId: undefined, transaction: tx }, () =>
      instance.bal.performUpdate((s) => {
        s.cents += 1;
      }),
    );
    await agent.resolve(tx);

    expect((await ledgerStorage.load("balance", id)).committedState).toEqual({ cents: 778 });
    expect((await defaultStorage.load("balance", id)).committedState).toBeUndefined();
    unbindTransactionalStates(instance);
  });

  it("fails to bind when the named provider is not registered", async () => {
    const registry = new TransactionalStorageRegistry().add(
      "default",
      new MemoryTransactionalStorage(),
    );
    await expect(
      bindTransactionalStates(new LedgerInstance(), grainId("ledger-2"), registry),
    ).rejects.toThrow(/ledger/);
  });
});
