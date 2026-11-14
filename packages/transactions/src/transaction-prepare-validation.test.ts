import { describe, expect, it } from "vitest";
import { GrainId } from "@thresh/core/grain-id";
import type { GrainType } from "@thresh/core/grain-type";
import { TransactionAbortedError } from "@thresh/core/errors";
import type { TransactionInfo } from "@thresh/core/transaction-info";
import type { Dispatcher } from "@thresh/runtime/dispatcher";
import type { InvocationRequest } from "@thresh/core/request";
import { invocationContext } from "@thresh/runtime/invocation-context";
import { systemTimeProvider } from "@thresh/runtime/time-provider";
import { TransactionAgent } from "@thresh/runtime/transaction-agent";
import { MemoryTransactionalStorage } from "@thresh/transactions/memory-transactional-storage";
import { TransactionalStateImpl } from "@thresh/transactions/transactional-state-impl";

interface Balance {
  cents: number;
}

const grainId = (key: string) => new GrainId("Account" as GrainType, key);

function inTransaction<R>(tx: TransactionInfo, fn: () => Promise<R>): Promise<R> {
  return invocationContext.run({ senderId: undefined, ownerId: undefined, transaction: tx }, fn);
}

/**
 * A dispatcher standing in for the wire: every routed protocol call reaches
 * whatever activation `current()` names at the moment it arrives — so a test
 * can replace the activation between a participant's work and its prepare,
 * exactly as a deactivation, a migration or a dead silo would.
 */
function routeTo(current: () => Promise<TransactionalStateImpl<Balance>>): Dispatcher {
  return {
    invoke: async (req: InvocationRequest) => {
      const target = await current();
      const [, ...rest] = req.args;
      const method = (target as unknown as Record<string, (...a: unknown[]) => unknown>)[
        req.method
      ]!;
      return method.apply(target, rest);
    },
  } as unknown as Dispatcher;
}

/** Hand a participant to the root as a remote silo's reply would: id and access only. */
function asRemote(tx: TransactionInfo, key: string): void {
  const entry = [...tx.participants.entries()].find(([k]) => k.includes(`/${key}/`))!;
  tx.participants.set(entry[0], { id: entry[1].id, access: entry[1].access });
}

async function newState(
  storage: MemoryTransactionalStorage,
  key: string,
): Promise<TransactionalStateImpl<Balance>> {
  const state = new TransactionalStateImpl<Balance>(
    "balance",
    grainId(key),
    () => ({ cents: 100 }),
    storage,
  );
  await state.load();
  return state;
}

async function committedCents(storage: MemoryTransactionalStorage, key: string): Promise<number> {
  const loaded = await storage.load("balance", grainId(key));
  return loaded.committedSequenceId === 0 ? 100 : (loaded.committedState as Balance).cents;
}

describe("prepare validates the participant's lock (T2)", () => {
  it("aborts a transfer whose writer was re-activated between its write and its prepare", async () => {
    const storage = new MemoryTransactionalStorage();
    const a = await newState(storage, "a");
    const b = await newState(storage, "b");
    let replacement: TransactionalStateImpl<Balance> | undefined;
    const agent = new TransactionAgent(systemTimeProvider);
    agent.setDispatcher(routeTo(async () => (replacement ??= await newState(storage, "b"))));

    const tx = agent.startTransaction();
    await inTransaction(tx, () => a.performUpdate((s) => (s.cents -= 30)));
    await inTransaction(tx, () => b.performUpdate((s) => (s.cents += 30)));
    asRemote(tx, "b");

    await expect(agent.resolve(tx)).rejects.toBeInstanceOf(TransactionAbortedError);
    expect(await committedCents(storage, "a")).toBe(100);
    expect(await committedCents(storage, "b")).toBe(100);
  });

  it("aborts when a read-only participant lost its lock before prepare", async () => {
    const storage = new MemoryTransactionalStorage();
    const writer = await newState(storage, "w");
    const reader = await newState(storage, "r");
    let replacement: TransactionalStateImpl<Balance> | undefined;
    const agent = new TransactionAgent(systemTimeProvider);
    agent.setDispatcher(routeTo(async () => (replacement ??= await newState(storage, "r"))));

    const tx = agent.startTransaction();
    const seen = await inTransaction(tx, () => reader.performRead((s) => s.cents));
    await inTransaction(tx, () => writer.performUpdate((s) => (s.cents = seen + 1)));
    asRemote(tx, "r");

    await expect(agent.resolve(tx)).rejects.toBeInstanceOf(TransactionAbortedError);
    expect(await committedCents(storage, "w")).toBe(100);
  });

  it("votes no when the access the coordinator recorded differs from what the lock saw", async () => {
    const storage = new MemoryTransactionalStorage();
    const state = await newState(storage, "a");
    const agent = new TransactionAgent(systemTimeProvider);
    const tx = agent.startTransaction();
    await inTransaction(tx, () => state.performUpdate((s) => (s.cents += 1)));
    const manager = { grainId: grainId("a"), stateName: "balance" };

    expect(await state.prepare(tx.id, { reads: 0, writes: 2 }, tx.timeStamp, manager)).toBe(false);

    // The failed validation rolled the transaction back (Orleans
    // `ValidateLock` → `Rollback`): a later writer is not blocked by it.
    const later = agent.startTransaction();
    await inTransaction(later, () => state.performUpdate((s) => (s.cents += 5)));
    await agent.resolve(later);
    expect(await committedCents(storage, "a")).toBe(105);
  });

  it("votes yes when the lock still holds exactly the recorded access", async () => {
    const storage = new MemoryTransactionalStorage();
    const state = await newState(storage, "a");
    const agent = new TransactionAgent(systemTimeProvider);
    const tx = agent.startTransaction();
    await inTransaction(tx, () => state.performRead((s) => s.cents));
    await inTransaction(tx, () => state.performUpdate((s) => (s.cents += 1)));
    const manager = { grainId: grainId("a"), stateName: "balance" };

    expect(await state.prepare(tx.id, { reads: 1, writes: 1 }, tx.timeStamp, manager)).toBe(true);
  });
});
