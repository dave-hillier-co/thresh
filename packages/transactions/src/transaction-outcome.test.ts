import { describe, expect, it } from "vitest";
import { GrainId } from "@thresh/core/grain-id";
import type { GrainType } from "@thresh/core/grain-type";
import { FakeTimeProvider } from "@thresh/core/test-support/fake-time-provider";
import { TransactionAbortedError } from "@thresh/core/errors";
import type { InvocationRequest } from "@thresh/core/request";
import type { ParticipantId, TransactionInfo } from "@thresh/core/transaction-info";
import type { Dispatcher } from "@thresh/runtime/dispatcher";
import { invocationContext } from "@thresh/runtime/invocation-context";
import { TransactionAgent } from "@thresh/runtime/transaction-agent";
import { MemoryTransactionalStorage } from "@thresh/transactions/memory-transactional-storage";
import { TransactionalStateImpl } from "@thresh/transactions/transactional-state-impl";

interface Balance {
  cents: number;
}

const grainId = (key: string) => new GrainId("Account" as GrainType, key);
const participant = (key: string): ParticipantId => ({
  grainId: grainId(key),
  stateName: "balance",
});

function inTransaction<R>(tx: TransactionInfo, fn: () => Promise<R>): Promise<R> {
  return invocationContext.run({ senderId: undefined, ownerId: undefined, transaction: tx }, fn);
}

/** Let every promise chain a fired timer started run to completion. */
const settle = () => new Promise<void>((resolve) => setImmediate(resolve));

async function committed(storage: MemoryTransactionalStorage, key: string) {
  const loaded = await storage.load("balance", grainId(key));
  return {
    cents: loaded.committedSequenceId === 0 ? 100 : (loaded.committedState as Balance).cents,
    pending: loaded.pendingStates.length,
  };
}

/**
 * A two-account transfer whose participants have both prepared: `a` is the
 * elected TM, `b` an ordinary participant whose in-doubt queries are answered
 * by `a` directly, standing in for the routed `status` call.
 */
async function preparedTransfer(clock: FakeTimeProvider) {
  const storage = new MemoryTransactionalStorage();
  const a = new TransactionalStateImpl<Balance>(
    "balance",
    grainId("a"),
    () => ({ cents: 100 }),
    storage,
    undefined,
    undefined,
    clock,
  );
  await a.load();
  const newB = async () => {
    const b = new TransactionalStateImpl<Balance>(
      "balance",
      grainId("b"),
      () => ({ cents: 100 }),
      storage,
      async (_manager, transactionId) => a.status(transactionId),
      { confirmationIntervalMs: 1_000, confirmationMaxIntervalMs: 4_000 },
      clock,
    );
    await b.load();
    return b;
  };
  const b = await newB();
  const agent = new TransactionAgent(clock);
  const tx = agent.startTransaction();
  await inTransaction(tx, () => a.performUpdate((s) => (s.cents -= 30)));
  await inTransaction(tx, () => b.performUpdate((s) => (s.cents += 30)));
  const manager = participant("a");
  expect(await a.prepare(tx.id, { reads: 0, writes: 1 }, tx.timeStamp, manager)).toBe(true);
  expect(await b.prepare(tx.id, { reads: 0, writes: 1 }, tx.timeStamp, manager)).toBe(true);
  return { storage, a, newB, tx, manager };
}

describe("the TM's answer to a status query is tri-state (T3)", () => {
  it("answers undetermined while it holds the transaction, committed once recorded, presumed abort when it has neither", async () => {
    const clock = new FakeTimeProvider();
    const { a, tx, manager } = await preparedTransfer(clock);

    expect(await a.status(tx.id)).toBe("undetermined");
    await a.recordCommit(tx.id, tx.timeStamp, [manager, participant("b")]);
    expect(await a.status(tx.id)).toBe("committed");
    expect(await a.status("never-heard-of-it")).toBe("presumedAbort");
  });

  it("applies a prepared write whose activation was replaced before the TM recorded the commit", async () => {
    const clock = new FakeTimeProvider();
    const { storage, a, newB, tx, manager } = await preparedTransfer(clock);

    // B's activation is replaced (migration, drain, rebalance) while the TM is
    // still deciding: the fresh one finds its prepared record in doubt.
    const replacement = await newB();

    // The decision then proceeds exactly as `TransactionAgent.resolve` drives it.
    await a.recordCommit(tx.id, tx.timeStamp, [manager, participant("b")]);
    await a.commit(tx.id);
    await replacement.commit(tx.id);

    expect(await committed(storage, "a")).toEqual({ cents: 70, pending: 0 });
    expect(await committed(storage, "b")).toEqual({ cents: 130, pending: 0 });
  });

  it("keeps an undetermined record and resolves it through the confirmation worker once the TM decides", async () => {
    const clock = new FakeTimeProvider();
    const { storage, a, newB, tx, manager } = await preparedTransfer(clock);
    const replacement = await newB();

    // The coordinator records the commit and then dies before telling B.
    await a.recordCommit(tx.id, tx.timeStamp, [manager, participant("b")]);
    await a.commit(tx.id);
    clock.advance(1_000);
    await settle();

    expect(await committed(storage, "b")).toEqual({ cents: 130, pending: 0 });
    replacement.unload();
  });

  it("drops a prepared record when the TM answers presumed abort", async () => {
    const clock = new FakeTimeProvider();
    const { storage, a, newB, tx } = await preparedTransfer(clock);

    // The TM aborts: it holds neither the transaction nor a commit record.
    await a.abort(tx.id);
    const replacement = await newB();

    expect(await committed(storage, "b")).toEqual({ cents: 100, pending: 0 });
    replacement.unload();
  });

  it("aborts when the TM was replaced after preparing, since it refuses to record a commit it no longer holds", async () => {
    const clock = new FakeTimeProvider();
    const storage = new MemoryTransactionalStorage();
    const newState = async (key: string) => {
      const state = new TransactionalStateImpl<Balance>(
        "balance",
        grainId(key),
        () => ({ cents: 100 }),
        storage,
        async () => "presumedAbort",
        undefined,
        clock,
      );
      await state.load();
      return state;
    };
    const tm = await newState("a");
    const b = await newState("b");
    // The TM is reached over the wire: its prepare lands on the activation that
    // did the work, every later call on a fresh one (its first one died).
    let replacement: TransactionalStateImpl<Balance> | undefined;
    const agent = new TransactionAgent(clock);
    agent.setDispatcher({
      invoke: async (req: InvocationRequest) => {
        const target = req.method === "prepare" ? tm : (replacement ??= await newState("a"));
        const [, ...rest] = req.args;
        const method = (target as unknown as Record<string, (...a: unknown[]) => unknown>)[
          req.method
        ]!;
        return method.apply(target, rest);
      },
    } as unknown as Dispatcher);

    const tx = agent.startTransaction();
    await inTransaction(tx, () => tm.performUpdate((s) => (s.cents -= 30)));
    await inTransaction(tx, () => b.performUpdate((s) => (s.cents += 30)));
    const key = [...tx.participants.keys()].find((k) => k.includes("/a/"))!;
    const entry = tx.participants.get(key)!;
    tx.participants.set(key, { id: entry.id, access: entry.access });

    await expect(agent.resolve(tx)).rejects.toBeInstanceOf(TransactionAbortedError);
    expect(await committed(storage, "a")).toEqual({ cents: 100, pending: 0 });
    expect(await committed(storage, "b")).toEqual({ cents: 100, pending: 0 });
  });
});
