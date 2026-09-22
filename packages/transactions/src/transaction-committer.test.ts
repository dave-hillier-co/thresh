import { describe, expect, it } from "vitest";
import { GrainId } from "@thresh/core/grain-id";
import type { GrainType } from "@thresh/core/grain-type";
import type { TransactionInfo } from "@thresh/core/transaction-info";
import { TransactionInDoubtError, TransactionReadOnlyViolatedError } from "@thresh/core/errors";
import { invocationContext } from "@thresh/runtime/invocation-context";
import { systemTimeProvider } from "@thresh/runtime/time-provider";
import { TransactionAgent } from "@thresh/runtime/transaction-agent";
import { MemoryTransactionalStorage } from "@thresh/transactions/memory-transactional-storage";
import { TransactionCommitter } from "@thresh/transactions/transaction-committer";
import { TransactionalStateImpl } from "@thresh/transactions/transactional-state-impl";

const grainId = (key: string) => new GrainId("CommitterTest" as GrainType, key);

function inTransaction<R>(tx: TransactionInfo, fn: () => Promise<R>): Promise<R> {
  return invocationContext.run(
    { senderId: undefined, ownerId: undefined, reentrancyId: tx.id, transaction: tx },
    fn,
  );
}

interface Service {
  calls: { transactionId: string; data: string }[];
}

interface Balance {
  cents: number;
}

describe("TransactionCommitter", () => {
  const agent = new TransactionAgent(systemTimeProvider);

  it("applies the staged operation against the service on commit", async () => {
    const service: Service = { calls: [] };
    const committer = new TransactionCommitter(grainId("c"), "committer", service);
    const tx = agent.startTransaction();

    await inTransaction(tx, async () => {
      committer.onCommit({
        commit: (transactionId, svc) => {
          svc.calls.push({ transactionId, data: "pass" });
          return true;
        },
      });
      return Promise.resolve();
    });

    await agent.resolve(tx);

    expect(service.calls).toEqual([{ transactionId: tx.id, data: "pass" }]);
  });

  it("surfaces a throwing commit operation as TransactionInDoubtError", async () => {
    const service: Service = { calls: [] };
    const committer = new TransactionCommitter(grainId("c"), "committer", service);
    const tx = agent.startTransaction();

    await inTransaction(tx, async () => {
      committer.onCommit({
        commit: () => {
          throw new Error("boom");
        },
      });
      return Promise.resolve();
    });

    await expect(agent.resolve(tx)).rejects.toBeInstanceOf(TransactionInDoubtError);
  });

  it("rejects staging an operation in a read-only transaction", async () => {
    const service: Service = { calls: [] };
    const committer = new TransactionCommitter(grainId("c"), "committer", service);
    const tx = agent.startTransaction(/* readOnly */ true);

    await expect(
      inTransaction(tx, async () => {
        committer.onCommit({ commit: () => true });
      }),
    ).rejects.toBeInstanceOf(TransactionReadOnlyViolatedError);
  });

  it("discards the staged operation on abort without applying it", async () => {
    const service: Service = { calls: [] };
    const committer = new TransactionCommitter(grainId("c"), "committer", service);
    const tx = agent.startTransaction();

    await inTransaction(tx, async () => {
      committer.onCommit({
        commit: (transactionId, svc) => {
          svc.calls.push({ transactionId, data: "should-not-run" });
          return true;
        },
      });
      return Promise.resolve();
    });

    await agent.abort(tx);

    expect(service.calls).toEqual([]);
  });

  it("never serves as the transaction manager, even enlisting before a grain's state does", async () => {
    // Only a manager-capable participant may be elected (Orleans
    // `ParticipantId.Role.Manager`): the TM durably records the commit before
    // any participant commits, and answers a recovering participant's `status`
    // query. A committer enlists as a write participant but has neither — it is
    // a memory-only stand-in for upstream's storage-backed `TransactionManager`
    // — so electing it would leave the transaction with no durable commit point
    // at all, and no one to answer recovery. It lands in the participant set
    // first here, which is what used to decide the election.
    const storage = new MemoryTransactionalStorage();
    const state = new TransactionalStateImpl<Balance>(
      "balance",
      grainId("g"),
      () => ({ cents: 0 }),
      storage,
    );
    await state.load();
    const service: Service = { calls: [] };
    const committer = new TransactionCommitter(grainId("c"), "committer", service);
    const tx = agent.startTransaction();

    await inTransaction(tx, async () => {
      committer.onCommit({
        commit: (transactionId, svc) => {
          svc.calls.push({ transactionId, data: "pass" });
          return true;
        },
      });
      await state.performUpdate((s) => (s.cents = 5));
    });

    await agent.resolve(tx);

    // The commit record is the grain state's, not the committer's, so the
    // recovery query a sibling participant would make finds it. (Before the
    // fix the committer was elected and no record was written at all.)
    expect(state.status(tx.id)).toBe(true);
    expect(service.calls).toEqual([{ transactionId: tx.id, data: "pass" }]);
  });
});
