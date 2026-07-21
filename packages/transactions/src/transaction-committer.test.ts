import { describe, expect, it } from "vitest";
import { GrainId } from "@tsva/core/grain-id";
import type { GrainType } from "@tsva/core/grain-type";
import type { TransactionInfo } from "@tsva/core/transaction-info";
import { TransactionInDoubtError, TransactionReadOnlyViolatedError } from "@tsva/core/errors";
import { invocationContext } from "@tsva/runtime/invocation-context";
import { systemTimeProvider } from "@tsva/runtime/time-provider";
import { TransactionAgent } from "@tsva/runtime/transaction-agent";
import { TransactionCommitter } from "@tsva/transactions/transaction-committer";

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
});
