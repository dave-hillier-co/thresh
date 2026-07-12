import { describe, expect, it } from "vitest";
import { GrainId } from "@tsva/core/grain-id";
import type { TransactionInfo } from "@tsva/core/transaction-info";
import { ActivationData } from "@tsva/runtime/activation";
import { GrainFactory } from "@tsva/runtime/grain-factory";
import { GrainRuntimeImpl } from "@tsva/runtime/grain-runtime-impl";
import { invocationContext } from "@tsva/runtime/invocation-context";
import { FakeTimeProvider } from "@tsva/runtime/test-support/fake-time-provider";

// GAP-TRANSACTION-CONTEXT-INTROSPECTION: grain code has no way to observe
// whether its turn is running inside a transaction, or that transaction's id
// (Orleans' `TransactionContext.GetTransactionInfo()`). `currentTransaction()`
// (@tsva/runtime/invocation-context) already tracks the ambient transaction
// for boundary resolution, but nothing on `GrainRuntime` exposes it.
function makeRuntime(): GrainRuntimeImpl {
  const id = new GrainId("TxIntrospectionGrain", "a");
  const activation = new ActivationData(id, new FakeTimeProvider(), 30_000, false, "act-1");
  const factory = new GrainFactory(() => {
    throw new Error("not used in this test");
  });
  return new GrainRuntimeImpl(factory, activation);
}

const transaction = (id: string): TransactionInfo => ({
  id,
  timeStamp: 0,
  readOnly: false,
  participants: new Map(),
  pendingCalls: 0,
});

describe("GrainRuntime transaction introspection", () => {
  it("reports no transaction outside an ambient transaction", async () => {
    const runtime = makeRuntime();

    await invocationContext.run(
      { senderId: undefined, ownerId: undefined, reentrancyId: "r1" },
      async () => {
        expect(runtime.isInTransaction()).toBe(false);
        expect(runtime.getTransactionId()).toBeUndefined();
      },
    );
  });

  it("reports the ambient transaction's id when one is in scope", async () => {
    const runtime = makeRuntime();

    await invocationContext.run(
      {
        senderId: undefined,
        ownerId: undefined,
        reentrancyId: "r1",
        transaction: transaction("tx-1"),
      },
      async () => {
        expect(runtime.isInTransaction()).toBe(true);
        expect(runtime.getTransactionId()).toBe("tx-1");
      },
    );
  });

  it("reports no transaction when called outside any turn at all", () => {
    const runtime = makeRuntime();

    expect(runtime.isInTransaction()).toBe(false);
    expect(runtime.getTransactionId()).toBeUndefined();
  });
});
