import { describe, expect, it } from "vitest";
import { defineGrainInterface } from "@thresh/core/grain-interface";
import type { GrainKey } from "@thresh/core/key-kinds";
import type { InvocationRequest } from "@thresh/core/request";
import { systemTimeProvider } from "@thresh/core/time-provider";
import type { Dispatcher } from "@thresh/runtime/dispatcher";
import { GrainFactory } from "@thresh/runtime/grain-factory";
import { TransactionAgent } from "@thresh/runtime/transaction-agent";

// `oneWay` and a transaction boundary are mutually exclusive. A `oneWay` call
// resolves as soon as the message is on its way (see `dispatchDetachingOneWay`)
// — the caller never learns the callee's outcome, because Orleans' `[OneWay]`
// registers no response callback at all (`InsideRuntimeClient.SendRequest`).
// A method that also OPENS a transaction (`create`/`createOrJoin`) or JOINS the
// caller's (`join`) needs exactly that outcome: the root resolves the
// transaction the moment its call returns, so with a detached one-way call it
// would commit a transaction the callee has not yet contributed to — an empty
// commit for `create`, and for `join` a commit that races the callee's writes.
// Reject the combination where it is declared rather than committing nothing.

interface IThing extends GrainKey<string> {
  notifyAndCreate(): Promise<void>;
  notifyAndJoin(): Promise<void>;
  notify(): Promise<void>;
}

const IThing = defineGrainInterface<IThing>("IThing.one-way-transaction", {
  options: {
    notifyAndCreate: { oneWay: true, transaction: "create" },
    notifyAndJoin: { oneWay: true, transaction: "join" },
    notify: { oneWay: true },
  },
});

function buildFactory(): { factory: GrainFactory; seen: InvocationRequest[] } {
  const seen: InvocationRequest[] = [];
  const invoke: Dispatcher["invoke"] = (req) => {
    seen.push(req);
    return Promise.resolve(undefined);
  };
  const factory = new GrainFactory(() => "Thing");
  factory.setDispatcher({ invoke });
  // A real agent, so the rejection below is the oneWay/transaction conflict and
  // not `TransactionsDisabledError` from an unwired one.
  factory.setTransactionAgent(new TransactionAgent(systemTimeProvider));
  return { factory, seen };
}

describe("a oneWay method that also declares a transaction boundary", () => {
  it("rejects a transaction-creating oneWay call instead of committing an empty transaction", async () => {
    const { factory, seen } = buildFactory();

    await expect(factory.getGrain(IThing, "a").notifyAndCreate()).rejects.toThrow(
      /oneWay.*transaction/i,
    );
    expect(seen).toEqual([]);
  });

  it("rejects a transaction-joining oneWay call", async () => {
    const { factory, seen } = buildFactory();

    await expect(factory.getGrain(IThing, "a").notifyAndJoin()).rejects.toThrow(
      /oneWay.*transaction/i,
    );
    expect(seen).toEqual([]);
  });

  it("still dispatches an ordinary oneWay call with no transaction attribute", async () => {
    const { factory, seen } = buildFactory();

    await expect(factory.getGrain(IThing, "a").notify()).resolves.toBeUndefined();
    expect(seen).toHaveLength(1);
    expect(seen[0]!.transaction).toBeUndefined();
  });
});
