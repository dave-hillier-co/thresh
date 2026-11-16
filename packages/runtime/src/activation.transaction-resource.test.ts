import { describe, expect, it } from "vitest";
import { Grain } from "@thresh/core/grain";
import { GrainId } from "@thresh/core/grain-id";
import { registerTransactionalField } from "@thresh/core/transactional-state-metadata";
import type { InvocationRequest } from "@thresh/core/request";
import type { TransactionalState } from "@thresh/core/transactional-state";
import { TransactionResourceInterface } from "@thresh/core/transaction-resource";
import { ActivationData } from "@thresh/runtime/activation";
import { FakeTimeProvider } from "@thresh/runtime/test-support/fake-time-provider";

/**
 * Transaction-resource protocol calls (`prepare`/`commit`/...) are always-interleave,
 * so one can land while a `@readOnly` turn has the transactional field swapped for
 * its read-only guard. The call must still reach the real facet, not the guard.
 */

interface Balance {
  cents: number;
}

/** A facet that records the protocol calls it receives, as the real one would act on them. */
class RecordingFacet implements TransactionalState<Balance> {
  readonly prepared: string[] = [];

  async performRead<R>(read: (state: Balance) => R): Promise<R> {
    return read({ cents: 100 });
  }

  async performUpdate<R>(update: (state: Balance) => R): Promise<R> {
    return update({ cents: 100 });
  }

  async prepare(transactionId: string): Promise<boolean> {
    this.prepared.push(transactionId);
    return true;
  }
}

let release: () => void = () => undefined;

class WalletGrain extends Grain {
  wallet!: TransactionalState<Balance>;

  constructor() {
    super();
    registerTransactionalField(this, {
      fieldName: "wallet",
      stateName: "wallet",
      initial: () => ({ cents: 0 }),
    });
  }

  /** A readOnly turn that stays in flight, so the guard stays installed. */
  async parked(): Promise<void> {
    await new Promise<void>((r) => (release = r));
  }
}

const id = new GrainId("WalletGrain", "w1");

function request(
  method: string,
  args: unknown[],
  interfaceId: number,
  options: InvocationRequest["options"],
): InvocationRequest {
  return { target: id, interfaceId, method, args, options, reentrancyId: `r-${method}` };
}

describe("a transaction resource call during a readOnly turn", () => {
  it("reaches the real transactional state through the read-only guard", async () => {
    const activation = new ActivationData(id, new FakeTimeProvider(), 30_000, false, "act-1");
    const grain = new WalletGrain();
    grain.setContext(activation);
    activation.instance = grain;
    activation.readOnlyStateGuard = true;
    activation.beginActivate("incoming-call");
    const facet = new RecordingFacet();
    grain.wallet = facet;

    const turn = activation.invoke(request("parked", [], 0, { readOnly: true }));
    await new Promise((r) => setTimeout(r, 0));
    // The guard is in place: the field is no longer the facet itself.
    expect(grain.wallet).not.toBe(facet);

    const vote = await activation.invoke(
      request("prepare", ["wallet", "tx-1"], TransactionResourceInterface.id, {
        alwaysInterleave: true,
      }),
    );

    expect(vote).toBe(true);
    expect(facet.prepared).toEqual(["tx-1"]);
    release();
    await turn;
  });
});
