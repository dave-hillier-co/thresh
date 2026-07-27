import { describe, expect, it } from "vitest";
import { Grain } from "@thresh/core/grain";
import { GrainId } from "@thresh/core/grain-id";
import { registerReducerField } from "@thresh/core/reducer-state-metadata";
import { registerDurableField } from "@thresh/core/durable-state-metadata";
import { registerTransactionalField } from "@thresh/core/transactional-state-metadata";
import { ReadOnlyStateViolationError } from "@thresh/core/errors";
import type { ReducerState } from "@thresh/core/reducer-state";
import type { DurableValue } from "@thresh/core/durable-state";
import type { TransactionalState } from "@thresh/core/transactional-state";
import type { InvocationRequest } from "@thresh/core/request";
import { ActivationData } from "@thresh/runtime/activation";
import { FakeTimeProvider } from "@thresh/runtime/test-support/fake-time-provider";

/**
 * Extends `activation.readonly-guard.test.ts` (which covers `@persistentState`
 * only) to the remaining state facets the same `readOnlyStateGuard` opt-in
 * covers: reducer, durable-journaling and transactional
 * (`GAP-READONLY-ENFORCEMENT` follow-up, issue #25).
 */

interface Balance {
  cents: number;
}

function fakeReducerState(initial: Balance): ReducerState<Balance, { amount: number }> {
  let value = initial;
  return {
    get value() {
      return value;
    },
    etag: undefined,
    exists: true,
    raise(event) {
      value = { cents: value.cents + event.amount };
      return value;
    },
    async read() {},
    async write() {},
  };
}

function fakeDurableValue(initial: Balance): DurableValue<Balance> {
  let current: Balance | undefined = initial;
  return {
    get value() {
      return current;
    },
    get() {
      return current;
    },
    async set(v) {
      current = v;
    },
    async clear() {
      current = undefined;
    },
  };
}

function fakeTransactionalState(initial: Balance): TransactionalState<Balance> {
  const value = initial;
  return {
    async performRead(read) {
      return read(value);
    },
    async performUpdate(update) {
      const result = update(value);
      return result;
    },
  };
}

class MultiFacetGrain extends Grain {
  ledger!: ReducerState<Balance, { amount: number }>;
  cache!: DurableValue<Balance>;
  wallet!: TransactionalState<Balance>;

  constructor() {
    super();
    registerReducerField(this, {
      fieldName: "ledger",
      stateName: "ledger",
      initial: () => ({ cents: 0 }),
      reduce: (s) => s,
    });
    registerDurableField(this, { fieldName: "cache", stateName: "cache", kind: "value" });
    registerTransactionalField(this, {
      fieldName: "wallet",
      stateName: "wallet",
      initial: () => ({ cents: 0 }),
    });
  }

  async raiseOnLedger(): Promise<number> {
    return this.ledger.raise({ amount: 1 }).cents;
  }

  async setCache(): Promise<void> {
    await this.cache.set({ cents: 1 });
  }

  async updateWallet(): Promise<number> {
    return this.wallet.performUpdate((s) => {
      s.cents += 1;
      return s.cents;
    });
  }

  async readWallet(): Promise<number> {
    return this.wallet.performRead((s) => s.cents);
  }
}

const id = new GrainId("MultiFacetGrain", "m1");

function makeActivation(readOnlyStateGuard: boolean): {
  activation: ActivationData;
  grain: MultiFacetGrain;
} {
  const activation = new ActivationData(id, new FakeTimeProvider(), 30_000, false, "act-1");
  const grain = new MultiFacetGrain();
  grain.setContext(activation);
  activation.instance = grain;
  activation.readOnlyStateGuard = readOnlyStateGuard;
  activation.beginActivate("incoming-call");
  grain.ledger = fakeReducerState({ cents: 100 });
  grain.cache = fakeDurableValue({ cents: 100 });
  grain.wallet = fakeTransactionalState({ cents: 100 });
  return { activation, grain };
}

function call(method: string, readOnly: boolean): InvocationRequest {
  return {
    target: id,
    interfaceId: 0,
    method,
    args: [],
    options: readOnly ? { readOnly: true } : {},
    reentrancyId: `r-${method}-${String(readOnly)}`,
  };
}

describe("dev-mode @readOnly mutation guard breadth (GAP-READONLY-ENFORCEMENT follow-up, #25)", () => {
  describe("reducer state", () => {
    it("is advisory (off) by default: a readOnly turn can still raise onto reducer state", async () => {
      const { activation } = makeActivation(false);
      await expect(activation.invoke(call("raiseOnLedger", true))).resolves.toBe(101);
    });

    it("when enabled, rejects raise() during a readOnly turn", async () => {
      const { activation, grain } = makeActivation(true);
      await expect(activation.invoke(call("raiseOnLedger", true))).rejects.toBeInstanceOf(
        ReadOnlyStateViolationError,
      );
      expect(grain.ledger.value.cents).toBe(100);
    });

    it("when enabled, does not guard a non-readOnly turn", async () => {
      const { activation } = makeActivation(true);
      await expect(activation.invoke(call("raiseOnLedger", false))).resolves.toBe(101);
    });
  });

  describe("durable-journaling state", () => {
    it("is advisory (off) by default: a readOnly turn can still set() durable state", async () => {
      const { activation } = makeActivation(false);
      await expect(activation.invoke(call("setCache", true))).resolves.toBeUndefined();
    });

    it("when enabled, rejects set() during a readOnly turn", async () => {
      const { activation, grain } = makeActivation(true);
      await expect(activation.invoke(call("setCache", true))).rejects.toBeInstanceOf(
        ReadOnlyStateViolationError,
      );
      expect(grain.cache.value).toEqual({ cents: 100 });
    });

    it("when enabled, does not guard a non-readOnly turn", async () => {
      const { activation } = makeActivation(true);
      await expect(activation.invoke(call("setCache", false))).resolves.toBeUndefined();
    });
  });

  describe("transactional state", () => {
    it("is advisory (off) by default: a readOnly call can still performUpdate", async () => {
      const { activation } = makeActivation(false);
      await expect(activation.invoke(call("updateWallet", true))).resolves.toBe(101);
    });

    it("when enabled, rejects performUpdate during a readOnly call even though the transaction itself is not read-only", async () => {
      const { activation } = makeActivation(true);
      await expect(activation.invoke(call("updateWallet", true))).rejects.toBeInstanceOf(
        ReadOnlyStateViolationError,
      );
    });

    it("when enabled, still allows performRead during a readOnly call", async () => {
      const { activation } = makeActivation(true);
      await expect(activation.invoke(call("readWallet", true))).resolves.toBe(100);
    });

    it("when enabled, does not guard a non-readOnly turn", async () => {
      const { activation } = makeActivation(true);
      await expect(activation.invoke(call("updateWallet", false))).resolves.toBe(101);
    });
  });

  it("when enabled, restores every real facet after a readOnly turn so a later call reads/writes live state", async () => {
    const { activation, grain } = makeActivation(true);
    await expect(activation.invoke(call("raiseOnLedger", true))).rejects.toBeInstanceOf(
      ReadOnlyStateViolationError,
    );
    // the guard is only installed for the duration of the readOnly turn
    expect(grain.ledger.raise).toBeInstanceOf(Function);
    await expect(activation.invoke(call("raiseOnLedger", false))).resolves.toBe(101);
    await expect(activation.invoke(call("setCache", false))).resolves.toBeUndefined();
    await expect(activation.invoke(call("updateWallet", false))).resolves.toBe(101);
  });
});
