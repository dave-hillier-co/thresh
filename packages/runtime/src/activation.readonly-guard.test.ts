import { describe, expect, it } from "vitest";
import { Grain } from "@thresh/core/grain";
import { GrainId } from "@thresh/core/grain-id";
import { registerPersistentField } from "@thresh/core/persistent-state-metadata";
import { ReadOnlyStateViolationError } from "@thresh/core/errors";
import type { PersistentState } from "@thresh/core/persistent-state";
import type { InvocationRequest } from "@thresh/core/request";
import { ActivationData } from "@thresh/runtime/activation";
import { FakeTimeProvider } from "@thresh/runtime/test-support/fake-time-provider";

interface Balance {
  cents: number;
}

function fakePersistentState(initial: Balance): PersistentState<Balance> {
  let value = initial;
  return {
    get value() {
      return value;
    },
    set value(next: Balance) {
      value = next;
    },
    etag: undefined,
    exists: true,
    async read() {},
    async write() {},
    async clear() {},
  };
}

class AccountGrain extends Grain {
  balance!: PersistentState<Balance>;

  constructor() {
    super();
    registerPersistentField(this, { fieldName: "balance", stateName: "balance" });
  }

  async getCents(): Promise<number> {
    return this.balance.value.cents;
  }

  async mutateInPlace(): Promise<number> {
    this.balance.value.cents += 1;
    return this.balance.value.cents;
  }

  async writeBalance(): Promise<void> {
    await this.balance.write();
  }
}

const id = new GrainId("AccountGrain", "a1");

function makeActivation(readOnlyStateGuard: boolean): {
  activation: ActivationData;
  grain: AccountGrain;
} {
  const activation = new ActivationData(id, new FakeTimeProvider(), 30_000, false, "act-1");
  const grain = new AccountGrain();
  grain.setContext(activation);
  activation.instance = grain;
  activation.readOnlyStateGuard = readOnlyStateGuard;
  activation.beginActivate("incoming-call");
  grain.balance = fakePersistentState({ cents: 100 });
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

describe("dev-mode @readOnly mutation guard (GAP-READONLY-ENFORCEMENT)", () => {
  it("is advisory (off) by default: a readOnly turn can still mutate state in place", async () => {
    const { activation } = makeActivation(false);
    await expect(activation.invoke(call("mutateInPlace", true))).resolves.toBe(101);
  });

  it("when enabled, rejects an in-place mutation of persistent state during a readOnly turn", async () => {
    const { activation, grain } = makeActivation(true);
    await expect(activation.invoke(call("mutateInPlace", true))).rejects.toBeInstanceOf(
      ReadOnlyStateViolationError,
    );
    expect(grain.balance.value.cents).toBe(100);
  });

  it("when enabled, rejects write() during a readOnly turn", async () => {
    const { activation } = makeActivation(true);
    await expect(activation.invoke(call("writeBalance", true))).rejects.toBeInstanceOf(
      ReadOnlyStateViolationError,
    );
  });

  it("when enabled, does not guard a non-readOnly turn: mutation and write succeed", async () => {
    const { activation } = makeActivation(true);
    await expect(activation.invoke(call("mutateInPlace", false))).resolves.toBe(101);
    await expect(activation.invoke(call("writeBalance", false))).resolves.toBeUndefined();
  });

  it("when enabled, restores the real facet after a readOnly turn so a later call reads live state", async () => {
    const { activation, grain } = makeActivation(true);
    await expect(activation.invoke(call("getCents", true))).resolves.toBe(100);
    // the guard is only installed for the duration of the readOnly turn
    expect(grain.balance.write).toBeInstanceOf(Function);
    await expect(activation.invoke(call("mutateInPlace", false))).resolves.toBe(101);
  });
});
