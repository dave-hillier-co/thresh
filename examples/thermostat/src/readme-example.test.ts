import { describe, expect, it } from "vitest";
import { defineGrain, usePersistentState } from "@thresh/core/define-grain";
import { defineGrainInterface } from "@thresh/core/grain-interface";
import { SiloAddress } from "@thresh/core/silo-address";
import { InProcessNetwork } from "@thresh/messaging/in-process-transport";
import { createSilo } from "@thresh/hosting/silo-builder";

/**
 * The README's "Quick example" fences, verbatim, so the shop window cannot rot.
 * Both fences are reproduced below; edit them together with `README.md`.
 */

// ---- README fence 1: a fused definition ----
interface Reading {
  tempC: number;
  targetC: number;
}
type Command = { kind: "heat" } | { kind: "cool" };

const Thermostat = defineGrain(
  "Thermostat",
  () => {
    const state = usePersistentState<Reading>("thermostat", {
      defaultValue: (): Reading => ({ tempC: 20, targetC: 21 }),
    });

    return {
      onUpdate: async (tempC: number): Promise<Command[]> => {
        state.value.tempC = tempC;
        await state.write();
        if (tempC < state.value.targetC) return [{ kind: "heat" }];
        if (tempC > state.value.targetC) return [{ kind: "cool" }];
        return [];
      },

      getStatus: async (): Promise<Reading> => ({ ...state.value }),
    };
  },
  { interfaceOptions: { getStatus: { readOnly: true } } },
);

// ---- README fence 2: a separately declared cross-boundary contract ----
// contract module — the only thing a remote caller imports
interface Ledger {
  post(amount: bigint): Promise<bigint>;
}
const Ledger = defineGrainInterface<Ledger, "integer">("example.Ledger");

// implementation module — never imported by callers
const LedgerGrain = defineGrain<Ledger, "integer">("Ledger", () => {
  let balance = 0n;
  return {
    post: async (amount: bigint): Promise<bigint> => (balance += amount),
  };
});

const local = new SiloAddress("silo-0", "uid-0", "silo-0:11111");

describe("README quick example", () => {
  it("hosts and calls a fused definition", async () => {
    const silo = createSilo({ clusterId: "demo", local })
      .useStaticMembership([local])
      .useInProcessTransport(new InProcessNetwork())
      .useMemoryStorage()
      .registerGrain(Thermostat)
      .build();

    await silo.start();
    try {
      const thermostat = silo.getGrain(Thermostat, "kitchen");

      expect(await thermostat.onUpdate(18)).toEqual([{ kind: "heat" }]);
      expect(await thermostat.getStatus()).toEqual({ tempC: 18, targetC: 21 });
    } finally {
      await silo.stop();
    }
  });

  it("hosts an implementation under a separately declared contract", async () => {
    const silo = createSilo({ clusterId: "demo2", local })
      .useStaticMembership([local])
      .useInProcessTransport(new InProcessNetwork())
      .registerGrain(LedgerGrain, { interfaces: [Ledger] })
      .build();

    await silo.start();
    try {
      // The integer key kind is declared once, on the interface, and types `getGrain`.
      expect(await silo.getGrain(Ledger, 42n).post(5n)).toBe(5n);
    } finally {
      await silo.stop();
    }
  });
});
