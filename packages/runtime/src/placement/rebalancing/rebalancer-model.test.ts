import { describe, expect, it } from "vitest";
import {
  adaptiveScaling,
  clusterImbalance,
  DEFAULT_REBALANCER_OPTIONS,
  formSiloPairs,
  INITIAL_CYCLE_STATE,
  planCycle,
  shannonEntropy,
  type CycleState,
} from "@tsva/runtime/placement/rebalancing/rebalancer-model";

const snap = (entries: Record<string, number>): Map<string, number> =>
  new Map(Object.entries(entries));

describe("shannonEntropy", () => {
  it("is maximal (ln S) for an even distribution", () => {
    expect(shannonEntropy([5, 5])).toBeCloseTo(Math.log(2), 10);
    expect(shannonEntropy([4, 4, 4])).toBeCloseTo(Math.log(3), 10);
  });

  it("is ~0 when all load is on one silo", () => {
    expect(shannonEntropy([100, 0])).toBeCloseTo(0, 6);
  });

  it("matches the hand-computed value for a skewed distribution", () => {
    // p = [0.2, 0.8]; -(.2 ln.2 + .8 ln.8)
    const expected = -(0.2 * Math.log(0.2) + 0.8 * Math.log(0.8));
    expect(shannonEntropy([20, 80])).toBeCloseTo(expected, 10);
  });

  it("is 0 for an empty or zero-total snapshot", () => {
    expect(shannonEntropy([])).toBe(0);
    expect(shannonEntropy([0, 0])).toBe(0);
  });
});

describe("clusterImbalance", () => {
  it("is ~0 for a perfectly even cluster and approaches 1 as it skews", () => {
    expect(clusterImbalance([50, 50])).toBeCloseTo(0, 10);
    expect(clusterImbalance([20, 80])).toBeGreaterThan(0.2);
    expect(clusterImbalance([1, 999])).toBeGreaterThan(0.9);
  });
});

describe("adaptiveScaling", () => {
  it("ramps up over a session's cycles and is damped by cluster size", () => {
    const o = DEFAULT_REBALANCER_OPTIONS;
    const early = adaptiveScaling(2, 1, o);
    const late = adaptiveScaling(2, 50, o);
    expect(late).toBeGreaterThan(early); // more cycles -> faster migration
    expect(adaptiveScaling(10, 50, o)).toBeLessThan(adaptiveScaling(2, 50, o)); // bigger cluster -> damped
  });
});

describe("formSiloPairs", () => {
  it("pairs the least-loaded silo with the most-loaded, working inward", () => {
    const pairs = formSiloPairs(snap({ a: 1, b: 9, c: 5, d: 3 }));
    // sorted by load: a(1), d(3), c(5), b(9) -> (a,b),(d,c)
    expect(pairs).toEqual([
      ["a", "b"],
      ["d", "c"],
    ]);
  });

  it("leaves the median silo unpaired when the count is odd", () => {
    const pairs = formSiloPairs(snap({ a: 1, c: 5, b: 9 }));
    expect(pairs).toEqual([["a", "b"]]);
  });
});

describe("planCycle", () => {
  const opts = DEFAULT_REBALANCER_OPTIONS;

  it("does not run with fewer than two silos", () => {
    const plan = planCycle(snap({ only: 10 }), opts, INITIAL_CYCLE_STATE);
    expect(plan.ran).toBe(false);
    expect(plan.moves).toEqual([]);
  });

  it("completes the session when the cluster is already balanced", () => {
    const plan = planCycle(snap({ a: 50, b: 50 }), opts, INITIAL_CYCLE_STATE);
    expect(plan.stop).toBe("complete");
    expect(plan.moves).toEqual([]);
    expect(plan.imbalance).toBeCloseTo(0, 10);
  });

  it("migrates from the high silo to the low silo on an imbalanced cluster", () => {
    const plan = planCycle(snap({ a: 20, b: 80 }), opts, INITIAL_CYCLE_STATE);
    expect(plan.ran).toBe(true);
    expect(plan.stop).toBeUndefined();
    expect(plan.moves).toHaveLength(1);
    expect(plan.moves[0]!.from).toBe("b");
    expect(plan.moves[0]!.to).toBe("a");
    expect(plan.moves[0]!.count).toBeGreaterThanOrEqual(1);
    expect(plan.nextState.rebalancingCycle).toBe(1);
    expect(plan.nextState.stagnantCycles).toBe(0);
  });

  it("counts a stagnant cycle (no moves) when entropy barely changes", () => {
    const s = snap({ a: 20, b: 80 });
    const entropy = shannonEntropy([...s.values()]);
    // Seed previousEntropy at the current entropy so the change is ~0.
    const state: CycleState = { rebalancingCycle: 5, stagnantCycles: 0, previousEntropy: entropy };
    const plan = planCycle(s, opts, state);
    expect(plan.moves).toEqual([]);
    expect(plan.stop).toBeUndefined();
    expect(plan.nextState.stagnantCycles).toBe(1);
  });

  it("stops the session as stagnated once max stagnant cycles is reached", () => {
    const state: CycleState = { rebalancingCycle: 8, stagnantCycles: opts.maxStagnantCycles, previousEntropy: 0.4 };
    const plan = planCycle(snap({ a: 20, b: 80 }), opts, state);
    expect(plan.stop).toBe("stagnated");
    expect(plan.moves).toEqual([]);
  });

  it("clamps each move to the migration count limit", () => {
    // Late cycle + strong imbalance yields a delta well above 1; cap it at 3.
    const limited = { ...opts, activationMigrationCountLimit: 3 };
    const state: CycleState = { rebalancingCycle: 49, stagnantCycles: 0, previousEntropy: 0 };
    const plan = planCycle(snap({ a: 10, b: 90 }), limited, state);
    expect(plan.moves).toHaveLength(1);
    expect(plan.moves[0]!.count).toBe(3);
  });

  it("is deterministic for the same inputs", () => {
    const a = planCycle(snap({ a: 30, b: 70, c: 50 }), opts, INITIAL_CYCLE_STATE);
    const b = planCycle(snap({ a: 30, b: 70, c: 50 }), opts, INITIAL_CYCLE_STATE);
    expect(a).toEqual(b);
  });

  it("converges a skewed cluster toward balance over a session", () => {
    const loads = snap({ a: 20, b: 80 });
    let state = INITIAL_CYCLE_STATE;
    const before = clusterImbalance([...loads.values()]);
    let stopped: string | undefined;
    for (let i = 0; i < 500; i++) {
      const plan = planCycle(loads, opts, state);
      state = plan.nextState;
      for (const m of plan.moves) {
        loads.set(m.from, loads.get(m.from)! - m.count);
        loads.set(m.to, loads.get(m.to)! + m.count);
      }
      if (plan.stop !== undefined) {
        stopped = plan.stop;
        break;
      }
    }
    const after = clusterImbalance([...loads.values()]);
    expect(after).toBeLessThan(before);
    // The session terminates (does not run forever). With integer activation
    // counts the final sub-1 deltas truncate to 0 just shy of an exact 50/50, so
    // a near-balanced cluster ends "stagnated" rather than "complete" — both are
    // valid session ends; what matters is it converged and stopped.
    expect(stopped).toBeDefined();
    // The two silos end up close to even (started 20/80, gap 60).
    expect(Math.abs(loads.get("a")! - loads.get("b")!)).toBeLessThan(10);
  });
});
