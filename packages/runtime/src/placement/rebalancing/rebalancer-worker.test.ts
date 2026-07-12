import { describe, expect, it } from "vitest";
import { SiloAddress } from "@tsva/core/silo-address";
import type { MembershipService, MembershipSnapshot } from "@tsva/core/membership";
import { FakeTimeProvider } from "@tsva/core/test-support/fake-time-provider";
import type { CycleState, StopReason } from "@tsva/runtime/placement/rebalancing/rebalancer-model";
import { DEFAULT_REBALANCER_OPTIONS } from "@tsva/runtime/placement/rebalancing/rebalancer-model";
import {
  ActivationRebalancerWorker,
  type RebalanceCycleResult,
} from "@tsva/runtime/placement/rebalancing/rebalancer-worker";

const silo = (n: number) => new SiloAddress(`silo-${n}`, `uid-${n}`, `silo-${n}:1111${n}`);

/** A membership service backed by a fixed list of active silos. */
function membershipOf(local: SiloAddress, silos: readonly SiloAddress[]): MembershipService {
  const snapshot: MembershipSnapshot = {
    version: 1,
    silos: silos.map((address) => ({ address, status: "active" as const })),
  };
  return {
    current: () => snapshot,
    localSilo: () => local,
    async *updates() {
      // no changes in these tests
    },
  };
}

/** A cycle runner that records how many times it ran and returns a fixed result each tick. */
function recordingRunner(results: RebalanceCycleResult[]): {
  run: (state: CycleState) => Promise<RebalanceCycleResult>;
  calls: number;
} {
  const rec = {
    calls: 0,
    run: async (_state: CycleState): Promise<RebalanceCycleResult> => {
      const result = results[Math.min(rec.calls, results.length - 1)]!;
      rec.calls++;
      return result;
    },
  };
  return rec;
}

const period = 1000;
const opts = { ...DEFAULT_REBALANCER_OPTIONS };

describe("ActivationRebalancerWorker — elected timer-driven singleton (slice 2b)", () => {
  it("runs cycles only on the elected leader (lowest active ring key)", async () => {
    const silos = [silo(2), silo(0), silo(1)];
    const time = new FakeTimeProvider();
    const leaderRunner = recordingRunner([
      {
        nextState: { rebalancingCycle: 1, stagnantCycles: 0, previousEntropy: 0.5 },
        imbalance: 0.4,
        moved: 2,
      },
    ]);
    const followerRunner = recordingRunner([
      {
        nextState: { rebalancingCycle: 1, stagnantCycles: 0, previousEntropy: 0.5 },
        imbalance: 0.4,
        moved: 2,
      },
    ]);

    // silo-0 is the leader (lowest ring key); silo-1 is a follower.
    const leader = new ActivationRebalancerWorker({
      local: silo(0),
      membership: membershipOf(silo(0), silos),
      time,
      runCycle: leaderRunner.run,
      options: opts,
      sessionCyclePeriodMs: period,
    });
    const follower = new ActivationRebalancerWorker({
      local: silo(1),
      membership: membershipOf(silo(1), silos),
      time,
      runCycle: followerRunner.run,
      options: opts,
      sessionCyclePeriodMs: period,
    });

    leader.start();
    follower.start();
    try {
      time.advance(period);
      await Promise.resolve();
      await Promise.resolve();
      expect(leaderRunner.calls).toBe(1);
      expect(followerRunner.calls).toBe(0);
    } finally {
      leader.stop();
      follower.stop();
    }
  });

  it("threads cycle state between ticks", async () => {
    const silos = [silo(0), silo(1)];
    const time = new FakeTimeProvider();
    const seen: CycleState[] = [];
    const worker = new ActivationRebalancerWorker({
      local: silo(0),
      membership: membershipOf(silo(0), silos),
      time,
      runCycle: async (state) => {
        seen.push(state);
        const cycle = state.rebalancingCycle + 1;
        return {
          nextState: { rebalancingCycle: cycle, stagnantCycles: 0, previousEntropy: cycle },
          imbalance: 0.3,
          moved: 1,
        };
      },
      options: opts,
      sessionCyclePeriodMs: period,
    });
    worker.start();
    try {
      time.advance(period);
      await Promise.resolve();
      await Promise.resolve();
      time.advance(period);
      await Promise.resolve();
      await Promise.resolve();
      expect(seen[0]!.rebalancingCycle).toBe(0);
      expect(seen[1]!.rebalancingCycle).toBe(1);
    } finally {
      worker.stop();
    }
  });

  it("backs off after a session completes (cools down one period) and resets state", async () => {
    const silos = [silo(0), silo(1)];
    const time = new FakeTimeProvider();
    const states: CycleState[] = [];
    let tick = 0;
    const worker = new ActivationRebalancerWorker({
      local: silo(0),
      membership: membershipOf(silo(0), silos),
      time,
      runCycle: async (state) => {
        states.push(state);
        tick++;
        const stop: StopReason | undefined = tick === 1 ? "complete" : undefined;
        return {
          nextState: { rebalancingCycle: tick, stagnantCycles: 0, previousEntropy: 0 },
          imbalance: 0.0001,
          moved: 0,
          ...(stop !== undefined ? { stop } : {}),
        };
      },
      options: opts,
      sessionCyclePeriodMs: period,
    });
    worker.start();
    try {
      time.advance(period); // cycle 1 -> complete
      await Promise.resolve();
      await Promise.resolve();
      // Cool-down: the next cycle starts from INITIAL state again.
      time.advance(period);
      await Promise.resolve();
      await Promise.resolve();
      expect(states).toHaveLength(2);
      expect(states[1]!.rebalancingCycle).toBe(0);
    } finally {
      worker.stop();
    }
  });

  it("backs off exponentially on stagnation", async () => {
    const silos = [silo(0), silo(1)];
    const time = new FakeTimeProvider();
    let calls = 0;
    const worker = new ActivationRebalancerWorker({
      local: silo(0),
      membership: membershipOf(silo(0), silos),
      time,
      runCycle: async (_state) => {
        calls++;
        return {
          nextState: { rebalancingCycle: 1, stagnantCycles: 3, previousEntropy: 0 },
          imbalance: 0.5,
          moved: 0,
          stop: "stagnated",
        };
      },
      options: opts,
      sessionCyclePeriodMs: period,
    });
    worker.start();
    try {
      time.advance(period); // cycle 1 -> stagnated, backoff = 2*period
      await Promise.resolve();
      await Promise.resolve();
      expect(calls).toBe(1);
      // One period later is NOT enough — backoff is 2 periods.
      time.advance(period);
      await Promise.resolve();
      await Promise.resolve();
      expect(calls).toBe(1);
      time.advance(period);
      await Promise.resolve();
      await Promise.resolve();
      expect(calls).toBe(2);
    } finally {
      worker.stop();
    }
  });

  it("suspends and resumes", async () => {
    const silos = [silo(0), silo(1)];
    const time = new FakeTimeProvider();
    const runner = recordingRunner([
      {
        nextState: { rebalancingCycle: 1, stagnantCycles: 0, previousEntropy: 0 },
        imbalance: 0.3,
        moved: 1,
      },
    ]);
    const worker = new ActivationRebalancerWorker({
      local: silo(0),
      membership: membershipOf(silo(0), silos),
      time,
      runCycle: runner.run,
      options: opts,
      sessionCyclePeriodMs: period,
    });
    worker.start();
    try {
      worker.suspend();
      expect(worker.report().status).toBe("suspended");
      time.advance(period);
      await Promise.resolve();
      await Promise.resolve();
      expect(runner.calls).toBe(0);
      worker.resume();
      expect(worker.report().status).toBe("executing");
      time.advance(period);
      await Promise.resolve();
      await Promise.resolve();
      expect(runner.calls).toBe(1);
    } finally {
      worker.stop();
    }
  });

  it("suspends indefinitely by default, reporting a (very large) suspension duration", async () => {
    const silos = [silo(0), silo(1)];
    const time = new FakeTimeProvider();
    const worker = new ActivationRebalancerWorker({
      local: silo(0),
      membership: membershipOf(silo(0), silos),
      time,
      runCycle: recordingRunner([]).run,
      options: opts,
      sessionCyclePeriodMs: period,
    });
    worker.start();
    try {
      expect(worker.report().suspensionDurationMs).toBeUndefined();
      worker.suspend();
      const report = worker.report();
      expect(report.status).toBe("suspended");
      expect(report.suspensionDurationMs).not.toBeUndefined();
      expect(report.suspensionDurationMs).toBe(Number.POSITIVE_INFINITY);
    } finally {
      worker.stop();
    }
  });

  it("suspends for a duration and auto-resumes off the shared clock", async () => {
    const silos = [silo(0), silo(1)];
    const time = new FakeTimeProvider();
    const runner = recordingRunner([
      {
        nextState: { rebalancingCycle: 1, stagnantCycles: 0, previousEntropy: 0 },
        imbalance: 0.3,
        moved: 1,
      },
    ]);
    const worker = new ActivationRebalancerWorker({
      local: silo(0),
      membership: membershipOf(silo(0), silos),
      time,
      runCycle: runner.run,
      options: opts,
      sessionCyclePeriodMs: period,
    });
    worker.start();
    try {
      worker.suspend(500);
      expect(worker.report().status).toBe("suspended");
      expect(worker.report().suspensionDurationMs).toBeLessThanOrEqual(500);
      expect(worker.report().suspensionDurationMs).toBeGreaterThan(0);

      // The 500ms suspension auto-resumes (at t=500) before the next scheduled
      // tick (at t=1000, the full period), so that tick runs normally.
      time.advance(period);
      await Promise.resolve();
      await Promise.resolve();
      expect(worker.report().status).toBe("executing");
      expect(worker.report().suspensionDurationMs).toBeUndefined();
      expect(runner.calls).toBe(1);
    } finally {
      worker.stop();
    }
  });

  it("notifies subscribed report listeners after each cycle, and stops after unsubscribe", async () => {
    const silos = [silo(0), silo(1)];
    const time = new FakeTimeProvider();
    const runner = recordingRunner([
      {
        nextState: { rebalancingCycle: 1, stagnantCycles: 0, previousEntropy: 0 },
        imbalance: 0.3,
        moved: 1,
      },
    ]);
    const worker = new ActivationRebalancerWorker({
      local: silo(0),
      membership: membershipOf(silo(0), silos),
      time,
      runCycle: runner.run,
      options: opts,
      sessionCyclePeriodMs: period,
    });
    const reports: Array<{ status: string }> = [];
    const listener = (report: { status: string }) => reports.push(report);
    worker.subscribe(listener);

    worker.start();
    try {
      time.advance(period);
      await Promise.resolve();
      await Promise.resolve();
      expect(reports.length).toBeGreaterThan(0);
      expect(reports.at(-1)!.status).toBe("executing");

      worker.unsubscribe(listener);
      const before = reports.length;
      worker.suspend();
      expect(reports.length).toBe(before); // unsubscribed: no further notifications
    } finally {
      worker.stop();
    }
  });

  it("tracks a report with host, imbalance and per-silo dispersed/acquired", async () => {
    const silos = [silo(0), silo(1)];
    const time = new FakeTimeProvider();
    const worker = new ActivationRebalancerWorker({
      local: silo(0),
      membership: membershipOf(silo(0), silos),
      time,
      runCycle: async (_state) => ({
        nextState: { rebalancingCycle: 1, stagnantCycles: 0, previousEntropy: 0.5 },
        imbalance: 0.42,
        moved: 3,
        moves: [{ from: "silo-1", to: "silo-0", count: 3 }],
      }),
      options: opts,
      sessionCyclePeriodMs: period,
    });
    worker.start();
    try {
      time.advance(period);
      await Promise.resolve();
      await Promise.resolve();
      const report = worker.report();
      expect(report.host).toBe("silo-0");
      expect(report.status).toBe("executing");
      expect(report.clusterImbalance).toBeCloseTo(0.42);
      expect(report.dispersed.get("silo-1")).toBe(3);
      expect(report.acquired.get("silo-0")).toBe(3);
    } finally {
      worker.stop();
    }
  });
});
