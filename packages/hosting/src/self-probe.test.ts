import { describe, expect, it } from "vitest";
import type { LogFields, Logger } from "@thresh/core/logger";
import { FakeTimeProvider } from "@thresh/core/test-support/fake-time-provider";
import { HealthCheck } from "@thresh/hosting/health-check";
import { SelfProbeWorker } from "@thresh/hosting/self-probe";

const flush = (): Promise<void> => new Promise((r) => setTimeout(r, 0));

/** A probe function whose resolution/rejection the test drives by hand. */
function pendingProbe(): { probe: () => Promise<void>; settle: (error?: Error) => void } {
  let settle!: (error?: Error) => void;
  const probe = () =>
    new Promise<void>((resolve, reject) => {
      settle = (error?: Error) => (error === undefined ? resolve() : reject(error));
    });
  return { probe, settle: (error?: Error) => settle(error) };
}

describe("SelfProbeWorker", () => {
  it("does not flip dispatcherResponsive false on misses below missedThreshold", async () => {
    const time = new FakeTimeProvider();
    const health = new HealthCheck();
    let calls = 0;
    const worker = new SelfProbeWorker({
      probe: () => {
        calls += 1;
        return Promise.reject(new Error("boom"));
      },
      health,
      time,
      intervalMs: 1000,
      timeoutMs: 100,
      missedThreshold: 3,
    });
    try {
      worker.start();
      time.advance(1000);
      await flush();
      time.advance(1000);
      await flush();

      expect(calls).toBe(2);
      expect(health.ready().checks.dispatcherResponsive).not.toBe(false);
    } finally {
      worker.stop();
    }
  });

  it("flips dispatcherResponsive false on reaching missedThreshold consecutive misses", async () => {
    const time = new FakeTimeProvider();
    const health = new HealthCheck();
    health.update({
      started: true,
      transportReady: true,
      membershipHealthy: true,
      draining: false,
      overloaded: false,
    });
    const worker = new SelfProbeWorker({
      probe: () => Promise.reject(new Error("boom")),
      health,
      time,
      intervalMs: 1000,
      timeoutMs: 100,
      missedThreshold: 3,
    });
    try {
      worker.start();
      for (let i = 0; i < 3; i += 1) {
        time.advance(1000);
        await flush();
      }

      expect(health.ready().ok).toBe(false);
      expect(health.ready().checks.dispatcherResponsive).toBe(false);
    } finally {
      worker.stop();
    }
  });

  it("recovers dispatcherResponsive and resets the miss counter on the next success", async () => {
    const time = new FakeTimeProvider();
    const health = new HealthCheck();
    let shouldFail = true;
    const worker = new SelfProbeWorker({
      probe: () => (shouldFail ? Promise.reject(new Error("boom")) : Promise.resolve()),
      health,
      time,
      intervalMs: 1000,
      timeoutMs: 100,
      missedThreshold: 3,
    });
    try {
      worker.start();
      for (let i = 0; i < 3; i += 1) {
        time.advance(1000);
        await flush();
      }
      expect(health.ready().checks.dispatcherResponsive).toBe(false);

      shouldFail = false;
      time.advance(1000);
      await flush();

      expect(health.ready().checks.dispatcherResponsive).toBe(true);

      // The counter reset on recovery: two more misses (below missedThreshold)
      // must not flip it false again immediately.
      shouldFail = true;
      time.advance(1000);
      await flush();
      time.advance(1000);
      await flush();
      expect(health.ready().checks.dispatcherResponsive).toBe(true);
    } finally {
      worker.stop();
    }
  });

  it("enforces timeoutMs against a probe that never settles, without leaking an unhandled rejection", async () => {
    const time = new FakeTimeProvider();
    const health = new HealthCheck();
    const { probe, settle } = pendingProbe();
    const unhandled: unknown[] = [];
    const onUnhandledRejection = (reason: unknown) => unhandled.push(reason);
    process.on("unhandledRejection", onUnhandledRejection);

    const worker = new SelfProbeWorker({
      probe,
      health,
      time,
      intervalMs: 1000,
      timeoutMs: 100,
      missedThreshold: 1,
    });
    try {
      worker.start();
      time.advance(1000); // fires the first probe call
      await flush();
      time.advance(100); // the probe's own deadline fires before it ever settles
      await flush();

      expect(health.ready().checks.dispatcherResponsive).toBe(false);

      // The still-pending call finally settles (rejects) long after the
      // worker already decided the miss — this must not surface as an
      // unhandled rejection.
      settle(new Error("late rejection"));
      await flush();

      expect(unhandled).toHaveLength(0);
    } finally {
      process.off("unhandledRejection", onUnhandledRejection);
      worker.stop();
    }
  });

  it("a rejecting probe function itself never produces an unhandled promise rejection", async () => {
    const time = new FakeTimeProvider();
    const health = new HealthCheck();
    const unhandled: unknown[] = [];
    const onUnhandledRejection = (reason: unknown) => unhandled.push(reason);
    process.on("unhandledRejection", onUnhandledRejection);

    const worker = new SelfProbeWorker({
      probe: () => Promise.reject(new Error("boom")),
      health,
      time,
      intervalMs: 1000,
      timeoutMs: 100,
      missedThreshold: 1,
    });
    try {
      worker.start();
      time.advance(1000);
      await flush();
      time.advance(1000);
      await flush();

      expect(unhandled).toHaveLength(0);
    } finally {
      process.off("unhandledRejection", onUnhandledRejection);
      worker.stop();
    }
  });

  it("does not flip readiness while the silo is draining", async () => {
    const time = new FakeTimeProvider();
    const health = new HealthCheck();
    health.update({ draining: true });
    const worker = new SelfProbeWorker({
      probe: () => Promise.reject(new Error("boom")),
      health,
      time,
      intervalMs: 1000,
      timeoutMs: 100,
      missedThreshold: 1,
    });
    try {
      worker.start();
      time.advance(1000);
      await flush();
      time.advance(1000);
      await flush();

      // dispatcherResponsive stays at its optimistic default: a drain already
      // pulls the endpoint from service via `notDraining`, and the probe's
      // misses during drain (connections closing, turns being rejected) must
      // not additionally flip a signal that isn't about the drain.
      expect(health.ready().checks.dispatcherResponsive).toBe(true);
    } finally {
      worker.stop();
    }
  });

  describe("reporting", () => {
    function recorder(): {
      logger: Logger;
      entries: Array<{ level: string; message: string; fields?: LogFields }>;
    } {
      const entries: Array<{ level: string; message: string; fields?: LogFields }> = [];
      const at =
        (level: string) =>
        (message: string, fields?: LogFields): void => {
          entries.push({ level, message, ...(fields !== undefined ? { fields } : {}) });
        };
      return {
        entries,
        logger: { debug: at("debug"), info: at("info"), warn: at("warn"), error: at("error") },
      };
    }

    it("logs the readiness flip when the dispatcher stops answering, and the recovery", async () => {
      const time = new FakeTimeProvider();
      const health = new HealthCheck();
      const { logger, entries } = recorder();
      let fail = true;
      const worker = new SelfProbeWorker({
        probe: () => (fail ? Promise.reject(new Error("boom")) : Promise.resolve()),
        health,
        time,
        logger,
        intervalMs: 1000,
        timeoutMs: 100,
        missedThreshold: 2,
      });
      try {
        worker.start();
        for (let i = 0; i < 3; i += 1) {
          time.advance(1000);
          await flush();
        }
        // One line for the flip, not one per further miss.
        expect(entries.filter((e) => e.level === "warn").map((e) => e.message)).toEqual([
          "self-probe failed; the silo is no longer ready",
        ]);
        expect(entries.find((e) => e.level === "warn")!.fields).toMatchObject({
          consecutiveMisses: 2,
          error: "boom",
        });

        fail = false;
        time.advance(1000);
        await flush();
        expect(entries.map((e) => [e.level, e.message]).at(-1)).toEqual([
          "info",
          "self-probe recovered; the silo is ready again",
        ]);
      } finally {
        worker.stop();
      }
    });

    it("records when the failure streak began, for liveness", async () => {
      const time = new FakeTimeProvider();
      const health = new HealthCheck({ time, livenessFailureMs: 1500 });
      let fail = true;
      const worker = new SelfProbeWorker({
        probe: () => (fail ? Promise.reject(new Error("boom")) : Promise.resolve()),
        health,
        time,
        intervalMs: 1000,
        timeoutMs: 100,
        missedThreshold: 5,
      });
      try {
        worker.start();
        time.advance(1000);
        await flush();
        expect(health.live().ok).toBe(true);
        time.advance(1000);
        await flush();
        time.advance(1000);
        await flush();
        expect(health.live().ok).toBe(false);

        fail = false;
        time.advance(1000);
        await flush();
        expect(health.live().ok).toBe(true);
      } finally {
        worker.stop();
      }
    });
  });
});

describe("SelfProbeWorker with a logger that throws", () => {
  it("still flips dispatcherResponsive false, without an unhandled rejection", async () => {
    const time = new FakeTimeProvider();
    const health = new HealthCheck();
    health.update({
      started: true,
      transportReady: true,
      membershipHealthy: true,
      draining: false,
      overloaded: false,
    });
    const broken: Logger = {
      debug() {},
      info() {
        throw new Error("logger down");
      },
      warn() {
        throw new Error("logger down");
      },
      error() {},
    };
    const worker = new SelfProbeWorker({
      probe: () => Promise.reject(new Error("boom")),
      health,
      time,
      intervalMs: 1000,
      timeoutMs: 100,
      missedThreshold: 1,
      logger: broken,
    });
    try {
      worker.start();
      time.advance(1000);
      await flush();
      expect(health.ready().checks.dispatcherResponsive).toBe(false);
    } finally {
      worker.stop();
    }
  });
});
