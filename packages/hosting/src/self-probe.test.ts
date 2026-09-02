import { describe, expect, it } from "vitest";
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
});
