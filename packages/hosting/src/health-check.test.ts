import { describe, expect, it } from "vitest";
import { FakeTimeProvider } from "@thresh/core/test-support/fake-time-provider";
import { HealthCheck } from "@thresh/hosting/health-check";

const healthy = {
  started: true,
  transportReady: true,
  membershipHealthy: true,
  draining: false,
  overloaded: false,
  dispatcherResponsive: true,
};

describe("HealthCheck", () => {
  it("is live as long as it can answer", () => {
    expect(new HealthCheck().live().ok).toBe(true);
  });

  it("is not started until startup completes", () => {
    const health = new HealthCheck();
    expect(health.startup().ok).toBe(false);
    health.update({ started: true });
    expect(health.startup().ok).toBe(true);
  });

  it("is ready only when every condition holds", () => {
    const health = new HealthCheck();
    expect(health.ready().ok).toBe(false);
    health.update(healthy);
    expect(health.ready().ok).toBe(true);
  });

  it("is not ready while draining (so K8s pulls it from endpoints)", () => {
    const health = new HealthCheck();
    health.update({ ...healthy, draining: true });
    const probe = health.ready();
    expect(probe.ok).toBe(false);
    expect(probe.checks.notDraining).toBe(false);
  });

  it("is not ready when overloaded", () => {
    const health = new HealthCheck();
    health.update({ ...healthy, overloaded: true });
    expect(health.ready().ok).toBe(false);
  });

  it("is not ready if the membership watch is unhealthy", () => {
    const health = new HealthCheck();
    health.update({ ...healthy, membershipHealthy: false });
    expect(health.ready().ok).toBe(false);
  });

  it("starts dispatcher-responsive (optimistic, before the first self-probe cycle runs)", () => {
    const health = new HealthCheck();
    health.update({ ...healthy });
    expect(health.ready().ok).toBe(true);
  });

  it("is not ready when the self-probe worker reports a hung dispatcher", () => {
    const health = new HealthCheck();
    health.update({ ...healthy, dispatcherResponsive: false });
    const probe = health.ready();
    expect(probe.ok).toBe(false);
    expect(probe.checks.dispatcherResponsive).toBe(false);
  });

  it("exposes draining so SelfProbeWorker can gate its readiness flip on it", () => {
    const health = new HealthCheck();
    expect(health.isDraining()).toBe(false);
    health.update({ draining: true });
    expect(health.isDraining()).toBe(true);
  });

  it("follows a live overload source instead of a flag nothing sets", () => {
    let overloaded = false;
    const health = new HealthCheck();
    health.update(healthy);
    health.followOverload(() => overloaded);
    expect(health.ready().ok).toBe(true);
    overloaded = true;
    expect(health.ready().ok).toBe(false);
    expect(health.ready().checks.notOverloaded).toBe(false);
    overloaded = false;
    expect(health.ready().ok).toBe(true);
  });

  describe("liveness on a dispatcher that stays unresponsive", () => {
    it("stays live however long the probe fails when no period is configured", () => {
      const time = new FakeTimeProvider();
      const health = new HealthCheck({ time });
      health.update({ probeFailingSinceMs: time.now() });
      time.advance(24 * 3600_000);
      expect(health.live().ok).toBe(true);
    });

    it("fails once the probe has failed continuously for the configured period", () => {
      const time = new FakeTimeProvider();
      const health = new HealthCheck({ time, livenessFailureMs: 60_000 });
      health.update({ probeFailingSinceMs: time.now() });
      time.advance(59_999);
      expect(health.live().ok).toBe(true);
      time.advance(1);
      const probe = health.live();
      expect(probe.ok).toBe(false);
      expect(probe.checks.dispatcherResponsive).toBe(false);
    });

    it("recovers when the probe succeeds again", () => {
      const time = new FakeTimeProvider();
      const health = new HealthCheck({ time, livenessFailureMs: 1000 });
      health.update({ probeFailingSinceMs: time.now() });
      time.advance(5000);
      expect(health.live().ok).toBe(false);
      health.update({ probeFailingSinceMs: undefined });
      expect(health.live().ok).toBe(true);
    });

    it("does not blame a draining silo for probe misses", () => {
      const time = new FakeTimeProvider();
      const health = new HealthCheck({ time, livenessFailureMs: 1000 });
      health.update({ probeFailingSinceMs: time.now(), draining: true });
      time.advance(5000);
      expect(health.live().ok).toBe(true);
    });
  });
});
