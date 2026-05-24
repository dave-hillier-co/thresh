import { describe, expect, it } from "vitest";
import { HealthCheck } from "@tsva/hosting/health-check";

const healthy = {
  started: true,
  transportReady: true,
  membershipHealthy: true,
  draining: false,
  overloaded: false,
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
});
