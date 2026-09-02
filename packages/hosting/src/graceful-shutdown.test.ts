import { describe, expect, it } from "vitest";
import { HealthCheck } from "@thresh/hosting/health-check";
import { GracefulShutdown, type Drainable } from "@thresh/hosting/graceful-shutdown";

const ready = {
  started: true,
  transportReady: true,
  membershipHealthy: true,
  draining: false,
  overloaded: false,
};

describe("GracefulShutdown", () => {
  it("flips readiness to not-ready before stopping the node", async () => {
    const health = new HealthCheck();
    health.update(ready);
    let readyAtStop: boolean | undefined;
    const node: Drainable = {
      stop: async () => {
        readyAtStop = health.ready().ok;
      },
    };

    // A no-op injected delay: this test is about ordering, not timing, and the default
    // graceMs is non-zero (see the "defaults graceMs" test below) — asserting ordering
    // without stubbing the delay would make this test wait out the real default.
    await new GracefulShutdown(health, node, { delay: () => Promise.resolve() }).drain();

    expect(readyAtStop).toBe(false); // not-ready was published before stop()
    expect(health.ready().ok).toBe(false);
  });

  it("waits the grace period before stopping", async () => {
    const health = new HealthCheck();
    const order: string[] = [];
    const node: Drainable = {
      stop: async () => {
        order.push("stop");
      },
    };
    const delay = (ms: number): Promise<void> => {
      order.push(`delay:${ms}`);
      return Promise.resolve();
    };

    await new GracefulShutdown(health, node, { graceMs: 5000, delay }).drain();
    expect(order).toEqual(["delay:5000", "stop"]);
  });

  it("defaults graceMs to a non-zero grace period, so a peer's readiness watch has time to observe the flip before the node stops", async () => {
    const health = new HealthCheck();
    let observedMs: number | undefined;
    const node: Drainable = { stop: async () => {} };
    const delay = (ms: number): Promise<void> => {
      observedMs = ms;
      return Promise.resolve();
    };

    await new GracefulShutdown(health, node, { delay }).drain();

    expect(observedMs).toBeGreaterThan(0);
  });

  it("drains only once", async () => {
    const health = new HealthCheck();
    let stops = 0;
    const node: Drainable = {
      stop: async () => void stops++,
    };
    const shutdown = new GracefulShutdown(health, node, { delay: () => Promise.resolve() });
    await Promise.all([shutdown.drain(), shutdown.drain()]);
    await shutdown.drain();
    expect(stops).toBe(1);
  });
});
