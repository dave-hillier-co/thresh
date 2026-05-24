import { describe, expect, it } from "vitest";
import { HealthCheck } from "@tsva/hosting/health-check";
import { GracefulShutdown, type Drainable } from "@tsva/hosting/graceful-shutdown";

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

    await new GracefulShutdown(health, node).drain();

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

  it("drains only once", async () => {
    const health = new HealthCheck();
    let stops = 0;
    const node: Drainable = {
      stop: async () => void stops++,
    };
    const shutdown = new GracefulShutdown(health, node);
    await Promise.all([shutdown.drain(), shutdown.drain()]);
    await shutdown.drain();
    expect(stops).toBe(1);
  });
});
