import { describe, expect, it } from "vitest";
import { GrainId } from "@thresh/core/grain-id";
import { leaderboard } from "@thresh/example-cluster/interfaces";
import { buildWebSocketCluster, untilConverged } from "@thresh/example-cluster/cluster";
import { runClusterDemo } from "@thresh/example-cluster/demo";

const LEADERBOARD = new GrainId("Leaderboard", "global");

describe("WebSocket cluster (cross-silo routing acceptance)", () => {
  it("routes calls from every silo to one shared activation over real sockets", async () => {
    const cluster = await buildWebSocketCluster(3);
    await cluster.start();
    try {
      // Each record originates on a different silo; all must reach one activation.
      await cluster.silos[0]!.getGrain(leaderboard, "global").record("ada", 10);
      await cluster.silos[1]!.getGrain(leaderboard, "global").record("grace", 30);
      await cluster.silos[2]!.getGrain(leaderboard, "global").record("ada", 25);

      // A read from yet another silo sees every write — proof of one tally.
      expect(await cluster.silos[1]!.getGrain(leaderboard, "global").top()).toEqual([
        { player: "grace", score: 30 },
        { player: "ada", score: 25 },
      ]);

      // Directory CAS means exactly one silo hosts the grain.
      expect(cluster.silos.filter((s) => s.isActive(LEADERBOARD))).toHaveLength(1);
    } finally {
      await cluster.stop();
    }
    // Real-socket, multi-silo: generous timeout so CPU starvation under a loaded
    // parallel suite can't time it out (it runs in ~0.4s in isolation).
  }, 60_000);

  it("reactivates the grain on a surviving silo when its host leaves the view", async () => {
    const cluster = await buildWebSocketCluster(3);
    await cluster.start();
    try {
      await cluster.silos[0]!.getGrain(leaderboard, "global").record("ada", 10);
      const hostIndex = cluster.silos.findIndex((s) => s.isActive(LEADERBOARD));
      expect(hostIndex).toBeGreaterThanOrEqual(0);

      // The hosting silo dies: stop it and drop it from the shared view.
      await cluster.silos[hostIndex]!.stop();
      cluster.membership.removeSilo(cluster.addresses[hostIndex]!);

      // A surviving silo's next call reactivates the grain elsewhere (fresh, since
      // state was in-memory on the dead silo); it retries while routing re-resolves.
      const survivor = cluster.silos.find((_, i) => i !== hostIndex)!;
      await untilConverged(() => survivor.getGrain(leaderboard, "global").record("grace", 5));

      const survivingHosts = cluster.silos.filter(
        (s, i) => i !== hostIndex && s.isActive(LEADERBOARD),
      );
      expect(survivingHosts).toHaveLength(1);
      expect(await survivor.getGrain(leaderboard, "global").top()).toEqual([
        { player: "grace", score: 5 },
      ]);
    } finally {
      await Promise.all(cluster.silos.map((s) => s.stop().catch(() => undefined)));
    }
  }, 60_000);

  it("runs the runnable demo end-to-end", async () => {
    const result = await runClusterDemo();
    expect(result.top).toEqual([
      { player: "grace", score: 30 },
      { player: "ada", score: 25 },
    ]);
    expect(result.afterFailover.newHostSilo).not.toBe(result.hostSilo);
    expect(result.afterFailover.top).toEqual([{ player: "grace", score: 5 }]);
  }, 60_000);
});
