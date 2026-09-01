import { GrainId } from "@thresh/core/grain-id";
import { Leaderboard, type ScoreEntry } from "@thresh/example-cluster/leaderboard-grain";
import { buildWebSocketCluster, untilConverged } from "@thresh/example-cluster/cluster";

export interface ClusterDemoResult {
  /** Which silo the single leaderboard activation landed on. */
  hostSilo: string;
  /** The board after records issued from three different silos. */
  top: ScoreEntry[];
  /** After the host silo dies: the new host and the (fresh) board. */
  afterFailover: { newHostSilo: string; top: ScoreEntry[] };
}

const LEADERBOARD = new GrainId("Leaderboard", "global");

/**
 * Runs three silos over the real WebSocket transport. Records scores from every
 * silo against one cluster-wide leaderboard grain — showing location-transparent
 * cross-silo routing to a single activation — then kills the hosting silo and
 * shows the grain reactivating elsewhere on the next call.
 */
export async function runClusterDemo(): Promise<ClusterDemoResult> {
  const cluster = await buildWebSocketCluster(3);
  await cluster.start();
  try {
    await cluster.silos[0]!.getGrain(Leaderboard, "global").record("ada", 10);
    await cluster.silos[1]!.getGrain(Leaderboard, "global").record("grace", 30);
    await cluster.silos[2]!.getGrain(Leaderboard, "global").record("ada", 25);

    const hostIndex = cluster.silos.findIndex((s) => s.isActive(LEADERBOARD));
    const hostSilo = cluster.addresses[hostIndex]!.podName;
    const top = await cluster.silos[1]!.getGrain(Leaderboard, "global").top();

    // The hosting silo dies.
    await cluster.silos[hostIndex]!.stop();
    cluster.membership.removeSilo(cluster.addresses[hostIndex]!);

    // A survivor records again; it retries while the cluster re-resolves the grain.
    const survivor = cluster.silos.find((_, i) => i !== hostIndex)!;
    await untilConverged(() => survivor.getGrain(Leaderboard, "global").record("grace", 5));
    const newHostIndex = cluster.silos.findIndex(
      (s, i) => i !== hostIndex && s.isActive(LEADERBOARD),
    );

    return {
      hostSilo,
      top,
      afterFailover: {
        newHostSilo: cluster.addresses[newHostIndex]!.podName,
        top: await survivor.getGrain(Leaderboard, "global").top(),
      },
    };
  } finally {
    await Promise.all(cluster.silos.map((s) => s.stop().catch(() => undefined)));
  }
}
