import { createServer } from "node:net";
import { SiloAddress } from "@tsva/core/silo-address";
import { createSilo } from "@tsva/hosting/silo-builder";
import type { SiloHost } from "@tsva/hosting/silo-host";
import { StaticMembershipService } from "@tsva/runtime/static-membership";
import { ILeaderboard } from "@tsva/example-cluster/interfaces";
import { LeaderboardGrain } from "@tsva/example-cluster/leaderboard-grain";

const CLUSTER = "leaderboard";

/** Ask the OS for an unused TCP port so silos don't collide on a fixed one. */
function freePort(): Promise<number> {
  return new Promise((resolve, reject) => {
    const probe = createServer();
    probe.once("error", reject);
    probe.listen(0, "127.0.0.1", () => {
      const info = probe.address();
      const port = typeof info === "object" && info !== null ? info.port : 0;
      probe.close(() => resolve(port));
    });
  });
}

export interface Cluster {
  readonly silos: SiloHost[];
  readonly addresses: SiloAddress[];
  /** The shared view; mutate it (e.g. `removeSilo`) to simulate a membership change. */
  readonly membership: StaticMembershipService;
  start(): Promise<void>;
  stop(): Promise<void>;
}

/**
 * Stands up `count` silos in one process, each listening on its own real
 * WebSocket port, all sharing one membership view so a change reaches every
 * silo. Placement is deterministic (first ring candidate) so the example reads
 * predictably.
 */
export async function buildWebSocketCluster(count: number): Promise<Cluster> {
  const addresses: SiloAddress[] = [];
  for (let i = 0; i < count; i++) {
    const port = await freePort();
    addresses.push(new SiloAddress(`silo-${i}`, `uid-${i}`, `127.0.0.1:${port}`));
  }

  // One shared membership service: every silo reads the same view and observes
  // the same updates (ClusterNode keys off `current()`, not `localSilo()`).
  const membership = new StaticMembershipService(addresses[0]!, addresses);

  const silos = addresses.map((local) =>
    createSilo({ clusterId: CLUSTER, local, random: () => 0 })
      .useMembership(membership)
      .useWebSocketTransport()
      .registerGrain(LeaderboardGrain, { interfaces: [ILeaderboard] })
      .build(),
  );

  return {
    silos,
    addresses,
    membership,
    start: async () => {
      for (const silo of silos) await silo.start();
    },
    stop: async () => {
      await Promise.all(silos.map((silo) => silo.stop()));
    },
  };
}
