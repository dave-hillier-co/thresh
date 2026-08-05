import { createServer } from "node:net";
import { SiloAddress } from "@thresh/core/silo-address";
import { createSilo } from "@thresh/hosting/silo-builder";
import type { SiloHost } from "@thresh/hosting/silo-host";
import { StaticMembershipService } from "@thresh/runtime/static-membership";
import { leaderboard } from "@thresh/example-cluster/interfaces";
import { LeaderboardGrain } from "@thresh/example-cluster/leaderboard-grain";

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

const tick = () => new Promise((r) => setTimeout(r, 0));

/**
 * Run an idempotent call, retrying until it converges. Right after a silo dies,
 * the first attempts from another silo can race the cluster's view update (a
 * cached location still points at the departed silo); we bound each attempt so a
 * call to a gone silo can't stall the caller, and retry until routing re-resolves.
 */
export async function untilConverged<T>(call: () => Promise<T>): Promise<T> {
  let lastError: unknown;
  for (let attempt = 0; attempt < 50; attempt++) {
    await tick();
    const pending = call();
    pending.catch(() => undefined); // a bounded-out attempt may settle later; don't leak it
    try {
      return await Promise.race([
        pending,
        new Promise<never>((_, reject) => setTimeout(() => reject(new Error("retry")), 250)),
      ]);
    } catch (error) {
      lastError = error;
    }
  }
  throw lastError;
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
      .registerGrain(LeaderboardGrain, { interfaces: [leaderboard] })
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
