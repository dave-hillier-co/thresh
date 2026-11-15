import { describe, expect, it } from "vitest";
import { newActivationId } from "@thresh/core/activation-id";
import type { GrainAddress } from "@thresh/core/grain-address";
import { GrainId } from "@thresh/core/grain-id";
import type { MembershipSnapshot, SiloStatus } from "@thresh/core/membership";
import type { InvocationRequest } from "@thresh/core/request";
import { SiloAddress } from "@thresh/core/silo-address";
import { FakeTimeProvider } from "@thresh/core/test-support/fake-time-provider";
import type { DirectoryPeer } from "@thresh/directory/directory-peer";
import { DirectoryView } from "@thresh/directory/directory-view";
import { DistributedGrainDirectory } from "@thresh/directory/distributed-grain-directory";
import {
  GrainDirectoryPartition,
  type PartitionPeers,
} from "@thresh/directory/grain-directory-partition";
import { LocationCache } from "@thresh/directory/location-cache";
import { connectFailure, type ConnectFailureCause } from "@thresh/messaging/transport";
import type { Catalog } from "@thresh/runtime/catalog";
import { DistributedDispatcher, type RemoteInvoker } from "@thresh/runtime/distributed-dispatcher";

/**
 * A refused dial removes the directory entry that was followed, and only that entry
 * (docs/design-membership-and-directory.md, "A dead process that is still listed"). The caller
 * `A` and the owner `O` of the grain's entry each run a real partition and directory; the
 * network between them is a direct call, and the calls `A` sends to grain hosts go to a fake
 * remote that plays each host, alive or gone.
 */
const A = new SiloAddress("silo-a", "ua", "a:1");
const O = new SiloAddress("silo-o", "uo", "o:1");
const X = new SiloAddress("silo-x", "ux", "x:1");
const Y = new SiloAddress("silo-y", "uy", "y:1");

const VIEW = 7;
const snapshot: MembershipSnapshot = {
  version: VIEW,
  silos: (
    [
      [A, "active"],
      [O, "active"],
      [X, "joining"],
      [Y, "active"],
    ] as Array<[SiloAddress, SiloStatus]>
  ).map(([address, status]) => ({ address, status })),
};
const view = DirectoryView.of(snapshot);

/** A grain whose entry `O` owns. */
const grain = ((): GrainId => {
  for (let i = 0; ; i++) {
    const id = new GrainId("Counter", `k-${i}`);
    if (view.owns(O, id.getUniformHashCode())) return id;
  }
})();

const entryOn = (silo: SiloAddress): GrainAddress => ({
  grainId: grain,
  silo,
  activationId: newActivationId(),
});

const request = (oneWay = false): InvocationRequest => ({
  target: grain,
  interfaceId: 1,
  method: "inc",
  args: [],
  options: oneWay ? { oneWay: true } : {},
  reentrancyId: "r",
});

async function flush(): Promise<void> {
  for (let i = 0; i < 50; i++) await Promise.resolve();
}

class Silos {
  readonly time = new FakeTimeProvider();
  readonly partitions = new Map<string, GrainDirectoryPartition>();
  readonly directories = new Map<string, DistributedGrainDirectory>();
  readonly unregistrations: GrainAddress[] = [];

  static async start(): Promise<Silos> {
    const silos = new Silos();
    for (const p of silos.partitions.values()) p.applyView(view);
    await flush();
    for (const p of silos.partitions.values()) await p.settled();
    return silos;
  }

  private constructor() {
    for (const silo of [A, O]) {
      const peers: PartitionPeers = {
        getSnapshot: async () => undefined,
        acknowledgeSnapshot: async () => undefined,
        recoverActivations: async () => [],
        registeredActivations: async () => [],
      };
      const refresh = async (): Promise<void> => undefined;
      const partition = new GrainDirectoryPartition({
        local: silo,
        peers,
        activations: { registeredIn: () => [] },
        refreshView: refresh,
        time: this.time,
      });
      this.partitions.set(silo.podName, partition);
      const peer: DirectoryPeer = {
        lookup: async (owner, version, id) => this.partition(owner).lookup(version, id),
        register: async (owner, version, a, previous) =>
          this.partition(owner).register(version, a, previous),
        unregister: async (owner, version, a) => {
          this.unregistrations.push(a);
          return this.partition(owner).unregister(version, a);
        },
      };
      this.directories.set(
        silo.podName,
        new DistributedGrainDirectory({ local: silo, partition, peer, refresh, time: this.time }),
      );
    }
  }

  partition(silo: SiloAddress): GrainDirectoryPartition {
    return this.partitions.get(silo.podName)!;
  }

  /** Register `entry` at its owner, replacing `previous` if given; returns the entry held. */
  async register(entry: GrainAddress, previous?: GrainAddress): Promise<GrainAddress> {
    const result = await this.partition(O).register(VIEW, entry, previous);
    if (result.kind !== "ok") throw new Error("O does not own the grain");
    return result.value;
  }

  async held(): Promise<GrainAddress | undefined> {
    const result = await this.partition(O).lookup(VIEW, grain);
    return result.kind === "ok" ? result.value : undefined;
  }

  /** `A`'s dispatcher, sending calls to grain hosts through `remote`. */
  dispatcher(remote: RemoteInvoker, cache = new LocationCache()): DistributedDispatcher {
    return new DistributedDispatcher({
      local: A,
      directory: this.directories.get(A.podName)!,
      cache,
      // `A` hosts nothing here, and no call it makes in these tests is delivered on `A`.
      catalog: {
        isStatelessWorkerType: () => false,
        canonicalId: (id: GrainId) => id,
        resolveLive: async () => undefined,
      } as unknown as Catalog,
      remote,
      activeSilos: () => [Y],
      placementFor: () => ({ choose: (_type, candidates) => candidates[0]! }),
      filtersFor: () => [],
      placementContext: () => ({ random: () => 0 }),
    });
  }
}

/** A host that answers every call it is sent with its own name. */
const answering = (sent: SiloAddress[]): RemoteInvoker => ({
  send: async (silo) => {
    sent.push(silo);
    return `answered by ${silo.podName}`;
  },
});

/** Dials to `dead` fail with `cause` (once, if `onFirst` replaces the entry first); other hosts answer. */
function deadHost(
  dead: SiloAddress,
  cause: ConnectFailureCause,
  sent: SiloAddress[],
  onFirst?: () => Promise<void>,
): RemoteInvoker {
  let first = true;
  return {
    send: async (silo) => {
      sent.push(silo);
      if (silo.equals(dead) && (first || onFirst === undefined)) {
        first = false;
        await onFirst?.();
        throw connectFailure(silo, "no listener", cause);
      }
      return `answered by ${silo.podName}`;
    },
  };
}

describe("a refused dial removes the entry that was followed", () => {
  it("unregisters the entry it looked up and places the grain afresh", async () => {
    const silos = await Silos.start();
    const dead = await silos.register(entryOn(X));
    const sent: SiloAddress[] = [];

    const answer = await silos.dispatcher(deadHost(X, "refused", sent)).invoke(request());

    expect(answer).toBe("answered by silo-y");
    expect(sent).toEqual([X, Y]);
    expect(silos.unregistrations).toEqual([dead]);
    expect(await silos.held()).toBeUndefined();
  });

  it("unregisters the entry a cached route followed", async () => {
    const silos = await Silos.start();
    const dead = await silos.register(entryOn(X));
    const cache = new LocationCache();
    cache.put(dead);
    const sent: SiloAddress[] = [];

    const answer = await silos.dispatcher(deadHost(X, "refused", sent), cache).invoke(request());

    expect(answer).toBe("answered by silo-y");
    expect(silos.unregistrations).toEqual([dead]);
    expect(cache.get(grain)).toBeUndefined();
  });

  it("delivers a one-way call afresh too: it never left", async () => {
    const silos = await Silos.start();
    await silos.register(entryOn(X));
    const sent: SiloAddress[] = [];

    await silos.dispatcher(deadHost(X, "refused", sent)).invoke(request(true));
    await flush();

    expect(sent).toEqual([X, Y]);
    expect(await silos.held()).toBeUndefined();
  });

  it("leaves an entry that the same pod, restarted, registered meanwhile, and follows it", async () => {
    const silos = await Silos.start();
    const dead = await silos.register(entryOn(X));
    const restarted = entryOn(X);
    const sent: SiloAddress[] = [];
    const remote = deadHost(X, "refused", sent, async () => {
      await silos.register(restarted, dead);
    });

    const answer = await silos.dispatcher(remote).invoke(request());

    expect(answer).toBe("answered by silo-x");
    expect(sent).toEqual([X, X]);
    expect(silos.unregistrations).toEqual([dead]);
    expect(await silos.held()).toEqual(expect.objectContaining(restarted));
  });

  it("leaves an entry that another silo registered meanwhile, and follows it", async () => {
    const silos = await Silos.start();
    const dead = await silos.register(entryOn(X));
    const elsewhere = entryOn(O);
    const sent: SiloAddress[] = [];
    const remote = deadHost(X, "refused", sent, async () => {
      await silos.register(elsewhere, dead);
    });

    const answer = await silos.dispatcher(remote).invoke(request());

    expect(answer).toBe("answered by silo-o");
    expect(sent).toEqual([X, O]);
    expect(await silos.held()).toEqual(expect.objectContaining(elsewhere));
  });

  for (const cause of ["timeout", "handshake"] as const) {
    it(`removes nothing when the dial fails with a ${cause}, and fails the call as before`, async () => {
      const silos = await Silos.start();
      const entry = await silos.register(entryOn(X));
      const sent: SiloAddress[] = [];

      const err = await silos
        .dispatcher(deadHost(X, cause, sent))
        .invoke(request())
        .catch((e: unknown) => e);

      expect(String(err)).toContain("unable to connect to");
      // Followed once and retried once against a fresh lookup, as without the repair.
      expect(sent).toEqual([X, X]);
      expect(silos.unregistrations).toEqual([]);
      expect(await silos.held()).toEqual(entry);
    });
  }

  it("removes at most a bounded number of entries for one call", async () => {
    const silos = await Silos.start();
    // Every entry it follows names a host that has gone, and each time it is removed another
    // dead one takes its place.
    let current = await silos.register(entryOn(X));
    const sent: SiloAddress[] = [];
    const remote: RemoteInvoker = {
      send: async (silo) => {
        sent.push(silo);
        const replaced = current;
        current = await silos.register(entryOn(X), replaced);
        throw connectFailure(silo, "no listener", "refused");
      },
    };

    const err = await silos
      .dispatcher(remote)
      .invoke(request())
      .catch((e: unknown) => e);

    expect(String(err)).toContain("unable to connect to");
    expect(sent.length).toBeLessThanOrEqual(4);
  });

  it("answers a call it routes to a live host without touching the directory", async () => {
    const silos = await Silos.start();
    const entry = await silos.register(entryOn(X));
    const sent: SiloAddress[] = [];

    expect(await silos.dispatcher(answering(sent)).invoke(request())).toBe("answered by silo-x");
    expect(silos.unregistrations).toEqual([]);
    expect(await silos.held()).toEqual(entry);
  });
});
