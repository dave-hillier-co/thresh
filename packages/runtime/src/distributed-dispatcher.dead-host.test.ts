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
import { RejectionError } from "@thresh/core/errors";
import { mayResend } from "@thresh/messaging/resend-policy";
import { connectFailure, type ConnectFailureCause } from "@thresh/messaging/transport";
import { grain as grainClass } from "@thresh/core/decorators";
import { Grain } from "@thresh/core/grain";
import { getGrainMetadata } from "@thresh/core/grain-metadata";
import { Catalog, type RegisteredGrain } from "@thresh/runtime/catalog";
import { GrainFactory } from "@thresh/runtime/grain-factory";
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

@grainClass()
class DeadHostCounter extends Grain {
  private n = 0;
  async inc(): Promise<number> {
    return ++this.n;
  }
}
const metadata = getGrainMetadata(DeadHostCounter)!;

/** A real catalog for a silo that hosts the counter. */
function hostCatalog(time: FakeTimeProvider): Catalog {
  return new Catalog({
    grainTypes: new Map<string, RegisteredGrain>([
      [metadata.grainType, { ctor: DeadHostCounter, metadata }],
    ]),
    factory: new GrainFactory(() => metadata.grainType, time),
    time,
    defaultCollectionAgeSeconds: 900,
  });
}

/** A grain whose entry `O` owns. */
const grain = ((): GrainId => {
  for (let i = 0; ; i++) {
    const id = new GrainId(metadata.grainType, `k-${i}`);
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
  /** While set, a registration is applied at its owner but its answer waits for this. */
  registerGate: Promise<void> | undefined;
  /** While set, a lookup sent to an owner waits for this before it is answered. */
  lookupGate: Promise<void> | undefined;

  static async start(): Promise<Silos> {
    const silos = new Silos();
    for (const p of silos.partitions.values()) p.applyView(view);
    await flush();
    for (const p of silos.partitions.values()) await p.settled();
    return silos;
  }

  private constructor() {
    for (const silo of [A, O, X]) {
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
        lookup: async (owner, version, id) => {
          await this.lookupGate;
          return this.partition(owner).lookup(version, id);
        },
        register: async (owner, version, a, previous) => {
          const result = await this.partition(owner).register(version, a, previous);
          await this.registerGate;
          return result;
        },
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

  /**
   * `X`'s dispatcher: `X` hosts the counter in a real catalog, may activate only while
   * `mayActivate` says so, and sends whatever it forwards through `remote`.
   */
  host(
    catalog: Catalog,
    mayActivate: () => boolean,
    remote: RemoteInvoker = answering([]),
  ): DistributedDispatcher {
    return new DistributedDispatcher({
      local: X,
      directory: this.directories.get(X.podName)!,
      cache: new LocationCache(),
      catalog,
      remote,
      activeSilos: () => [A, O, Y],
      mayActivate,
      placementFor: () => ({ choose: (_type, candidates) => candidates[0]! }),
      filtersFor: () => [],
      placementContext: () => ({ random: () => 0 }),
      time: this.time,
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

/** The rejection a call failed with, or a failure if it was answered. */
async function refusal(call: Promise<unknown>): Promise<RejectionError> {
  const outcome = await call.then(
    (value) => ({ value }),
    (error: unknown) => ({ error }),
  );
  if (!("error" in outcome)) throw new Error(`answered ${String(outcome.value)}`);
  expect(outcome.error).toBeInstanceOf(RejectionError);
  return outcome.error as RejectionError;
}

const gate = (): { promise: Promise<void>; open: () => void } => {
  let open!: () => void;
  const promise = new Promise<void>((resolve) => (open = resolve));
  return { promise, open };
};

describe("a silo that does not host what the directory says it hosts", () => {
  it("removes an entry naming it for an activation it does not have, outside its ring, and refuses the call", async () => {
    const silos = await Silos.start();
    const stale = await silos.register(entryOn(X));
    const catalog = hostCatalog(silos.time);

    const err = await refusal(silos.host(catalog, () => false).deliverLocal(request()));

    expect(err.kind).toBe("staleView");
    expect(mayResend(err)).toBe(true);
    expect(silos.unregistrations).toEqual([stale]);
    expect(await silos.held()).toBeUndefined();
    expect(catalog.get(grain)).toBeUndefined();
  });

  it("refuses, removing nothing, when the directory has no entry", async () => {
    const silos = await Silos.start();

    const err = await refusal(
      silos.host(hostCatalog(silos.time), () => false).deliverLocal(request()),
    );

    expect(mayResend(err)).toBe(true);
    expect(silos.unregistrations).toEqual([]);
  });

  it("forwards a call whose entry names another silo, under the forward cap", async () => {
    const silos = await Silos.start();
    const elsewhere = await silos.register(entryOn(Y));
    const sent: SiloAddress[] = [];
    const forwarded: InvocationRequest[] = [];
    const remote: RemoteInvoker = {
      send: async (silo, req) => {
        sent.push(silo);
        forwarded.push(req);
        return `answered by ${silo.podName}`;
      },
    };
    const host = silos.host(hostCatalog(silos.time), () => false, remote);

    expect(await host.deliverLocal(request())).toBe("answered by silo-y");
    expect(sent).toEqual([Y]);
    expect(forwarded[0]!.forwardCount).toBe(1);
    expect(await silos.held()).toEqual(elsewhere);

    const capped = await refusal(host.deliverLocal({ ...request(), forwardCount: 2 }));
    expect(mayResend(capped)).toBe(true);
    expect(sent).toEqual([Y]);
  });

  it("refuses without waiting on a lookup the directory does not answer", async () => {
    const silos = await Silos.start();
    await silos.register(entryOn(X));
    const never = gate();
    silos.lookupGate = never.promise;

    const call = refusal(silos.host(hostCatalog(silos.time), () => false).deliverLocal(request()));
    await flush();
    silos.time.advance(60_000);
    const err = await call;

    expect(mayResend(err)).toBe(true);
    expect(silos.unregistrations).toEqual([]);
    never.open();
  });

  for (const inRing of [true, false]) {
    it(`keeps an entry for an activation that is coming up here, ${inRing ? "in" : "outside"} its ring`, async () => {
      const silos = await Silos.start();
      const catalog = hostCatalog(silos.time);
      let mayActivate = true;
      const host = silos.host(catalog, () => mayActivate);
      // A claim whose registration has reached the owner and has not yet been answered.
      const answer = gate();
      silos.registerGate = answer.promise;
      const first = host.deliverLocal(request());
      await flush();
      const claimed = await silos.held();
      expect(claimed?.silo).toEqual(X);
      expect(catalog.get(grain)).toBeUndefined();

      // A second call arrives meanwhile, with the silo in its ring or out of it.
      mayActivate = inRing;
      const second = host.deliverLocal(request());
      await flush();
      silos.registerGate = undefined;
      answer.open();

      expect([await first, await second].sort()).toEqual([1, 2]);
      expect(silos.unregistrations).toEqual([]);
      expect(await silos.held()).toEqual(claimed);
      expect(catalog.get(grain)?.activationId).toBe(claimed!.activationId);
    });

    it(`keeps an entry for a migrated activation being rehydrated here, ${inRing ? "in" : "outside"} its ring`, async () => {
      const silos = await Silos.start();
      const catalog = hostCatalog(silos.time);
      const source = await silos.register(entryOn(O));
      // The migration landed: its activation is in the catalog, rehydrating, and its
      // registration replaced the source's entry.
      const migrated = await catalog.activateMigrated(grain, newActivationId(), {}, source);
      const moved = await silos.register(
        { grainId: grain, silo: X, activationId: migrated.activationId },
        source,
      );
      expect(moved.silo).toEqual(X);

      const answer = await silos.host(catalog, () => inRing).deliverLocal(request());

      expect(answer).toBe(1);
      expect(silos.unregistrations).toEqual([]);
      expect(await silos.held()).toEqual(moved);
      expect(catalog.get(grain)).toBe(migrated);
    });
  }
});
