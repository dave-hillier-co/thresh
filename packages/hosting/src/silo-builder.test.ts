import { randomUUID } from "node:crypto";
import { createServer } from "node:net";
import { afterAll, describe, expect, it } from "vitest";
import { Pool } from "pg";
import { createClient } from "redis";
import { grain } from "@thresh/core/decorators";
import { Grain } from "@thresh/core/grain";
import { GrainId } from "@thresh/core/grain-id";
import { defineGrainInterface } from "@thresh/core/grain-interface";
import type { GrainKey, GrainWithStringKey } from "@thresh/core/key-kinds";
import { SiloAddress } from "@thresh/core/silo-address";
import { ISiloProbe } from "@thresh/core/silo-probe-grain";
import { FakeTimeProvider } from "@thresh/core/test-support/fake-time-provider";
import { InProcessNetwork } from "@thresh/messaging/in-process-transport";
import { MemoryGrainStorage } from "@thresh/persistence/memory-grain-storage";
import { PostgresGrainStorage } from "@thresh/persistence/postgres-grain-storage";
import { serializeValue } from "@thresh/core/value-codec";
import { constructGrain } from "@thresh/runtime/construct-grain";
import { createSilo, type Registration } from "@thresh/hosting/silo-builder";

interface ICounter extends GrainKey<string> {
  increment(by: number): Promise<number>;
}
const ICounter = defineGrainInterface<ICounter>("ICounter.hosting");

@grain()
class CounterGrain extends Grain implements ICounter {
  private count = 0;
  async increment(by: number): Promise<number> {
    this.count += by;
    return this.count;
  }
}

interface IBlocker extends GrainKey<string> {
  block(): Promise<string>;
}
const IBlocker = defineGrainInterface<IBlocker>("IBlocker.hosting");

/** Module-scoped so the test can hold a call open until it decides to release it. */
let blockerGate: { resolve: () => void; promise: Promise<void> };
function resetBlockerGate(): void {
  let resolve!: () => void;
  const promise = new Promise<void>((res) => {
    resolve = res;
  });
  blockerGate = { resolve, promise };
}

@grain()
class BlockerGrain extends Grain implements IBlocker {
  async block(): Promise<string> {
    await blockerGate.promise;
    return "done";
  }
}

const local = new SiloAddress("silo-0", "uid-0", "silo-0:11111");

function buildHost() {
  return createSilo({ clusterId: "c1", local })
    .useStaticMembership([local])
    .useInProcessTransport(new InProcessNetwork())
    .useMessagePackSerialization()
    .registerGrain(CounterGrain, { interfaces: [ICounter] })
    .build();
}

describe("createSilo / SiloHost", () => {
  it("is not ready until started, ready after, and serves grain calls", async () => {
    const host = buildHost();
    expect(host.health.ready().ok).toBe(false);

    await host.start();
    try {
      expect(host.health.ready().ok).toBe(true);
      expect(await host.getGrain(ICounter, "x").increment(5)).toBe(5);
      expect(host.isActive(new GrainId("Counter", "x"))).toBe(true);
    } finally {
      await host.stop();
    }
  });

  it("flips readiness off when drained", async () => {
    const host = buildHost();
    await host.start();
    expect(host.health.ready().ok).toBe(true);
    await host.stop();
    expect(host.health.ready().ok).toBe(false);
  });

  it("refuses to build without membership or transport", () => {
    expect(() => createSilo({ clusterId: "c1", local }).build()).toThrow(/membership/);
    expect(() =>
      createSilo({ clusterId: "c1", local }).useStaticMembership([local]).build(),
    ).toThrow(/transport/);
  });
});

describe("createSilo scheduler back-pressure (Orleans SchedulingOptions parity)", () => {
  it("rejects a call once a configured maxEnqueuedRequestsHardLimit is exceeded", async () => {
    resetBlockerGate();
    const host = createSilo({
      clusterId: "c1",
      local,
      scheduling: { maxEnqueuedRequestsHardLimit: 1 },
    })
      .useStaticMembership([local])
      .useInProcessTransport(new InProcessNetwork())
      .useMessagePackSerialization()
      .registerGrain(BlockerGrain, { interfaces: [IBlocker] })
      .build();
    await host.start();
    try {
      const blocker = host.getGrain(IBlocker, "x");
      // Fire several concurrent calls at the same activation: with the queue
      // capped at 1, at least one must be rejected as over the hard limit
      // rather than growing the queue without bound; whichever are admitted
      // still complete once the gate opens.
      const calls = Array.from({ length: 5 }, () => blocker.block());
      blockerGate.resolve();
      const results = await Promise.allSettled(calls);
      expect(results.some((r) => r.status === "rejected")).toBe(true);
      expect(results.some((r) => r.status === "fulfilled" && r.value === "done")).toBe(true);
    } finally {
      await host.stop();
    }
  });
});

// ---------------------------------------------------------------------------
// Registration typing (issue #58)
// ---------------------------------------------------------------------------

interface IGreeter extends GrainWithStringKey {
  greet(): Promise<string>;
}
const IGreeter = defineGrainInterface<IGreeter>("IGreeter.hosting");

/**
 * The normal shape of a grain once there is no DI container: the constructor
 * takes an options bag, and the silo's `GrainActivator` is the seam that hands
 * it one. Registering this must type-check without a cast.
 */
@grain()
class GreeterGrain extends Grain implements IGreeter {
  constructor(private readonly options: { greeting: string }) {
    super();
  }

  async greet(): Promise<string> {
    return `${this.options.greeting} from ${this.id.key}`;
  }
}

/**
 * A consumer's own shared registration list, named by the exported type rather
 * than restated structurally, and `readonly` so it can be a module constant
 * without callers having to copy it.
 */
const HOSTED_GRAINS: readonly Registration[] = [
  { ctor: GreeterGrain, interfaces: [IGreeter] },
  { ctor: CounterGrain, interfaces: [ICounter] },
];

describe("SiloBuilder grain registration typing", () => {
  it("registers a grain whose constructor takes an options bag, built by the activator", async () => {
    const built: string[] = [];
    const host = createSilo({ clusterId: "c1", local })
      .useStaticMembership([local])
      .useInProcessTransport(new InProcessNetwork())
      .useMessagePackSerialization()
      .registerGrains(HOSTED_GRAINS)
      .useGrainActivator({
        createInstance: (ctor) => {
          built.push(ctor.name);
          // No cast at the call site: the activator is handed the registered
          // class, and constructs the ones it knows about itself.
          if (ctor === GreeterGrain) return new GreeterGrain({ greeting: "hello" });
          return constructGrain(ctor);
        },
      })
      .build();

    await host.start();
    try {
      expect(await host.getGrain(IGreeter, "x").greet()).toBe("hello from x");
      // Every other grain type on the silo still falls through to `new ctor()`.
      expect(await host.getGrain(ICounter, "y").increment(3)).toBe(3);
    } finally {
      await host.stop();
    }
    expect(built).toContain("GreeterGrain");
    expect(built).toContain("CounterGrain");
  });
});

// ---------------------------------------------------------------------------
// The observer seam and the transport that can back it (issue #55)
// ---------------------------------------------------------------------------

interface IObserverSeam {
  notify(value: string): Promise<void>;
}
const IObserverSeam = defineGrainInterface<IObserverSeam>("IObserverSeam.hosting");

describe("SiloBuilder observer hosting (createObjectReference from a startup task)", () => {
  it("hosts an observer from a startup task when the seam is declared and backed", async () => {
    let reference: IObserverSeam | undefined;
    const host = createSilo({ clusterId: "c1", local })
      .useStaticMembership([local])
      .useInProcessTransport(new InProcessNetwork())
      .requireObserverHosting()
      .registerGrain(CounterGrain, { interfaces: [ICounter] })
      .addStartupTask(async (grains) => {
        reference = grains.createObjectReference(IObserverSeam, {
          notify: async () => undefined,
        });
        grains.deleteObjectReference(reference);
      })
      .build();

    await host.start();
    try {
      expect(reference).toBeDefined();
    } finally {
      await host.stop();
    }
  });

  it("builds a WebSocket silo that declares the observer seam", () => {
    // This used to throw: the embedded client leg's gate accepted only the
    // two transports the builder itself knew how to give a listening leg to.
    // Issue #65 removed the client leg's listener entirely — the embedded
    // client dials the silo, and dialling needs no listener — so there is
    // nothing left to gate on, and `requireObserverHosting()` never rejects.
    // The push path itself is proved over real sockets further down.
    expect(() =>
      createSilo({ clusterId: "c1", local })
        .useStaticMembership([local])
        .useWebSocketTransport()
        .requireObserverHosting()
        .addStartupTask(async (grains) => {
          grains.createObjectReference(IObserverSeam, { notify: async () => undefined });
        })
        .build(),
    ).not.toThrow();
  });

  it("still builds a WebSocket silo whose startup tasks never host an observer", () => {
    // The gate is the explicit declaration, not the mere presence of a startup
    // task: a silo that registers startup tasks for any other reason (BeneDB's
    // `addSpiceportGrainServices` registers one unconditionally) must keep
    // building on a WebSocket transport, as the examples in this repo do.
    expect(() =>
      createSilo({ clusterId: "c1", local })
        .useStaticMembership([local])
        .useWebSocketTransport()
        .registerGrain(CounterGrain, { interfaces: [ICounter] })
        .addStartupTask(async (grains) => {
          await grains.getGrain(ICounter, "x").increment(1);
        })
        .build(),
    ).not.toThrow();
  });
});

/** Ask the OS for a free TCP port so a WebSocket-hosted silo doesn't collide on one. */
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

interface INotifier extends GrainWithStringKey {
  subscribe(observer: IObserverSeam): Promise<void>;
  fire(value: string): Promise<void>;
}
const INotifier = defineGrainInterface<INotifier>("INotifier.hosting");

@grain()
class NotifierGrain extends Grain implements INotifier {
  private observer: IObserverSeam | undefined;
  async subscribe(observer: IObserverSeam): Promise<void> {
    this.observer = observer;
  }
  async fire(value: string): Promise<void> {
    await this.observer?.notify(value);
  }
}

describe("SiloBuilder observer hosting over real WebSocket sockets (issue #55)", () => {
  it("pushes to an observer a startup task hosted on a WebSocket-transport silo", async () => {
    // A TestCluster cannot see this: it always configures an in-process
    // transport, so the wire-level mechanism silo-builder.test.ts's own
    // in-process tests exercise never runs over real sockets. This proves the
    // same behaviour #55 pinned survives #65's duplex rewrite: the silo pushes
    // to the observer down the very socket the embedded client dialled to
    // reach it, never by dialling the client back.
    const port = await freePort();
    const address = new SiloAddress("silo-ws", "uid-ws", `127.0.0.1:${port}`);
    const received: string[] = [];

    const host = createSilo({ clusterId: "c-ws-observer", local: address })
      .useStaticMembership([address])
      .useWebSocketTransport()
      .requireObserverHosting()
      .registerGrain(NotifierGrain, { interfaces: [INotifier] })
      .addStartupTask(async (grains) => {
        const observer = grains.createObjectReference(IObserverSeam, {
          notify: async (value: string) => {
            received.push(value);
          },
        });
        await grains.getGrain(INotifier, "n").subscribe(observer);
        await grains.getGrain(INotifier, "n").fire("over-sockets");
      })
      .build();

    await host.start();
    try {
      expect(received).toEqual(["over-sockets"]);
    } finally {
      await host.stop();
    }
  }, 20_000);
});

describe("SiloBuilder identity read-back", () => {
  it("exposes the address this silo was configured with", () => {
    const local = new SiloAddress("s1", "uid-s1", "s1:11111");

    const builder = createSilo({ clusterId: "identity-readback", local });

    // A single-silo dev host configures its own membership view, and the only address it can name
    // is its own — which it would otherwise have to be told a second time.
    expect(builder.local).toBe(local);
    expect(builder.clusterId).toBe("identity-readback");
  });
});

describe("SiloBuilder storage read-back", () => {
  const config = () => ({
    clusterId: "storage-readback",
    local: new SiloAddress("s1", "uid-s1", "s1:11111"),
  });

  it("returns the provider registered under a name", () => {
    const provider = new MemoryGrainStorage();
    const builder = createSilo(config()).addStorage("datastore", provider);

    expect(builder.storageProvider("datastore")).toBe(provider);
  });

  it("returns undefined for a name nothing was registered under", () => {
    const builder = createSilo(config()).addStorage("datastore", new MemoryGrainStorage());

    expect(builder.storageProvider("other")).toBeUndefined();
  });

  it("returns undefined when no storage is configured at all", () => {
    expect(createSilo(config()).storageProvider("datastore")).toBeUndefined();
  });

  it("reads back the provider a convenience registration constructed", () => {
    // The Postgres helper builds the provider (and its pool) itself, so reading it back is the
    // only way a host can hand the SAME instance to something outside the facet machinery — a
    // grain that takes its storage by constructor injection, say. No connection is opened until
    // the silo starts, so this stays a pure builder assertion.
    const builder = createSilo(config()).addPostgresStorage("datastore", {
      connectionString: "postgres://localhost/thresh-readback",
    });

    expect(builder.storageProvider("datastore")).toBeInstanceOf(PostgresGrainStorage);
  });
});

// ---------------------------------------------------------------------------
// Service identity reaches the storage providers the builder constructs (#59)
// ---------------------------------------------------------------------------

const PG_URL = process.env.PG_URL ?? "postgres://localhost:5432/postgres";

/** Probe Postgres once at load time so the suite skips cleanly without it. */
async function postgresReachable(connectionString: string): Promise<Pool | undefined> {
  const probe = new Pool({ connectionString });
  probe.on("error", () => {});
  try {
    await probe.query("SELECT 1");
    return probe;
  } catch {
    await probe.end().catch(() => {});
    return undefined;
  }
}

const sharedPool = await postgresReachable(PG_URL);

describe.skipIf(sharedPool === undefined)(
  "SiloBuilder service identity in storage (issue #59)",
  () => {
    it("keeps two clusters sharing one Postgres table on separate rows", async () => {
      const table = `thresh_hosting_${randomUUID().replace(/-/g, "")}`;
      const build = (serviceId: string, podName: string) => {
        const address = new SiloAddress(podName, `uid-${podName}`, `${podName}:11111`);
        return createSilo({ clusterId: "shared-deployment", serviceId, local: address })
          .useStaticMembership([address])
          .useInProcessTransport(new InProcessNetwork())
          .addPostgresStorage("datastore", { connectionString: PG_URL, tableName: table });
      };

      const alphaBuilder = build("alpha", "silo-alpha");
      const betaBuilder = build("beta", "silo-beta");
      const alphaHost = alphaBuilder.build();
      const betaHost = betaBuilder.build();
      await alphaHost.start();
      await betaHost.start();
      try {
        const alpha = alphaBuilder.storageProvider("datastore")!;
        const beta = betaBuilder.storageProvider("datastore")!;
        const id = new GrainId("Datastore", "0");

        const a = { value: { cents: 1 }, etag: undefined as string | undefined, exists: false };
        await alpha.write("balance", id, a);

        // The other cluster's silo must see nothing under the same grain and
        // state name, and its own blind write must not collide with alpha's row.
        const b = { value: { cents: 0 }, etag: undefined as string | undefined, exists: false };
        await beta.read("balance", id, b);
        expect(b.exists).toBe(false);
        b.value = { cents: 2 };
        await beta.write("balance", id, b);

        const reread = {
          value: { cents: 0 },
          etag: undefined as string | undefined,
          exists: false,
        };
        await alpha.read("balance", id, reread);
        expect(reread.value.cents).toBe(1);
      } finally {
        await alphaHost.stop();
        await betaHost.stop();
        await sharedPool!.query(`DROP TABLE IF EXISTS ${table}`);
      }
    }, 20_000);

    // The case a real deployment actually hits: a silo configured with only a
    // `clusterId`, over a table that predates the service column. The migration
    // backfills existing rows to DEFAULT_SERVICE_ID, so the builder's default
    // MUST be that same literal — a default of `clusterId` would stamp the rows
    // "default" and then read them back under the cluster id, matching nothing:
    // the grain activates empty and its next write orphans the original row.
    it("reads rows migrated from the pre-service_id schema on a silo with no serviceId", async () => {
      const legacy = `thresh_hosting_legacy_${randomUUID().replace(/-/g, "")}`;
      await sharedPool!.query(
        `CREATE TABLE ${legacy} (
           grain_id text NOT NULL,
           state_name text NOT NULL,
           data text NOT NULL,
           etag text NOT NULL,
           PRIMARY KEY (grain_id, state_name)
         )`,
      );
      const id = new GrainId("Datastore", "0");
      await sharedPool!.query(
        `INSERT INTO ${legacy} (grain_id, state_name, data, etag) VALUES ($1, $2, $3, $4)`,
        [id.toString(), "balance", serializeValue({ cents: 77 }), "legacy-etag"],
      );

      const address = new SiloAddress("silo-legacy", "uid-legacy", "silo-legacy:11111");
      const builder = createSilo({ clusterId: "prod", local: address })
        .useStaticMembership([address])
        .useInProcessTransport(new InProcessNetwork())
        .addPostgresStorage("datastore", { connectionString: PG_URL, tableName: legacy });
      const host = builder.build();
      await host.start();
      try {
        const storage = builder.storageProvider("datastore")!;
        const state = { value: { cents: 0 }, etag: undefined as string | undefined, exists: false };
        await storage.read("balance", id, state);
        expect(state.exists).toBe(true);
        expect(state.value.cents).toBe(77);
      } finally {
        await host.stop();
        await sharedPool!.query(`DROP TABLE IF EXISTS ${legacy}`);
      }
    }, 20_000);

    // Orleans keeps ServiceId independent of ClusterId precisely so a
    // redeployment that changes the cluster id keeps its state
    // (ClusterOptions.cs:36). Two silos differing ONLY in clusterId therefore
    // share one service namespace and SEE each other's rows. This asserts the
    // fallback itself: it fails against `serviceId ?? clusterId`, which would
    // partition them and strand the first deployment's state.
    it("keeps one service namespace across a clusterId change when no serviceId is set", async () => {
      const table = `thresh_hosting_${randomUUID().replace(/-/g, "")}`;
      const build = (clusterId: string, podName: string) => {
        const address = new SiloAddress(podName, `uid-${podName}`, `${podName}:11111`);
        return createSilo({ clusterId, local: address })
          .useStaticMembership([address])
          .useInProcessTransport(new InProcessNetwork())
          .addPostgresStorage("datastore", { connectionString: PG_URL, tableName: table });
      };

      const beforeBuilder = build("prod-1", "silo-before");
      const afterBuilder = build("prod-2", "silo-after");
      const beforeHost = beforeBuilder.build();
      const afterHost = afterBuilder.build();
      await beforeHost.start();
      await afterHost.start();
      try {
        const id = new GrainId("Datastore", "0");
        const written = {
          value: { cents: 5 },
          etag: undefined as string | undefined,
          exists: false,
        };
        await beforeBuilder.storageProvider("datastore")!.write("balance", id, written);

        const reread = {
          value: { cents: 0 },
          etag: undefined as string | undefined,
          exists: false,
        };
        await afterBuilder.storageProvider("datastore")!.read("balance", id, reread);
        expect(reread.exists).toBe(true);
        expect(reread.value.cents).toBe(5);
      } finally {
        await beforeHost.stop();
        await afterHost.stop();
        await sharedPool!.query(`DROP TABLE IF EXISTS ${table}`);
      }
    }, 20_000);
  },
);

// ---------------------------------------------------------------------------
// The Redis half of the same wiring (#59). The Postgres cases above cannot see
// it: `addRedisStorage` threads `serviceId` through a separate call site, and
// dropping it there leaves the whole suite green.
// ---------------------------------------------------------------------------

const REDIS_URL = process.env.REDIS_URL ?? "redis://localhost:6379";

/**
 * Probe Redis once at load time so the suite skips cleanly without it.
 *
 * `reconnectStrategy: false` is load-bearing, not tidiness — the same trap
 * `redis-grain-storage.test.ts` documents: node-redis retries a refused connection FOREVER by
 * default, so `connect()` never settles when nothing is listening and this module-level await
 * hangs the entire file rather than skipping it.
 */
async function redisReachable(url: string): Promise<boolean> {
  const probe = createClient({ url, socket: { reconnectStrategy: false, connectTimeout: 500 } });
  probe.on("error", () => {});
  try {
    await probe.connect();
    await probe.ping();
    await probe.destroy();
    return true;
  } catch {
    try {
      await probe.destroy();
    } catch {
      /* never connected */
    }
    return false;
  }
}

const redisUp = await redisReachable(REDIS_URL);

describe.skipIf(!redisUp)("SiloBuilder service identity in Redis storage (issue #59)", () => {
  it("keeps two services sharing one Redis prefix on separate keys", async () => {
    const prefix = `thresh_hosting_${randomUUID().replace(/-/g, "")}`;
    const build = (serviceId: string, podName: string) => {
      const address = new SiloAddress(podName, `uid-${podName}`, `${podName}:11111`);
      return createSilo({ clusterId: "shared-deployment", serviceId, local: address })
        .useStaticMembership([address])
        .useInProcessTransport(new InProcessNetwork())
        .addRedisStorage("datastore", { url: REDIS_URL, keyPrefix: prefix });
    };

    const alphaBuilder = build("alpha", "silo-r-alpha");
    const betaBuilder = build("beta", "silo-r-beta");
    const alphaHost = alphaBuilder.build();
    const betaHost = betaBuilder.build();
    await alphaHost.start();
    await betaHost.start();
    try {
      const id = new GrainId("Datastore", "0");
      const a = { value: { cents: 1 }, etag: undefined as string | undefined, exists: false };
      await alphaBuilder.storageProvider("datastore")!.write("balance", id, a);

      const b = { value: { cents: 0 }, etag: undefined as string | undefined, exists: false };
      await betaBuilder.storageProvider("datastore")!.read("balance", id, b);
      expect(b.exists).toBe(false);
    } finally {
      await alphaHost.stop();
      await betaHost.stop();
      const cleanup = createClient({ url: REDIS_URL });
      cleanup.on("error", () => {});
      await cleanup.connect();
      // scanIterator yields a BATCH per iteration, not a single key.
      for await (const batch of cleanup.scanIterator({ MATCH: `${prefix}:*` })) {
        const keys = Array.isArray(batch) ? batch : [batch];
        if (keys.length > 0) await cleanup.del(keys);
      }
      await cleanup.quit();
    }
  }, 20_000);
});

// ---------------------------------------------------------------------------
// Self-probe liveness (docs/design-notes-parity-gaps.md item 9, option A)
// ---------------------------------------------------------------------------

/** Module-scoped so the wedged probe grain's single turn can be released before `host.stop()`. */
let probeGate: { resolve: () => void; promise: Promise<void> };
function resetProbeGate(): void {
  let resolve!: () => void;
  const promise = new Promise<void>((res) => {
    resolve = res;
  });
  probeGate = { resolve, promise };
}

/**
 * Overrides the built-in `ISiloProbe` activation with one whose `ping()`
 * never resolves on its own — the closest a single-process test harness can
 * get to a genuinely deadlocked turn scheduler: a REAL activation, REAL
 * `preferLocal` placement, REAL dispatch through the catalog and turn
 * scheduler, whose one exclusive turn simply never settles until the test
 * releases `probeGate`. Every self-probe call queued behind it while wedged
 * sits admitted-never turns in the same activation's queue rather than being
 * answered — exactly the failure mode `SelfProbeWorker` exists to catch.
 */
@grain()
class WedgedProbeGrain extends Grain implements ISiloProbe {
  async ping(): Promise<void> {
    await probeGate.promise;
  }
}

const flush = (): Promise<void> => new Promise((r) => setTimeout(r, 0));

describe("SiloBuilder self-probe liveness", () => {
  it("keeps readiness true while the real ISiloProbe grain answers (real placement, real dispatch)", async () => {
    const time = new FakeTimeProvider();
    const probeLocal = new SiloAddress("silo-probe-a", "uid-probe-a", "silo-probe-a:1");
    const host = createSilo({
      clusterId: "c-self-probe-a",
      local: probeLocal,
      time,
      selfProbe: { intervalMs: 10, timeoutMs: 5, missedThreshold: 2 },
    })
      .useStaticMembership([probeLocal])
      .useInProcessTransport(new InProcessNetwork())
      .useMessagePackSerialization()
      .build();
    await host.start();
    try {
      expect(host.health.ready().ok).toBe(true);
      // A few real probe cycles against the built-in ISiloProbe grain — no
      // seam is injected here at all.
      for (let i = 0; i < 3; i += 1) {
        time.advance(10);
        await flush();
      }
      expect(host.health.ready().ok).toBe(true);
      expect(host.health.ready().checks.dispatcherResponsive).toBe(true);
    } finally {
      await host.stop();
    }
  });

  it("flips readiness false when the self-probe grain's own activation is wedged", async () => {
    resetProbeGate();
    const time = new FakeTimeProvider();
    const probeLocal = new SiloAddress("silo-probe-b", "uid-probe-b", "silo-probe-b:1");
    const host = createSilo({
      clusterId: "c-self-probe-b",
      local: probeLocal,
      time,
      selfProbe: { intervalMs: 10, timeoutMs: 5, missedThreshold: 2 },
    })
      .useStaticMembership([probeLocal])
      .useInProcessTransport(new InProcessNetwork())
      .useMessagePackSerialization()
      // Swaps the built-in ISiloProbe activation for the wedged one above —
      // registered after the constructor's own built-in registration, so
      // this interface now resolves to WedgedProbeGrain's grain type.
      .registerGrain(WedgedProbeGrain, { interfaces: [ISiloProbe] })
      .build();
    await host.start();
    try {
      expect(host.health.ready().ok).toBe(true);
      // Two probe cycles, each interval followed by its own timeout — the
      // wedged grain never answers within `timeoutMs`, so both are misses.
      for (let i = 0; i < 2; i += 1) {
        time.advance(10);
        await flush();
        time.advance(5);
        await flush();
      }
      expect(host.health.ready().ok).toBe(false);
      expect(host.health.ready().checks.dispatcherResponsive).toBe(false);
    } finally {
      // Release the wedged turn before stopping: `node.stop()` deactivates
      // every activation, which would otherwise wait forever behind this
      // one's still-running exclusive turn.
      probeGate.resolve();
      await flush();
      await host.stop();
    }
  });
});

afterAll(async () => {
  await sharedPool?.end();
});
