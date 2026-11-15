import { afterEach, describe, expect, it } from "vitest";
import { defineGrain, useOnDeactivate } from "@thresh/core/define-grain";
import { RejectionError } from "@thresh/core/errors";
import { mayResend } from "@thresh/messaging/resend-policy";
import { GrainId } from "@thresh/core/grain-id";
import { castGrainReference } from "@thresh/core/grain-reference";
import type { ReminderEntry } from "@thresh/core/reminder";
import { SiloAddress } from "@thresh/core/silo-address";
import {
  FakeEndpointSliceApi,
  type FakeEndpointSliceSource,
  type FakePod,
} from "@thresh/clustering-k8s/fake-endpoint-slice-api";
import { KubernetesEndpointWatch } from "@thresh/clustering-k8s/kubernetes-endpoint-watch";
import { InProcessNetwork, InProcessTransport } from "@thresh/messaging/in-process-transport";
import type {
  Connection,
  ConnectionPreamble,
  MessageHandler,
  Transport,
} from "@thresh/messaging/transport";
import { SILO_PROBE_GRAIN_TYPE } from "@thresh/runtime/silo-probe-grain";
import { MemoryReminderTable } from "@thresh/reminders/memory-reminder-table";
import { IGrainManagementExtension } from "@thresh/runtime/grain-management-extension";
import { createSilo, type SiloBuilder, type SiloConfig } from "@thresh/hosting/silo-builder";
import type { SiloHost } from "@thresh/hosting/silo-host";
import { createClient } from "@thresh/client/client-node";
import type { GrainAddress } from "@thresh/core/grain-address";
import type { LocationCache } from "@thresh/directory/location-cache";
import { FakeTimeProvider } from "@thresh/core/test-support/fake-time-provider";
import { PendingTimerTracker } from "@thresh/core/test-support/pending-timer-tracker";

/**
 * The production membership path, composed end to end: `createSilo(...)
 * .useKubernetesMembership(new KubernetesEndpointWatch(source))` over a fake of the
 * Kubernetes API (the true boundary), with the in-process transport. Nothing here
 * shares a membership object between silos: each silo watches the fake API server
 * for itself, as a pod does.
 */

const GRAIN_TYPE = "K8sViewCounter";
const Counter = defineGrain(GRAIN_TYPE, () => {
  let n = 0;
  return { inc: async (): Promise<number> => ++n };
});
const grainId = (key: string): GrainId => new GrainId(GRAIN_TYPE, key);

/** Every deactivation hook run by a `Hooked` activation, by grain key. */
const hookRuns: string[] = [];
const HOOKED_TYPE = "K8sViewHooked";
const Hooked = defineGrain(HOOKED_TYPE, (ctx) => {
  let n = 0;
  useOnDeactivate(() => {
    hookRuns.push(ctx.id.toString());
  });
  return { inc: async (): Promise<number> => ++n };
});
const hookedId = (key: string): GrainId => new GrainId(HOOKED_TYPE, key);

/**
 * A stateless worker that answers with the name of the silo serving it; `workerActivations`
 * counts the activations each silo created, by silo name.
 */
const workerActivations = new Map<string, number>();
const Worker = defineGrain(
  "K8sViewWorker",
  (ctx) => {
    const silo = ctx.runtime.localSiloAddress().podName;
    workerActivations.set(silo, (workerActivations.get(silo) ?? 0) + 1);
    return { where: async (): Promise<string> => silo };
  },
  { stateless: true, maxLocalWorkers: 4 },
);

/** The node internals these tests read (introspection only). */
interface NodeFacet {
  readonly appliedViewVersion: number;
  ownedHashRanges(): Array<[number, number]>;
  locationCacheEntriesFor(silo: SiloAddress): number;
}
const nodeOf = (host: SiloHost): NodeFacet =>
  (host as unknown as { parts: { node: NodeFacet } }).parts.node;
const membershipVersionOf = (host: SiloHost): number =>
  (
    host as unknown as { parts: { membership: { current(): { version: number } } } }
  ).parts.membership.current().version;

/** A reminder table whose range reads (the reminder service's ownership hook) can be held. */
class HoldableReminderTable extends MemoryReminderTable {
  readonly reads: Array<[number, number]> = [];
  private gate: Promise<void> | undefined;
  private open: (() => void) | undefined;

  hold(): void {
    this.gate = new Promise((resolve) => (this.open = resolve));
  }

  release(): void {
    this.open?.();
    this.gate = undefined;
  }

  override async readRange(hashBegin: number, hashEnd: number): Promise<ReminderEntry[]> {
    if (this.gate !== undefined) await this.gate;
    this.reads.push([hashBegin, hashEnd]);
    return super.readRange(hashBegin, hashEnd);
  }
}

interface K8sSilo {
  readonly pod: FakePod;
  readonly address: SiloAddress;
  readonly host: SiloHost;
  readonly source: FakeEndpointSliceSource;
}

class Cluster {
  readonly api = new FakeEndpointSliceApi();
  readonly network = new InProcessNetwork();
  private readonly silos: K8sSilo[] = [];

  /**
   * A pod whose endpoint is published not-ready (unless `listed: false`, which leaves the
   * endpoint for the test to publish), then a silo for it (not yet started).
   */
  silo(
    pod: Omit<FakePod, "ready">,
    configure?: (builder: SiloBuilder) => void,
    options: {
      listed?: boolean;
      selfProbe?: SiloConfig["selfProbe"];
      config?: Partial<SiloConfig>;
    } = {},
  ): K8sSilo {
    const full: FakePod = { ...pod, ready: false };
    if (options.listed !== false) this.api.setPod(full);
    const source = this.api.source();
    const address = new SiloAddress(pod.name, pod.uid, this.api.endpointOf(pod));
    const builder = createSilo({
      clusterId: "k8s-view",
      local: address,
      selfProbe: options.selfProbe ?? { enabled: false },
      callTimeout: { seconds: 2 },
      gracefulShutdownMs: 0,
      ...options.config,
    })
      .useKubernetesMembership(new KubernetesEndpointWatch(source), { portName: "silo" })
      .useInProcessTransport(this.network)
      .disableTransactions()
      .registerGrain(Counter)
      .registerGrain(Hooked);
    configure?.(builder);
    const silo = { pod: full, address, host: builder.build(), source };
    this.silos.push(silo);
    return silo;
  }

  /** Start the silo, then flip its endpoint ready, the way a pod's readiness probe does. */
  async join(silo: K8sSilo): Promise<void> {
    await silo.host.start();
    this.api.setReady(silo.pod.name, true);
    await settle();
  }

  async stopAll(): Promise<void> {
    for (const silo of this.silos.splice(0).reverse()) {
      await silo.host.stop().catch(() => undefined);
    }
  }
}

/** Let pending membership reconciliation, directory traffic and recovery pulls run. */
async function settle(rounds = 5): Promise<void> {
  for (let i = 0; i < rounds; i++) await new Promise((resolve) => setTimeout(resolve, 0));
}

/** Resolve the instant `predicate` holds, yielding a microtask at a time (timers as a fallback). */
async function until(predicate: () => boolean, label: string, timeoutMs = 2_000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  for (let i = 0; !predicate(); i++) {
    if (Date.now() > deadline) throw new Error(`timed out waiting for ${label}`);
    if (i < 10_000) await Promise.resolve();
    else await new Promise((resolve) => setTimeout(resolve, 0));
  }
}

const clusters: Cluster[] = [];
function cluster(): Cluster {
  const c = new Cluster();
  clusters.push(c);
  return c;
}
afterEach(async () => {
  for (const c of clusters.splice(0)) await c.stopAll();
});

let uidSeq = 0;
const uid = (): string => `uid-${++uidSeq}-${Math.random().toString(36).slice(2, 8)}`;

describe("a silo on Kubernetes membership", () => {
  it("owns ranges and serves a grain call after start, with no further membership event", async () => {
    const c = cluster();
    const a = c.silo({ name: "silo-a", uid: uid(), ip: "10.0.0.1" });

    // Sample readiness on every microtask while the silo starts: whenever it reports
    // ready, the node must already own ranges (an empty ring rejects every route as
    // staleView).
    let done = false;
    const violations: string[] = [];
    const sampler = (async () => {
      for (let i = 0; !done && i < 1_000_000; i++) {
        if (a.host.health.ready().ok && nodeOf(a.host).ownedHashRanges().length === 0) {
          violations.push("ready while owning no ranges");
        }
        if (i % 1000 === 999) await new Promise((resolve) => setTimeout(resolve, 0));
        else await Promise.resolve();
      }
    })();
    await a.host.start();
    done = true;
    await sampler;

    expect(nodeOf(a.host).ownedHashRanges().length).toBeGreaterThan(0);
    expect(a.host.health.ready().ok).toBe(true);
    expect(violations).toEqual([]);
    await expect(a.host.getGrain(Counter, "first").inc()).resolves.toBe(1);
  });

  it("lets a startup task call a grain", async () => {
    const c = cluster();
    let result: number | string = "not run";
    const a = c.silo({ name: "silo-a", uid: uid(), ip: "10.0.0.1" }, (builder) =>
      builder.addStartupTask(async (grains) => {
        result = await grains
          .getGrain(Counter, "from-startup")
          .inc()
          .catch((e: unknown) => `failed: ${String(e)}`);
      }),
    );
    await a.host.start();
    expect(result).toBe(1);
  });

  it("reports ready once its watch has listed it, before it is in any ring", async () => {
    const c = cluster();
    const a = c.silo({ name: "silo-a", uid: uid(), ip: "10.0.0.1" });
    await c.join(a);
    const b = c.silo({ name: "silo-b", uid: uid(), ip: "10.0.0.2" });
    expect(b.host.health.ready().checks.membershipHealthy).toBe(false);
    await b.host.start();
    // Joining: listed, not ready in the slices, outside every ring — and ready to serve.
    expect(b.host.health.ready().ok).toBe(true);
    expect(nodeOf(b.host).ownedHashRanges()).toEqual([]);
    expect(nodeOf(a.host).ownedHashRanges().length).toBeGreaterThan(0);
  });

  it("is not ready while its watch does not list it, and is once it does", async () => {
    const c = cluster();
    const a = c.silo({ name: "silo-a", uid: uid(), ip: "10.0.0.1" });
    await c.join(a);
    const b = c.silo({ name: "silo-b", uid: uid(), ip: "10.0.0.2" }, undefined, { listed: false });
    const started = b.host.start();
    await settle();
    // Start waits for the pod to be listed: it is neither ready nor fenced meanwhile.
    expect(b.host.health.ready().ok).toBe(false);
    expect(b.host.health.live().ok).toBe(true);

    c.api.setPod(b.pod);
    await started;
    expect(b.host.health.ready().ok).toBe(true);
  });
});

describe("a silo whose pod is not listed when it starts", () => {
  it("waits for the listing, then lets a startup task call a grain on the cold-start ring", async () => {
    const c = cluster();
    let result: number | string = "not run";
    const a = c.silo(
      { name: "silo-a", uid: uid(), ip: "10.0.0.1" },
      (builder) =>
        builder.addStartupTask(async (grains) => {
          result = await grains
            .getGrain(Counter, "from-startup")
            .inc()
            .catch((e: unknown) => `failed: ${String(e)}`);
        }),
      { listed: false },
    );
    let started = false;
    const starting = a.host.start().then(() => (started = true));
    await settle(20);
    expect(started).toBe(false);
    expect(result).toBe("not run");

    // Listed, not ready: the joining members are the ring.
    c.api.setPod(a.pod);
    await starting;
    expect(result).toBe(1);
  });

  it("fails start after the join timeout, naming what to check, and leaves nothing running", async () => {
    const c = cluster();
    const clock = new PendingTimerTracker(new FakeTimeProvider());
    const fake = (clock as unknown as { inner: FakeTimeProvider }).inner;
    const a = c.silo({ name: "silo-a", uid: uid(), ip: "10.0.0.1" }, undefined, {
      listed: false,
      config: { time: clock },
    });
    const outcome = a.host.start().then(
      () => undefined,
      (err: unknown) => err as Error,
    );
    await settle(20);
    // The default is Orleans' MaxJoinAttemptTime: five minutes.
    fake.advance(5 * 60_000 - 1);
    await settle(20);
    let settled = false;
    void outcome.then(() => (settled = true));
    await settle(5);
    expect(settled).toBe(false);

    fake.advance(1);
    const err = await outcome;
    expect(err).toBeInstanceOf(Error);
    expect(err!.message).toMatch(/never listed/);
    expect(err!.message).toMatch(/selector/);
    expect(err!.message).toMatch(/label/);
    expect(a.host.health.ready().ok).toBe(false);
    expect(clock.pendingTimers()).toEqual([]);
    expect((c.network as unknown as { endpoints: Map<string, unknown> }).endpoints.size).toBe(0);
  });

  it("honours a configured join timeout", async () => {
    const c = cluster();
    const fake = new FakeTimeProvider();
    const a = c.silo({ name: "silo-a", uid: uid(), ip: "10.0.0.1" }, undefined, {
      listed: false,
      config: { time: fake, joinTimeout: { seconds: 10 } },
    });
    const outcome = a.host.start().then(
      () => undefined,
      (err: unknown) => err as Error,
    );
    await settle(20);
    fake.advance(10_000);
    expect((await outcome)?.message).toMatch(/never listed/);
  });
});

describe("a refresh pending when the silo stops", () => {
  it("is rejected as a stale view, and no timer or pending refresh outlives the host", async () => {
    const c = cluster();
    const clock = new PendingTimerTracker(new FakeTimeProvider());
    const a = c.silo({ name: "silo-a", uid: uid(), ip: "10.0.0.1" }, undefined, {
      config: { time: clock },
    });
    await a.host.start();
    const membership = (
      a.host as unknown as {
        parts: {
          membership: {
            current(): { version: number };
            refresh(version: number): Promise<unknown>;
          };
        };
      }
    ).parts.membership;
    const refresh = membership.refresh(membership.current().version + 1000).then(
      () => undefined,
      (err: unknown) => err,
    );
    await settle(20);
    await a.host.stop();
    const err = await refresh;
    expect(err).toBeInstanceOf(RejectionError);
    expect((err as RejectionError).kind).toBe("staleView");
    await settle(5);
    expect(clock.pendingTimers()).toEqual([]);
  });
});

describe("a silo joining under load", () => {
  it("owns nothing and activates nothing until it is ready, and never activates a grain twice (20 runs)", async () => {
    const existing = Array.from({ length: 30 }, (_, i) => `k${i}`);
    for (let run = 0; run < 20; run++) {
      const fresh = Array.from({ length: 30 }, (_, i) => `fresh-${run}-${i}`);
      const all = [...existing, ...fresh];
      const c = cluster();
      const a = c.silo({ name: `incumbent-${run}-${uid()}`, uid: uid(), ip: "10.0.0.1" });
      await c.join(a);
      for (const k of existing) expect(await a.host.getGrain(Counter, k).inc()).toBe(1);

      const b = c.silo({ name: `joiner-${run}-${uid()}`, uid: uid(), ip: "10.0.0.2" });
      const starting = b.host.start();
      await until(() => nodeOf(b.host).appliedViewVersion > 0, "the joiner to apply a view");
      // Not ready: grains already live, and grains FIRST called through the joiner now.
      const notReady = await Promise.all(
        all.map((k) =>
          b.host
            .getGrain(Counter, k)
            .inc()
            .catch((e: unknown) => `error: ${String(e)}`),
        ),
      );
      await starting;
      expect(notReady, `run ${run}`).toEqual(all.map((k) => (existing.includes(k) ? 2 : 1)));
      expect(nodeOf(b.host).ownedHashRanges(), `run ${run}: joiner owns ranges`).toEqual([]);
      expect(
        all.filter((k) => b.host.isActive(grainId(k))),
        `run ${run}: activated on the joiner`,
      ).toEqual([]);
      expect(all.filter((k) => !a.host.isActive(grainId(k)))).toEqual([]);

      // Ready, with calls issued at once, while the ring moves.
      c.api.setReady(b.pod.name, true);
      const afterReady = await Promise.all(
        all.map((k, i) =>
          (i % 2 === 0 ? a : b).host
            .getGrain(Counter, k)
            .inc()
            .catch((e: unknown) => `error: ${String(e)}`),
        ),
      );
      expect(afterReady, `run ${run} after ready`).toEqual(
        all.map((k) => (existing.includes(k) ? 3 : 2)),
      );
      await settle();
      expect(nodeOf(b.host).ownedHashRanges().length, `run ${run}`).toBeGreaterThan(0);
      expect(
        all.filter((k) => a.host.isActive(grainId(k)) && b.host.isActive(grainId(k))),
        `run ${run}: active on both silos`,
      ).toEqual([]);
      await c.stopAll();
    }
  }, 60_000);
});
describe("the activation gate of a silo outside its ring", () => {
  /** A peer's location cache, the introspection point for a pointer the peer holds. */
  const cacheOf = (host: SiloHost): LocationCache =>
    (host as unknown as { parts: { node: { cache: LocationCache } } }).parts.node.cache;

  it("refuses a call a peer's stale cache sends to a joiner for a grain it does not host", async () => {
    const c = cluster();
    const a = c.silo({ name: "silo-a", uid: uid(), ip: "10.0.0.1" });
    await c.join(a);
    // The joiner is listed and not ready: a member, outside the ring, serving nothing yet.
    const b = c.silo({ name: "silo-b", uid: uid(), ip: "10.0.0.2" });
    await b.host.start();
    expect(nodeOf(b.host).ownedHashRanges()).toEqual([]);

    // The peer still points at the joiner for grains it never hosted (an entry left from before
    // the joiner's view changed): the call arrives at a silo that must not create the activation.
    const keys = Array.from({ length: 10 }, (_, i) => `stale-${i}`);
    for (const key of keys) {
      cacheOf(a.host).put({
        grainId: grainId(key),
        silo: b.address,
        activationId: `stale-${key}`,
      } as GrainAddress);
    }
    const results = await Promise.all(keys.map((key) => a.host.getGrain(Counter, key).inc()));

    expect(results).toEqual(keys.map(() => 1));
    expect(keys.filter((key) => b.host.isActive(grainId(key)))).toEqual([]);
    expect(keys.filter((key) => !a.host.isActive(grainId(key)))).toEqual([]);
  });
});

describe("a draining silo", () => {
  it("creates no activation, keeps serving its own, and has a new grain placed on an active silo", async () => {
    const c = cluster();
    const a = c.silo({ name: "silo-a", uid: uid(), ip: "10.0.0.1" });
    const b = c.silo({ name: "silo-b", uid: uid(), ip: "10.0.0.2" });
    await c.join(a);
    await c.join(b);
    const keys = Array.from({ length: 60 }, (_, i) => `d${i}`);
    for (const k of keys) expect(await a.host.getGrain(Counter, k).inc()).toBe(1);
    // A second round through A fills its location cache for every key.
    for (const k of keys) expect(await a.host.getGrain(Counter, k).inc()).toBe(2);
    const onB = keys.filter((k) => b.host.isActive(grainId(k)));
    expect(onB.length).toBeGreaterThan(1);

    c.api.setTerminating(b.pod.name);
    await settle();

    // It keeps serving what it hosts, through either silo.
    const served = await Promise.all(
      onB.map((k, i) => (i % 2 === 0 ? a : b).host.getGrain(Counter, k).inc()),
    );
    expect(served).toEqual(onB.map(() => 3));
    expect(onB.filter((k) => !b.host.isActive(grainId(k)))).toEqual([]);

    // A grain first called through it is activated on the active silo.
    const fresh = Array.from({ length: 20 }, (_, i) => `drain-fresh-${i}`);
    const freshResults = await Promise.all(fresh.map((k) => b.host.getGrain(Counter, k).inc()));
    expect(freshResults).toEqual(fresh.map(() => 1));
    expect(fresh.filter((k) => b.host.isActive(grainId(k)))).toEqual([]);
    expect(fresh.filter((k) => !a.host.isActive(grainId(k)))).toEqual([]);

    // A grain whose activation on it has gone is not re-created there, though A's cache still
    // points at it: the call is refused before it runs, and A re-resolves.
    const gone = onB[0]!;
    await castGrainReference(
      b.host.getGrain(Counter, gone),
      IGrainManagementExtension,
    ).deactivateOnIdle();
    await until(() => !b.host.isActive(grainId(gone)), "the grain to deactivate on B");
    await settle();
    expect(await a.host.getGrain(Counter, gone).inc()).toBe(1);
    expect(b.host.isActive(grainId(gone))).toBe(false);
    expect(a.host.isActive(grainId(gone))).toBe(true);
  });
});

describe("a stateless worker on a silo outside its ring", () => {
  it("is called on a ring silo from a joining silo, which creates no worker", async () => {
    workerActivations.clear();
    const c = cluster();
    const a = c.silo({ name: "silo-a", uid: uid(), ip: "10.0.0.1" }, (builder) =>
      builder.registerGrain(Worker),
    );
    await c.join(a);
    const b = c.silo({ name: "silo-b", uid: uid(), ip: "10.0.0.2" }, (builder) =>
      builder.registerGrain(Worker),
    );
    await b.host.start();
    expect(nodeOf(b.host).ownedHashRanges()).toEqual([]);

    const answers = await Promise.all(
      Array.from({ length: 10 }, (_, i) => b.host.getGrain(Worker, `w${i % 3}`).where()),
    );
    expect(answers).toEqual(answers.map(() => "silo-a"));
    expect(workerActivations.get("silo-b") ?? 0).toBe(0);
  });

  it("is served by the workers a draining silo already hosts, and placed on an active silo otherwise", async () => {
    workerActivations.clear();
    const c = cluster();
    const a = c.silo({ name: "silo-a", uid: uid(), ip: "10.0.0.1" }, (builder) =>
      builder.registerGrain(Worker),
    );
    const b = c.silo({ name: "silo-b", uid: uid(), ip: "10.0.0.2" }, (builder) =>
      builder.registerGrain(Worker),
    );
    await c.join(a);
    await c.join(b);
    // Active: a call through B runs on B's own pool.
    expect(await b.host.getGrain(Worker, "held").where()).toBe("silo-b");
    const onB = workerActivations.get("silo-b") ?? 0;
    expect(onB).toBe(1);

    c.api.setTerminating(b.pod.name);
    await settle();

    // Its existing worker keeps serving, however many calls arrive at once: nothing scales.
    const held = await Promise.all(
      Array.from({ length: 8 }, () => b.host.getGrain(Worker, "held").where()),
    );
    expect(held).toEqual(held.map(() => "silo-b"));
    // A worker it does not host is called on the active silo.
    const fresh = await Promise.all(
      Array.from({ length: 8 }, (_, i) => b.host.getGrain(Worker, `fresh-${i}`).where()),
    );
    expect(fresh).toEqual(fresh.map(() => "silo-a"));
    expect(workerActivations.get("silo-b")).toBe(onB);
  });

  it("is forwarded to an active silo when a client calls it through a draining gateway", async () => {
    workerActivations.clear();
    const c = cluster();
    const a = c.silo({ name: "silo-a", uid: uid(), ip: "10.0.0.1" }, (builder) =>
      builder.registerGrain(Worker),
    );
    const b = c.silo({ name: "silo-b", uid: uid(), ip: "10.0.0.2" }, (builder) =>
      builder.registerGrain(Worker),
    );
    await c.join(a);
    await c.join(b);
    const client = createClient({
      clusterId: "k8s-view",
      transport: new InProcessTransport(c.network, "k8s-view"),
      gateway: b.address,
    }).registerGrain(Worker);
    await client.connect();
    try {
      c.api.setTerminating(b.pod.name);
      await settle();
      const answers = await Promise.all(
        Array.from({ length: 6 }, (_, i) => client.getGrain(Worker, `via-client-${i}`).where()),
      );
      expect(answers).toEqual(answers.map(() => "silo-a"));
      expect(workerActivations.get("silo-b") ?? 0).toBe(0);
    } finally {
      await client.close();
    }
  });
});

describe("a silo removed from the slices while it runs", () => {
  it("fences itself: fails liveness, refuses calls so callers re-resolve, and runs no deactivation hook", async () => {
    const c = cluster();
    const a = c.silo({ name: "silo-a", uid: uid(), ip: "10.0.0.1" });
    const b = c.silo({ name: "silo-b", uid: uid(), ip: "10.0.0.2" });
    await c.join(a);
    await c.join(b);
    const keys = Array.from({ length: 40 }, (_, i) => `fenced-${i}`);
    for (const k of keys) expect(await a.host.getGrain(Hooked, k).inc()).toBe(1);
    for (const k of keys) expect(await a.host.getGrain(Hooked, k).inc()).toBe(2);
    const onB = keys.filter((k) => b.host.isActive(hookedId(k)));
    expect(onB.length).toBeGreaterThan(0);
    expect(b.host.health.live().ok).toBe(true);
    hookRuns.length = 0;

    // Kubernetes takes the pod out of the Service while its process keeps running.
    c.api.removePod(b.pod.name);
    await settle();

    expect(b.host.health.live().ok).toBe(false);
    expect(b.host.health.ready().ok).toBe(false);
    // Its activations are abandoned: none is left, and no hook ran.
    expect(onB.filter((k) => b.host.isActive(hookedId(k)))).toEqual([]);
    expect(hookRuns).toEqual([]);

    // A call made on it is refused before it runs, as one its caller may resend elsewhere.
    const refused = await b.host
      .getGrain(Hooked, onB[0]!)
      .inc()
      .then(
        () => undefined,
        (e: unknown) => e,
      );
    expect(refused).toBeInstanceOf(RejectionError);
    expect(mayResend(refused)).toBe(true);

    // Its peers re-resolve the grains it held: each comes back once, on a member.
    const results = await Promise.all(onB.map((k) => a.host.getGrain(Hooked, k).inc()));
    expect(results).toEqual(onB.map(() => 1));
    expect(onB.filter((k) => !a.host.isActive(hookedId(k)))).toEqual([]);

    // Stopping it afterwards runs no hook either: there is nothing left to deactivate.
    await b.host.stop();
    expect(hookRuns.filter((id) => onB.some((k) => id.endsWith(k)))).toEqual([]);
  });
});

/** A transport that records every directory request this silo sends, by the grain it names. */
class DirectoryRecordingTransport implements Transport {
  readonly directoryRequests: string[] = [];
  constructor(private readonly inner: Transport) {}
  listen(...args: Parameters<Transport["listen"]>): ReturnType<Transport["listen"]> {
    return this.inner.listen(...args);
  }
  async connect(
    to: SiloAddress,
    preamble: ConnectionPreamble,
    onMessage?: MessageHandler,
  ): Promise<Connection> {
    const connection = await this.inner.connect(to, preamble, onMessage);
    const recorded: Connection = {
      send: (message) => {
        if (message.system === "directory" && message.direction === "request") {
          this.directoryRequests.push(message.targetGrain.toString());
        }
        connection.send(message);
      },
      close: (reason) => connection.close(reason),
    };
    if (connection.onClose !== undefined) {
      recorded.onClose = (callback) => connection.onClose!(callback);
    }
    return recorded;
  }
}

describe("the self-probe", () => {
  const probing = { intervalMs: 10, timeoutMs: 50, missedThreshold: 2 };

  it("is answered by the silo that asks, even one that is not a placement candidate, with no directory call", async () => {
    const c = cluster();
    const a = c.silo({ name: "silo-a", uid: uid(), ip: "10.0.0.1" });
    await c.join(a);
    let recorder: DirectoryRecordingTransport | undefined;
    const b = c.silo(
      { name: "silo-b", uid: uid(), ip: "10.0.0.2" },
      (builder) => {
        recorder = new DirectoryRecordingTransport(new InProcessTransport(c.network, "k8s-view"));
        builder.useTransport(recorder);
      },
      { selfProbe: probing },
    );
    await b.host.start();
    // Joining: no placement candidate while A is active.
    expect(nodeOf(b.host).ownedHashRanges()).toEqual([]);
    const probeId = new GrainId(SILO_PROBE_GRAIN_TYPE, b.address.ringKey);
    await until(() => b.host.isActive(probeId), "the probe to activate on B");
    await new Promise((resolve) => setTimeout(resolve, 100)); // several probe rounds
    expect(b.host.health.ready().checks.dispatcherResponsive).toBe(true);
    expect(b.host.isActive(probeId)).toBe(true);
    expect(a.host.isActive(probeId)).toBe(false);
    expect(recorder!.directoryRequests.filter((g) => g.includes(SILO_PROBE_GRAIN_TYPE))).toEqual(
      [],
    );
  });

  it("still turns readiness false when the dispatcher is wedged on a silo outside the ring", async () => {
    const c = cluster();
    const a = c.silo({ name: "silo-a", uid: uid(), ip: "10.0.0.1" });
    await c.join(a);
    const b = c.silo(
      { name: "silo-b", uid: uid(), ip: "10.0.0.2" },
      (builder) =>
        builder.addIncomingCallFilter(async (ctx) => {
          if (ctx.interfaceName === "Thresh.Runtime.ISiloProbe") throw new Error("wedged");
          await ctx.invoke();
        }),
      { selfProbe: probing },
    );
    await b.host.start();
    expect(nodeOf(b.host).ownedHashRanges()).toEqual([]);
    await until(
      () => !b.host.health.ready().checks.dispatcherResponsive,
      "readiness to turn false",
      5_000,
    );
    expect(b.host.health.ready().ok).toBe(false);
  }, 15_000);
});

describe("the view a silo applies", () => {
  it("reflects every view change published while an ownership hook was blocked", async () => {
    const c = cluster();
    const table = new HoldableReminderTable();
    const a = c.silo({ name: "silo-a", uid: uid(), ip: "10.0.0.1" }, (builder) =>
      builder.useReminders(table).allowSharedInMemoryState(),
    );
    await c.join(a);

    table.hold();
    c.api.setPod({ name: "silo-x", uid: uid(), ip: "10.0.0.8", ready: false }); // hook blocks here
    await settle();
    c.api.setPod({ name: "silo-y", uid: uid(), ip: "10.0.0.9", ready: false });
    c.api.removePod("silo-x");
    await settle();
    table.release();
    await settle();

    expect(nodeOf(a.host).appliedViewVersion).toBe(membershipVersionOf(a.host));
  });

  it("runs the ownership hooks for the view it ended on, whichever path advanced the ring", async () => {
    const c = cluster();
    const table = new HoldableReminderTable();
    const a = c.silo({ name: "silo-a", uid: uid(), ip: "10.0.0.1" }, (builder) =>
      builder.useReminders(table).allowSharedInMemoryState(),
    );
    await c.join(a);
    const b = c.silo({ name: "silo-b", uid: uid(), ip: "10.0.0.2" });
    await b.host.start();
    await settle();

    // A's hook blocks on an unrelated view; the joiner's readiness lands meanwhile, and the
    // joiner's directory traffic advances A's ring through `awaitView` before A's own loop does.
    table.hold();
    c.api.setPod({ name: "silo-x", uid: uid(), ip: "10.0.0.8", ready: false });
    await settle();
    c.api.setReady(b.pod.name, true);
    await settle();
    await Promise.all(
      Array.from({ length: 20 }, (_, i) => b.host.getGrain(Counter, `g${i}`).inc()),
    );
    table.release();
    await settle(20);

    expect(nodeOf(a.host).appliedViewVersion).toBe(membershipVersionOf(a.host));
    const owned = nodeOf(a.host).ownedHashRanges();
    expect(table.reads.slice(-owned.length)).toEqual(owned);
  });

  it("clears a silo out of peers' routing state when it leaves the Kubernetes way", async () => {
    const c = cluster();
    const a = c.silo({ name: "silo-a", uid: uid(), ip: "10.0.0.1" });
    const b = c.silo({ name: "silo-b", uid: uid(), ip: "10.0.0.2" });
    const d = c.silo({ name: "silo-c", uid: uid(), ip: "10.0.0.3" });
    for (const s of [a, b, d]) await c.join(s);

    const keys = Array.from({ length: 90 }, (_, i) => `g${i}`);
    for (const k of keys) expect(await a.host.getGrain(Counter, k).inc()).toBe(1);
    // The second round resolves through the directory, which fills A's location cache.
    for (const k of keys) expect(await a.host.getGrain(Counter, k).inc()).toBe(2);
    const onDeparted = keys.filter((k) => d.host.isActive(grainId(k)));
    expect(onDeparted.length).toBeGreaterThan(0);
    expect(nodeOf(a.host).locationCacheEntriesFor(d.address)).toBeGreaterThan(0);

    // The pod dies: its process is gone (no listener, no watch), then Kubernetes marks
    // the endpoint not ready, and only later removes it.
    d.source.disconnect();
    c.network.unregister(d.address);
    c.api.setReady(d.pod.name, false);
    await settle();
    c.api.removePod(d.pod.name);
    await settle();

    for (const survivor of [a, b]) {
      expect(nodeOf(survivor.host).locationCacheEntriesFor(d.address)).toBe(0);
    }
    const results = await Promise.all(
      keys.map((k, i) =>
        (i % 2 === 0 ? a : b).host
          .getGrain(Counter, k)
          .inc()
          .catch((e: unknown) => `error: ${String(e)}`),
      ),
    );
    const expected = keys.map((k) => (onDeparted.includes(k) ? 1 : 3));
    expect(results).toEqual(expected);
  });

  it("converges on one version with a silo that listed after a slice deletion it never saw", async () => {
    const c = cluster();
    const a = c.silo({ name: "silo-a", uid: uid(), ip: "10.0.0.1" });
    const b = c.silo({ name: "silo-b", uid: uid(), ip: "10.0.0.2" });
    await c.join(a);
    await c.join(b);
    const late = c.silo({ name: "silo-late", uid: uid(), ip: "10.0.0.3" });
    c.api.setReady(late.pod.name, true);
    // A slice of the service comes and goes before the late silo lists: A and B see the
    // deletion's revision, the late silo only the slices that are left.
    c.api.addEmptySlice("silo-svc-extra");
    c.api.deleteEmptySlice("silo-svc-extra");
    await settle();
    await late.host.start();
    expect(membershipVersionOf(late.host)).toBeLessThan(membershipVersionOf(a.host));

    // No further slice write: the late silo catches up when its peers' calls show it is behind.
    const results = await Promise.all(
      Array.from({ length: 60 }, (_, i) =>
        (i % 2 === 0 ? a : b).host
          .getGrain(Counter, `after-deletion-${i}`)
          .inc()
          .catch((e: unknown) => `error: ${String(e)}`),
      ),
    );
    expect(results.filter((r) => r !== 1)).toEqual([]);
    await settle();
    expect(membershipVersionOf(late.host)).toBe(membershipVersionOf(a.host));
    expect(nodeOf(late.host).appliedViewVersion).toBe(nodeOf(a.host).appliedViewVersion);
  });

  it("carries the same version on silos with identical views, however many events each saw", async () => {
    const c = cluster();
    const a = c.silo({ name: "silo-a", uid: uid(), ip: "10.0.0.1" });
    await c.join(a);
    // A sees history B never will: pods coming and going, and writes to other objects.
    for (let i = 0; i < 4; i++) {
      c.api.setPod({ name: `transient-${i}`, uid: uid(), ip: `10.0.1.${i}`, ready: false });
      c.api.removePod(`transient-${i}`);
      c.api.unrelatedWrite();
    }
    await settle();
    const b = c.silo({ name: "silo-b", uid: uid(), ip: "10.0.0.2" });
    c.api.unrelatedWrite();
    await c.join(b);

    expect(membershipVersionOf(a.host)).toBe(membershipVersionOf(b.host));
    expect(nodeOf(a.host).appliedViewVersion).toBe(nodeOf(b.host).appliedViewVersion);

    const results = await Promise.all(
      Array.from({ length: 80 }, (_, i) =>
        (i % 2 === 0 ? a : b).host
          .getGrain(Counter, `fresh-${i}`)
          .inc()
          .catch((e: unknown) => `error: ${String(e)}`),
      ),
    );
    expect(results.filter((r) => r !== 1)).toEqual([]);
  });
});
