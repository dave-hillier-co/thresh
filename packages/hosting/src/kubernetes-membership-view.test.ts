import { afterEach, describe, expect, it } from "vitest";
import { defineGrain } from "@thresh/core/define-grain";
import { GrainId } from "@thresh/core/grain-id";
import type { ReminderEntry } from "@thresh/core/reminder";
import { SiloAddress } from "@thresh/core/silo-address";
import {
  FakeEndpointSliceApi,
  type FakeEndpointSliceSource,
  type FakePod,
} from "@thresh/clustering-k8s/fake-endpoint-slice-api";
import { KubernetesEndpointWatch } from "@thresh/clustering-k8s/kubernetes-endpoint-watch";
import { InProcessNetwork } from "@thresh/messaging/in-process-transport";
import { MemoryReminderTable } from "@thresh/reminders/memory-reminder-table";
import { createSilo, type SiloBuilder } from "@thresh/hosting/silo-builder";
import type { SiloHost } from "@thresh/hosting/silo-host";

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

  /** A pod whose endpoint is published not-ready, then a silo for it (not yet started). */
  silo(pod: Omit<FakePod, "ready">, configure?: (builder: SiloBuilder) => void): K8sSilo {
    const full: FakePod = { ...pod, ready: false };
    this.api.setPod(full);
    const source = this.api.source();
    const address = new SiloAddress(pod.name, pod.uid, this.api.endpointOf(pod));
    const builder = createSilo({
      clusterId: "k8s-view",
      local: address,
      selfProbe: { enabled: false },
      callTimeout: { seconds: 2 },
      gracefulShutdownMs: 0,
    })
      .useKubernetesMembership(new KubernetesEndpointWatch(source), { portName: "silo" })
      .useInProcessTransport(this.network)
      .disableTransactions()
      .registerGrain(Counter);
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
async function until(predicate: () => boolean, label: string): Promise<void> {
  const deadline = Date.now() + 2_000;
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

  it("reports not-ready membership until the node has applied a non-empty view", async () => {
    const c = cluster();
    const a = c.silo({ name: "silo-a", uid: uid(), ip: "10.0.0.1" });
    expect(a.host.health.ready().checks.membershipHealthy).toBe(false);
    await a.host.start();
    expect(a.host.health.ready().checks.membershipHealthy).toBe(true);
    expect(nodeOf(a.host).ownedHashRanges().length).toBeGreaterThan(0);
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
