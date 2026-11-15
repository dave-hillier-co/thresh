import { describe, expect, it } from "vitest";
import { RejectionError } from "@thresh/core/errors";
import { FakeTimeProvider } from "@thresh/core/test-support/fake-time-provider";
import { PendingTimerTracker } from "@thresh/core/test-support/pending-timer-tracker";
import { activeSilos, ringSilos, type SiloMember } from "@thresh/core/membership";
import { SiloAddress } from "@thresh/core/silo-address";
import {
  metadataFromSlices,
  siloMembersFromSlices,
  type EndpointSlice,
} from "@thresh/clustering-k8s/endpoint-slice";
import {
  KubernetesMembership,
  type EndpointWatch,
} from "@thresh/clustering-k8s/kubernetes-membership";

function slice(
  endpoints: Array<{
    ip: string;
    name: string;
    uid: string;
    ready: boolean;
    terminating?: boolean;
    metadata?: Record<string, string>;
  }>,
  port = 11111,
): EndpointSlice {
  return {
    ports: [{ name: "silo", port }],
    endpoints: endpoints.map((e) => ({
      addresses: [e.ip],
      conditions: {
        ready: e.ready,
        ...(e.terminating !== undefined ? { terminating: e.terminating } : {}),
      },
      targetRef: { name: e.name, uid: e.uid },
      ...(e.metadata !== undefined ? { metadata: e.metadata } : {}),
    })),
  };
}

/**
 * A hand-driven watch. Every update carries a revision, as the contract requires: one the test
 * names, or the next of a counter.
 */
class FakeWatch implements EndpointWatch {
  private cb: ((slices: EndpointSlice[], revision: number) => void) | undefined;
  private next = 0;
  subscribe(onSlices: (slices: EndpointSlice[], revision: number) => void): () => void {
    this.cb = onSlices;
    return () => (this.cb = undefined);
  }
  emit(slices: EndpointSlice[], revision?: number): void {
    this.next = revision ?? this.next + 1;
    this.cb?.(slices, this.next);
  }
  /** An update with no revision, as a watch that breaks the contract would send. */
  emitUndated(slices: EndpointSlice[]): void {
    (this.cb as unknown as (slices: EndpointSlice[]) => void)(slices);
  }
}

/** A watch that can re-list on demand, reporting the list's own revision. */
class RelistableWatch extends FakeWatch {
  relists = 0;
  constructor(
    private readonly listed: () => { slices: EndpointSlice[]; revision: number; list: number },
  ) {
    super();
  }
  async relist(): Promise<number | undefined> {
    this.relists += 1;
    const { slices, revision, list } = this.listed();
    this.emit(slices, revision);
    return list;
  }
}

describe("siloMembersFromSlices", () => {
  it("keeps every endpoint as a member, with pod name and uid from targetRef", () => {
    const silos = siloMembersFromSlices([
      slice([
        { ip: "10.0.0.1", name: "silo-0", uid: "uid-0", ready: true },
        { ip: "10.0.0.2", name: "silo-1", uid: "uid-1", ready: false },
      ]),
    ]);
    expect(silos).toHaveLength(2);
    expect(silos[0]!.address.podName).toBe("silo-0");
    expect(silos[0]!.address.podUid).toBe("uid-0");
    expect(silos[0]!.address.endpoint).toBe("10.0.0.1:11111");
  });

  it("reads each endpoint's status from its ready and terminating conditions", () => {
    const silos = siloMembersFromSlices([
      slice([
        { ip: "10.0.0.1", name: "silo-0", uid: "uid-0", ready: true },
        { ip: "10.0.0.2", name: "silo-1", uid: "uid-1", ready: false, terminating: true },
        { ip: "10.0.0.3", name: "silo-2", uid: "uid-2", ready: false },
        { ip: "10.0.0.4", name: "silo-3", uid: "uid-3", ready: false, terminating: false },
        { ip: "10.0.0.5", name: "silo-4", uid: "uid-4", ready: true, terminating: true },
      ]),
    ]);
    expect(silos.map((s) => s.status)).toEqual([
      "active",
      "draining",
      "joining",
      "joining",
      "active",
    ]);
  });

  it("prefers the named service port when present", () => {
    const s: EndpointSlice = {
      ports: [
        { name: "gateway", port: 8080 },
        { name: "silo", port: 11111 },
      ],
      endpoints: [
        {
          addresses: ["10.0.0.1"],
          conditions: { ready: true },
          targetRef: { name: "s", uid: "u" },
        },
      ],
    };
    expect(siloMembersFromSlices([s], "silo")[0]!.address.endpoint).toBe("10.0.0.1:11111");
  });
});

describe("metadataFromSlices", () => {
  it("returns nothing when no labelPrefix is given, even if endpoints carry labels", () => {
    const s = slice([
      {
        ip: "10.0.0.1",
        name: "silo-0",
        uid: "uid-0",
        ready: true,
        metadata: { "thresh.io/role": "worker" },
      },
    ]);
    expect(metadataFromSlices([s])).toEqual(new Map());
  });

  it("strips the prefix from matching label keys", () => {
    const s = slice([
      {
        ip: "10.0.0.1",
        name: "silo-0",
        uid: "uid-0",
        ready: true,
        metadata: { "thresh.io/role": "worker", "other.io/ignored": "x" },
      },
    ]);
    expect(metadataFromSlices([s], "thresh.io/")).toEqual(new Map([["uid-0", { role: "worker" }]]));
  });

  it("omits a pod with no matching labels rather than an empty object", () => {
    const s = slice([
      {
        ip: "10.0.0.1",
        name: "silo-0",
        uid: "uid-0",
        ready: true,
        metadata: { "other.io/x": "y" },
      },
    ]);
    expect(metadataFromSlices([s], "thresh.io/")).toEqual(new Map());
  });

  it("omits a pod with no metadata at all", () => {
    const s = slice([{ ip: "10.0.0.1", name: "silo-0", uid: "uid-0", ready: true }]);
    expect(metadataFromSlices([s], "thresh.io/")).toEqual(new Map());
  });
});

describe("KubernetesMembership", () => {
  const local = new SiloAddress("silo-0", "uid-0", "10.0.0.1:11111");

  it("reflects the ready endpoints from the latest watch event", () => {
    const watch = new FakeWatch();
    const membership = new KubernetesMembership(local, watch, { portName: "silo" });
    watch.emit([
      slice([
        { ip: "10.0.0.1", name: "silo-0", uid: "uid-0", ready: true },
        { ip: "10.0.0.2", name: "silo-1", uid: "uid-1", ready: true },
      ]),
    ]);
    expect(
      activeSilos(membership.current())
        .map((s) => s.ringKey)
        .sort(),
    ).toEqual(["silo-0", "silo-1"]);
    expect(membership.localSilo().ringKey).toBe("silo-0");
  });

  it("bumps the version and pushes a snapshot on each reconciliation", async () => {
    const watch = new FakeWatch();
    const membership = new KubernetesMembership(local, watch, { portName: "silo" });
    const next = membership.updates()[Symbol.asyncIterator]().next();

    watch.emit([slice([{ ip: "10.0.0.1", name: "silo-0", uid: "uid-0", ready: true }])]);

    const { value } = await next;
    expect(value.version).toBe(1);
    expect(activeSilos(value)).toHaveLength(1);
  });

  it("yields the latest view to an iterator that was busy when it was published", async () => {
    // The consumer is applying one view (its hooks do store I/O) while two more are
    // published: neither may be lost because nobody was waiting at that instant.
    const watch = new FakeWatch();
    const membership = new KubernetesMembership(local, watch, { portName: "silo" });
    const iterator = membership.updates()[Symbol.asyncIterator]();
    const peer = (ready: boolean) => ({ ip: "10.0.0.2", name: "silo-1", uid: "uid-1", ready });

    watch.emit([slice([peer(true)])], 10);
    const first = await iterator.next();
    expect(first.value.version).toBe(10);
    // Busy: not waiting on the iterator while these land.
    watch.emit([slice([peer(false)])], 11);
    watch.emit([slice([])], 12);

    const next = await iterator.next();
    expect(next.value.version).toBe(12);
    expect(next.value.silos.map((m: SiloMember) => m.address.ringKey)).toEqual([]);
  });

  it("takes the view version from the revision the watch observed", () => {
    const watch = new FakeWatch();
    const a = new KubernetesMembership(local, watch, { portName: "silo" });
    watch.emit([slice([{ ip: "10.0.0.2", name: "silo-1", uid: "uid-1", ready: true }])], 4711);
    expect(a.current().version).toBe(4711);
  });

  it("publishes nothing for a revision it already holds", async () => {
    const watch = new FakeWatch();
    const membership = new KubernetesMembership(local, watch, { portName: "silo" });
    const peer = slice([{ ip: "10.0.0.2", name: "silo-1", uid: "uid-1", ready: true }]);
    watch.emit([peer], 7);
    const held = membership.current();
    watch.emit([peer], 7); // a re-list that missed nothing
    expect(membership.current()).toBe(held);
  });

  it("takes a terminating silo out of the ring but keeps it in the view as draining", () => {
    const watch = new FakeWatch();
    const membership = new KubernetesMembership(local, watch, { portName: "silo" });
    watch.emit([
      slice([
        { ip: "10.0.0.1", name: "silo-0", uid: "uid-0", ready: true },
        { ip: "10.0.0.2", name: "silo-1", uid: "uid-1", ready: true },
      ]),
    ]);
    watch.emit([
      slice([
        { ip: "10.0.0.1", name: "silo-0", uid: "uid-0", ready: true },
        { ip: "10.0.0.2", name: "silo-1", uid: "uid-1", ready: false, terminating: true },
      ]),
    ]);
    // Out of the ring: it must take no new placements and own no new ranges.
    expect(activeSilos(membership.current()).map((s) => s.ringKey)).toEqual(["silo-0"]);
    // Still a member: its grains are still running there, so peers must not
    // treat its directory entries as pointers to a silo that will never answer.
    expect(
      membership
        .current()
        .silos.map((s) => `${s.address.ringKey}:${s.status}`)
        .sort(),
    ).toEqual(["silo-0:active", "silo-1:draining"]);
    expect(membership.current().version).toBe(2);
  });

  it("drops a silo once its endpoint is removed, not merely not ready", () => {
    const watch = new FakeWatch();
    const membership = new KubernetesMembership(local, watch, { portName: "silo" });
    watch.emit([
      slice([
        { ip: "10.0.0.1", name: "silo-0", uid: "uid-0", ready: true },
        { ip: "10.0.0.2", name: "silo-1", uid: "uid-1", ready: false },
      ]),
    ]);
    watch.emit([slice([{ ip: "10.0.0.1", name: "silo-0", uid: "uid-0", ready: true }])]);
    expect(membership.current().silos.map((s) => s.address.ringKey)).toEqual(["silo-0"]);
  });

  it("recognises a restarted pod by its new uid", () => {
    const watch = new FakeWatch();
    const membership = new KubernetesMembership(local, watch, { portName: "silo" });
    watch.emit([slice([{ ip: "10.0.0.5", name: "silo-2", uid: "uid-old", ready: true }])]);
    watch.emit([slice([{ ip: "10.0.0.6", name: "silo-2", uid: "uid-new", ready: true }])]);
    const silo2 = activeSilos(membership.current()).find((s) => s.ringKey === "silo-2");
    expect(silo2?.podUid).toBe("uid-new");
  });

  describe("the local silo's own membership", () => {
    const statuses = (membership: KubernetesMembership): string[] =>
      membership
        .current()
        .silos.map((m) => `${m.address.ringKey}:${m.status}`)
        .sort();

    it("takes its own entry from the slices like any other, and is absent until listed", () => {
      const watch = new FakeWatch();
      const membership = new KubernetesMembership(local, watch, { portName: "silo" });
      expect(membership.localStanding()).toBe("unlisted");

      // The watch shows a peer but not this silo's own endpoint yet: it is not in its view.
      watch.emit([slice([{ ip: "10.0.0.2", name: "silo-1", uid: "uid-1", ready: true }])]);
      expect(statuses(membership)).toEqual(["silo-1:active"]);
      expect(membership.localStanding()).toBe("unlisted");

      // Listed, not ready: joining, and out of the ring while a peer is active.
      watch.emit([
        slice([
          { ip: "10.0.0.1", name: "silo-0", uid: "uid-0", ready: false },
          { ip: "10.0.0.2", name: "silo-1", uid: "uid-1", ready: true },
        ]),
      ]);
      expect(statuses(membership)).toEqual(["silo-0:joining", "silo-1:active"]);
      expect(ringSilos(membership.current()).map((s) => s.ringKey)).toEqual(["silo-1"]);
      expect(membership.localStanding()).toBe("listed");

      // Ready: active and in the ring.
      watch.emit([
        slice([
          { ip: "10.0.0.1", name: "silo-0", uid: "uid-0", ready: true },
          { ip: "10.0.0.2", name: "silo-1", uid: "uid-1", ready: true },
        ]),
      ]);
      expect(statuses(membership)).toEqual(["silo-0:active", "silo-1:active"]);
    });

    it("drains itself out of its own ring when its endpoint is terminating", () => {
      const watch = new FakeWatch();
      const membership = new KubernetesMembership(local, watch, { portName: "silo" });
      watch.emit([
        slice([
          { ip: "10.0.0.1", name: "silo-0", uid: "uid-0", ready: false, terminating: true },
          { ip: "10.0.0.2", name: "silo-1", uid: "uid-1", ready: true },
        ]),
      ]);
      expect(statuses(membership)).toEqual(["silo-0:draining", "silo-1:active"]);
      expect(ringSilos(membership.current()).map((s) => s.ringKey)).toEqual(["silo-1"]);
    });

    it("holds the same view as a peer that observed the same slices, its own entry included", () => {
      const peerAddress = new SiloAddress("silo-1", "uid-1", "10.0.0.2:11111");
      const slices = [
        slice([
          { ip: "10.0.0.1", name: "silo-0", uid: "uid-0", ready: false },
          { ip: "10.0.0.2", name: "silo-1", uid: "uid-1", ready: true },
        ]),
      ];
      const watchA = new FakeWatch();
      const watchB = new FakeWatch();
      const a = new KubernetesMembership(local, watchA, { portName: "silo" });
      const b = new KubernetesMembership(peerAddress, watchB, { portName: "silo" });
      watchA.emit(slices, 31);
      watchB.emit(slices, 31);
      expect(a.current()).toEqual(b.current());
    });

    it("forms a ring of the joining silos while none is ready (a cold start)", () => {
      const watch = new FakeWatch();
      const membership = new KubernetesMembership(local, watch, { portName: "silo" });
      watch.emit([
        slice([
          { ip: "10.0.0.1", name: "silo-0", uid: "uid-0", ready: false },
          { ip: "10.0.0.2", name: "silo-1", uid: "uid-1", ready: false },
        ]),
      ]);
      expect(
        ringSilos(membership.current())
          .map((s) => s.ringKey)
          .sort(),
      ).toEqual(["silo-0", "silo-1"]);
    });

    it("reports itself removed once a view omits it after it was listed", () => {
      const watch = new FakeWatch();
      const membership = new KubernetesMembership(local, watch, { portName: "silo" });
      watch.emit([
        slice([
          { ip: "10.0.0.1", name: "silo-0", uid: "uid-0", ready: true },
          { ip: "10.0.0.2", name: "silo-1", uid: "uid-1", ready: true },
        ]),
      ]);
      watch.emit([slice([{ ip: "10.0.0.2", name: "silo-1", uid: "uid-1", ready: true }])]);
      expect(statuses(membership)).toEqual(["silo-1:active"]);
      expect(membership.localStanding()).toBe("removed");

      // A removal is final for this incarnation: a later listing does not undo it.
      watch.emit([
        slice([
          { ip: "10.0.0.1", name: "silo-0", uid: "uid-0", ready: true },
          { ip: "10.0.0.2", name: "silo-1", uid: "uid-1", ready: true },
        ]),
      ]);
      expect(membership.localStanding()).toBe("removed");
    });

    it("is not removed by a view that omits it before it was ever listed", () => {
      const watch = new FakeWatch();
      const membership = new KubernetesMembership(local, watch, { portName: "silo" });
      watch.emit([]);
      watch.emit([slice([{ ip: "10.0.0.2", name: "silo-1", uid: "uid-1", ready: true }])]);
      expect(membership.localStanding()).toBe("unlisted");
    });
  });

  describe("the revision", () => {
    it("refuses an update that carries no revision", () => {
      const watch = new FakeWatch();
      const membership = new KubernetesMembership(local, watch, { portName: "silo" });
      expect(() =>
        watch.emitUndated([slice([{ ip: "10.0.0.1", name: "silo-0", uid: "uid-0", ready: true }])]),
      ).toThrow(/revision/);
      expect(membership.current().silos).toEqual([]);
      expect(membership.current().version).toBe(0);
    });
  });

  describe("refresh", () => {
    const self = slice([{ ip: "10.0.0.1", name: "silo-0", uid: "uid-0", ready: true }]);

    it("resolves at once for a version it already holds", async () => {
      const watch = new RelistableWatch(() => ({ slices: [self], revision: 5, list: 5 }));
      const membership = new KubernetesMembership(local, watch, { portName: "silo" });
      watch.emit([self], 9);
      await membership.refresh(9);
      await membership.refresh(3);
      expect(watch.relists).toBe(0);
    });

    it("resolves once the watch delivers the version, without re-listing", async () => {
      const watch = new RelistableWatch(() => ({ slices: [self], revision: 5, list: 5 }));
      const membership = new KubernetesMembership(local, watch, {
        portName: "silo",
        refreshWatchGraceMs: 1_000,
      });
      watch.emit([self], 9);
      const refreshed = membership.refresh(12);
      watch.emit([self], 12);
      await refreshed;
      expect(membership.current().version).toBe(12);
      expect(watch.relists).toBe(0);
    });

    it("re-lists when the watch does not deliver, and adopts a version the list proves", async () => {
      // The peer is at 20: a slice deletion at 20 this silo's watch never saw. The list
      // (global revision 25) shows the slices it already holds, at slice revision 9.
      const watch = new RelistableWatch(() => ({ slices: [self], revision: 9, list: 25 }));
      const membership = new KubernetesMembership(local, watch, {
        portName: "silo",
        refreshWatchGraceMs: 0,
      });
      watch.emit([self], 9);
      await membership.refresh(20);
      expect(watch.relists).toBe(1);
      expect(membership.current().version).toBe(20);
      expect(membership.current().silos.map((m) => m.address.ringKey)).toEqual(["silo-0"]);
    });

    it("takes the listed contents when the list brings a change", async () => {
      const both = slice([
        { ip: "10.0.0.1", name: "silo-0", uid: "uid-0", ready: true },
        { ip: "10.0.0.2", name: "silo-1", uid: "uid-1", ready: true },
      ]);
      const watch = new RelistableWatch(() => ({ slices: [both], revision: 14, list: 30 }));
      const membership = new KubernetesMembership(local, watch, {
        portName: "silo",
        refreshWatchGraceMs: 0,
      });
      watch.emit([self], 9);
      await membership.refresh(14);
      expect(membership.current().version).toBe(14);
      expect(membership.current().silos.map((m) => m.address.ringKey)).toEqual([
        "silo-0",
        "silo-1",
      ]);
    });

    it("does not adopt a version the list cannot prove, and waits for the watch", async () => {
      const watch = new RelistableWatch(() => ({ slices: [self], revision: 9, list: 15 }));
      const membership = new KubernetesMembership(local, watch, {
        portName: "silo",
        refreshWatchGraceMs: 0,
      });
      watch.emit([self], 9);
      let done = false;
      const refreshed = membership.refresh(20).then(() => (done = true));
      await new Promise((resolve) => setTimeout(resolve, 5));
      expect(done).toBe(false);
      expect(membership.current().version).toBe(9);
      watch.emit([self], 21);
      await refreshed;
      expect(membership.current().version).toBe(21);
    });

    describe("bounded and stopped with the service", () => {
      const unreachable = () => ({ slices: [self], revision: 9, list: 15 });

      function build(refreshTimeoutMs?: number) {
        const fake = new FakeTimeProvider();
        const clock = new PendingTimerTracker(fake);
        // Advance the clock, then let the promise chains it released run.
        const advance = async (ms: number): Promise<void> => {
          fake.advance(ms);
          await new Promise((resolve) => setImmediate(resolve));
        };
        const watch = new RelistableWatch(unreachable);
        const membership = new KubernetesMembership(local, watch, {
          portName: "silo",
          refreshWatchGraceMs: 0,
          time: clock,
          ...(refreshTimeoutMs !== undefined ? { refreshTimeoutMs } : {}),
        });
        watch.emit([self], 9);
        return { clock, advance, watch, membership };
      }

      it("rejects with the retriable stale-view rejection when no list can prove the version", async () => {
        const { clock, advance, watch, membership } = build(5_000);
        const outcome = membership.refresh(20).then(
          () => undefined,
          (err: unknown) => err,
        );
        await advance(0);
        expect(watch.relists).toBe(1);
        await advance(4_999);
        expect(membership.current().version).toBe(9);
        await advance(1);
        const err = await outcome;
        expect(err).toBeInstanceOf(RejectionError);
        expect((err as RejectionError).kind).toBe("staleView");
        expect(clock.pendingTimers()).toEqual([]);
      });

      it("leaves no pending refresh behind once one has timed out", async () => {
        const { advance, watch, membership } = build(1_000);
        const first = membership.refresh(20).catch(() => undefined);
        await advance(1_000);
        await first;
        // A second request for the same version starts afresh rather than joining a dead one.
        const second = membership.refresh(20).then(
          () => "resolved",
          () => "rejected",
        );
        await advance(0);
        expect(watch.relists).toBe(2);
        watch.emit([self], 20);
        expect(await second).toBe("resolved");
      });

      it("settles a refresh still pending when the service stops, leaving no timer", async () => {
        const { clock, advance, membership } = build(60_000);
        const outcome = membership.refresh(20).then(
          () => undefined,
          (err: unknown) => err,
        );
        await advance(0);
        membership.stop();
        const err = await outcome;
        expect(err).toBeInstanceOf(RejectionError);
        expect((err as RejectionError).kind).toBe("staleView");
        expect(clock.pendingTimers()).toEqual([]);
      });

      it("refuses a new refresh after stop without arming a timer", async () => {
        const { clock, membership } = build();
        membership.stop();
        await expect(membership.refresh(20)).rejects.toBeInstanceOf(RejectionError);
        expect(clock.pendingTimers()).toEqual([]);
      });

      it("defaults the bound to the silo call timeout", async () => {
        const { advance, membership } = build();
        const outcome = membership.refresh(20).then(
          () => "resolved",
          () => "rejected",
        );
        await advance(29_999);
        let settled = false;
        void outcome.then(() => (settled = true));
        await Promise.resolve();
        expect(settled).toBe(false);
        await advance(1);
        expect(await outcome).toBe("rejected");
      });
    });

    it("publishes the adopted version to its updates", async () => {
      const watch = new RelistableWatch(() => ({ slices: [self], revision: 9, list: 25 }));
      const membership = new KubernetesMembership(local, watch, {
        portName: "silo",
        refreshWatchGraceMs: 0,
      });
      watch.emit([self], 9);
      const next = membership.updates()[Symbol.asyncIterator]().next();
      await membership.refresh(20);
      expect((await next).value.version).toBe(20);
    });
  });

  describe("metadata from pod labels", () => {
    const member = (m: KubernetesMembership, ringKey: string) =>
      m.current().silos.find((s) => s.address.ringKey === ringKey);

    it("surfaces labels under metadataLabelPrefix as SiloMember.metadata", () => {
      const watch = new FakeWatch();
      const membership = new KubernetesMembership(local, watch, {
        portName: "silo",
        metadataLabelPrefix: "thresh.io/",
      });
      watch.emit([
        slice([
          {
            ip: "10.0.0.2",
            name: "silo-1",
            uid: "uid-1",
            ready: true,
            metadata: { "thresh.io/role": "worker" },
          },
        ]),
      ]);
      expect(member(membership, "silo-1")?.metadata).toEqual({ role: "worker" });
    });

    it("propagates a label change on the next reconciliation", () => {
      const watch = new FakeWatch();
      const membership = new KubernetesMembership(local, watch, {
        portName: "silo",
        metadataLabelPrefix: "thresh.io/",
      });
      watch.emit([
        slice([
          {
            ip: "10.0.0.2",
            name: "silo-1",
            uid: "uid-1",
            ready: true,
            metadata: { "thresh.io/role": "worker" },
          },
        ]),
      ]);
      expect(member(membership, "silo-1")?.metadata).toEqual({ role: "worker" });

      watch.emit([
        slice([
          {
            ip: "10.0.0.2",
            name: "silo-1",
            uid: "uid-1",
            ready: true,
            metadata: { "thresh.io/role": "gateway" },
          },
        ]),
      ]);
      expect(member(membership, "silo-1")?.metadata).toEqual({ role: "gateway" });
    });

    it("leaves metadata absent for a silo whose pod has no matching labels", () => {
      const watch = new FakeWatch();
      const membership = new KubernetesMembership(local, watch, {
        portName: "silo",
        metadataLabelPrefix: "thresh.io/",
      });
      watch.emit([slice([{ ip: "10.0.0.2", name: "silo-1", uid: "uid-1", ready: true }])]);
      expect(member(membership, "silo-1")?.metadata).toBeUndefined();
    });

    it("leaves metadata absent everywhere when metadataLabelPrefix is not set", () => {
      const watch = new FakeWatch();
      const membership = new KubernetesMembership(local, watch, { portName: "silo" });
      watch.emit([
        slice([
          {
            ip: "10.0.0.2",
            name: "silo-1",
            uid: "uid-1",
            ready: true,
            metadata: { "thresh.io/role": "worker" },
          },
        ]),
      ]);
      expect(member(membership, "silo-1")?.metadata).toBeUndefined();
    });
  });
});
