import { describe, expect, it } from "vitest";
import { activeSilos } from "@thresh/core/membership";
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
    metadata?: Record<string, string>;
  }>,
  port = 11111,
): EndpointSlice {
  return {
    ports: [{ name: "silo", port }],
    endpoints: endpoints.map((e) => ({
      addresses: [e.ip],
      conditions: { ready: e.ready },
      targetRef: { name: e.name, uid: e.uid },
      ...(e.metadata !== undefined ? { metadata: e.metadata } : {}),
    })),
  };
}

class FakeWatch implements EndpointWatch {
  private cb: ((slices: EndpointSlice[]) => void) | undefined;
  subscribe(onSlices: (slices: EndpointSlice[]) => void): () => void {
    this.cb = onSlices;
    return () => (this.cb = undefined);
  }
  emit(slices: EndpointSlice[]): void {
    this.cb?.(slices);
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

  it("marks a ready endpoint active and a present-but-not-ready one draining", () => {
    const silos = siloMembersFromSlices([
      slice([
        { ip: "10.0.0.1", name: "silo-0", uid: "uid-0", ready: true },
        { ip: "10.0.0.2", name: "silo-1", uid: "uid-1", ready: false },
      ]),
    ]);
    expect(silos.map((s) => s.status)).toEqual(["active", "draining"]);
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

  it("takes a not-ready silo out of the ring but keeps it in the view as draining", () => {
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
        { ip: "10.0.0.2", name: "silo-1", uid: "uid-1", ready: false },
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
