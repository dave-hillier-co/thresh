import { describe, expect, it, vi } from "vitest";
import { activeSilos } from "@tsva/core/membership";
import { SiloAddress } from "@tsva/core/silo-address";
import { KubernetesMembership } from "@tsva/clustering-k8s/kubernetes-membership";
import {
  KubernetesEndpointWatch,
  toEndpointSlice,
  type EndpointSliceSource,
  type RawEndpointSlice,
} from "@tsva/clustering-k8s/kubernetes-endpoint-watch";
import type { WatchEventType } from "@tsva/clustering-k8s/watched-endpoints";

function rawSlice(name: string, uid: string, ip: string, ready = true): RawEndpointSlice {
  return {
    metadata: { name: `slice-${name}` },
    ports: [{ name: "silo", port: 11111 }],
    endpoints: [{ addresses: [ip], conditions: { ready }, targetRef: { name, uid } }],
  };
}

/** Fake of the true boundary (the Kubernetes API): records calls and lets a test
 * drive watch events and watch closure by hand. */
class FakeSource implements EndpointSliceSource {
  items: RawEndpointSlice[] = [];
  listCount = 0;
  watchCount = 0;
  private onEvent: ((type: WatchEventType, slice: RawEndpointSlice) => void) | undefined;
  private onClose: ((err?: unknown) => void) | undefined;

  async list(): Promise<RawEndpointSlice[]> {
    this.listCount += 1;
    return this.items;
  }

  watch(
    onEvent: (type: WatchEventType, slice: RawEndpointSlice) => void,
    onClose: (err?: unknown) => void,
  ): () => void {
    this.watchCount += 1;
    this.onEvent = onEvent;
    this.onClose = onClose;
    return () => {
      this.onEvent = undefined;
    };
  }

  push(type: WatchEventType, slice: RawEndpointSlice): void {
    this.onEvent?.(type, slice);
  }

  endWatch(err?: unknown): void {
    this.onClose?.(err);
  }
}

const ringKeys = (m: KubernetesMembership) =>
  activeSilos(m.current())
    .map((s) => s.ringKey)
    .sort();

describe("toEndpointSlice", () => {
  it("maps the raw slice fields the failure detector reads", () => {
    const slice = toEndpointSlice(rawSlice("silo-0", "u0", "10.0.0.1"));
    expect(slice.ports).toEqual([{ name: "silo", port: 11111 }]);
    expect(slice.endpoints).toEqual([
      {
        addresses: ["10.0.0.1"],
        conditions: { ready: true },
        targetRef: { name: "silo-0", uid: "u0" },
      },
    ]);
  });
});

describe("KubernetesEndpointWatch", () => {
  it("lists existing slices on start so membership reflects ready silos", async () => {
    const source = new FakeSource();
    source.items = [rawSlice("silo-0", "u0", "10.0.0.1"), rawSlice("silo-1", "u1", "10.0.0.2")];
    const watch = new KubernetesEndpointWatch(source);
    const local = new SiloAddress("silo-0", "u0", "10.0.0.1:11111");
    const membership = new KubernetesMembership(local, watch, { portName: "silo" });

    await watch.start();

    expect(source.listCount).toBe(1);
    expect(ringKeys(membership)).toEqual(["silo-0", "silo-1"]);
  });

  it("applies ADDED and DELETED watch events to the live set", async () => {
    const source = new FakeSource();
    source.items = [rawSlice("silo-0", "u0", "10.0.0.1")];
    const watch = new KubernetesEndpointWatch(source);
    const membership = new KubernetesMembership(
      new SiloAddress("silo-0", "u0", "10.0.0.1:11111"),
      watch,
      { portName: "silo" },
    );
    await watch.start();

    source.push("ADDED", rawSlice("silo-1", "u1", "10.0.0.2"));
    expect(ringKeys(membership)).toEqual(["silo-0", "silo-1"]);

    source.push("DELETED", rawSlice("silo-1", "u1", "10.0.0.2"));
    expect(ringKeys(membership)).toEqual(["silo-0"]);
  });

  it("treats a not-ready peer endpoint as not a member (the failure detector)", async () => {
    const source = new FakeSource();
    source.items = [rawSlice("silo-0", "u0", "10.0.0.1"), rawSlice("silo-1", "u1", "10.0.0.2")];
    const watch = new KubernetesEndpointWatch(source);
    const membership = new KubernetesMembership(
      new SiloAddress("silo-0", "u0", "10.0.0.1:11111"),
      watch,
      { portName: "silo" },
    );
    await watch.start();
    expect(ringKeys(membership)).toEqual(["silo-0", "silo-1"]);

    source.push("MODIFIED", rawSlice("silo-1", "u1", "10.0.0.2", false));
    expect(ringKeys(membership)).toEqual(["silo-0"]);
  });

  it("always includes the local silo so a first pod can bootstrap (empty slices)", async () => {
    const source = new FakeSource();
    source.items = [];
    const watch = new KubernetesEndpointWatch(source);
    const membership = new KubernetesMembership(
      new SiloAddress("silo-0", "u0", "10.0.0.1:11111"),
      watch,
      { portName: "silo" },
    );
    await watch.start();
    expect(ringKeys(membership)).toEqual(["silo-0"]);
  });

  it("re-lists and re-watches when the watch connection closes", async () => {
    vi.useFakeTimers();
    try {
      const source = new FakeSource();
      source.items = [rawSlice("silo-0", "u0", "10.0.0.1")];
      const watch = new KubernetesEndpointWatch(source, { reconnectMs: 10 });
      const membership = new KubernetesMembership(
        new SiloAddress("silo-0", "u0", "10.0.0.1:11111"),
        watch,
        { portName: "silo" },
      );
      await watch.start();
      expect(source.watchCount).toBe(1);

      // The server dropped the watch; a new pod was added while we were blind.
      source.items = [rawSlice("silo-0", "u0", "10.0.0.1"), rawSlice("silo-1", "u1", "10.0.0.2")];
      source.endWatch();
      await vi.advanceTimersByTimeAsync(20);

      expect(source.listCount).toBe(2);
      expect(source.watchCount).toBe(2);
      expect(ringKeys(membership)).toEqual(["silo-0", "silo-1"]);
    } finally {
      vi.useRealTimers();
    }
  });

  it("stops watching after stop()", async () => {
    vi.useFakeTimers();
    try {
      const source = new FakeSource();
      const watch = new KubernetesEndpointWatch(source, { reconnectMs: 10 });
      await watch.start();
      watch.stop();
      source.endWatch();
      await vi.advanceTimersByTimeAsync(50);
      // No reconnect after stop.
      expect(source.listCount).toBe(1);
    } finally {
      vi.useRealTimers();
    }
  });
});
