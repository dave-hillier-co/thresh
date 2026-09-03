import { describe, expect, it, vi } from "vitest";
import { activeSilos } from "@thresh/core/membership";
import { SiloAddress } from "@thresh/core/silo-address";
import { KubernetesMembership } from "@thresh/clustering-k8s/kubernetes-membership";
import {
  KubernetesEndpointWatch,
  toEndpointSlice,
  type EndpointSliceSource,
  type RawEndpointSlice,
} from "@thresh/clustering-k8s/kubernetes-endpoint-watch";
import type { WatchEventType } from "@thresh/clustering-k8s/watched-endpoints";

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
  /** The resourceVersion the next list() reports; bump it as the fake's world moves on. */
  resourceVersion: string | undefined = "1";
  listCount = 0;
  watchCount = 0;
  /**
   * When set, the next list() rejects with this instead of succeeding — simulating the API
   * server still being down (e.g. mid-restart) when a reconnect attempt relists.
   */
  listError: unknown;
  /** Every resourceVersion each watch() call was started from, in order. */
  watchedFromVersions: (string | undefined)[] = [];
  private onEvent: ((type: WatchEventType, slice: RawEndpointSlice) => void) | undefined;
  private onClose: ((err?: unknown) => void) | undefined;

  async list(): ReturnType<EndpointSliceSource["list"]> {
    this.listCount += 1;
    if (this.listError !== undefined) throw this.listError;
    return this.resourceVersion !== undefined
      ? { items: this.items, resourceVersion: this.resourceVersion }
      : { items: this.items };
  }

  watch(
    resourceVersion: string | undefined,
    onEvent: (type: WatchEventType, slice: RawEndpointSlice) => void,
    onClose: (err?: unknown) => void,
  ): () => void {
    this.watchCount += 1;
    this.watchedFromVersions.push(resourceVersion);
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

  it("passes an endpoint's pod labels through as metadata, when present", () => {
    const raw = rawSlice("silo-0", "u0", "10.0.0.1");
    raw.endpoints![0]!.metadata = { "thresh.io/role": "worker" };
    const slice = toEndpointSlice(raw);
    expect(slice.endpoints![0]!.metadata).toEqual({ "thresh.io/role": "worker" });
  });

  it("leaves metadata unset when the raw endpoint has none", () => {
    const slice = toEndpointSlice(rawSlice("silo-0", "u0", "10.0.0.1"));
    expect(slice.endpoints![0]!.metadata).toBeUndefined();
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
      source.resourceVersion = "2";
      source.endWatch();
      await vi.advanceTimersByTimeAsync(20);

      expect(source.listCount).toBe(2);
      expect(source.watchCount).toBe(2);
      expect(ringKeys(membership)).toEqual(["silo-0", "silo-1"]);
      // Reconnecting re-lists first (getting a fresh resourceVersion) and starts the new
      // watch from exactly that snapshot, not from "now" — the second watch is a fresh
      // relist's resourceVersion, never the first list's stale one.
      expect(source.watchedFromVersions).toEqual(["1", "2"]);
    } finally {
      vi.useRealTimers();
    }
  });

  it("starts the watch from the list's resourceVersion, so no event between list and watch is missed", async () => {
    const source = new FakeSource();
    source.items = [rawSlice("silo-0", "u0", "10.0.0.1")];
    source.resourceVersion = "42";
    const watch = new KubernetesEndpointWatch(source);
    await watch.start();

    expect(source.watchedFromVersions).toEqual(["42"]);
  });

  it("falls back to a fresh relist when the watch closes with a 410 Gone (resourceVersion too old)", async () => {
    vi.useFakeTimers();
    try {
      const source = new FakeSource();
      source.items = [rawSlice("silo-0", "u0", "10.0.0.1")];
      source.resourceVersion = "1";
      const watch = new KubernetesEndpointWatch(source, { reconnectMs: 10 });
      const membership = new KubernetesMembership(
        new SiloAddress("silo-0", "u0", "10.0.0.1:11111"),
        watch,
        { portName: "silo" },
      );
      await watch.start();
      expect(source.watchedFromVersions).toEqual(["1"]);

      source.items = [rawSlice("silo-0", "u0", "10.0.0.1"), rawSlice("silo-1", "u1", "10.0.0.2")];
      source.resourceVersion = "99";
      source.endWatch({ code: 410, reason: "Gone" });
      await vi.advanceTimersByTimeAsync(20);

      expect(source.listCount).toBe(2);
      expect(source.watchedFromVersions).toEqual(["1", "99"]);
      expect(ringKeys(membership)).toEqual(["silo-0", "silo-1"]);
    } finally {
      vi.useRealTimers();
    }
  });

  it("backs off exponentially with jitter across consecutive failed reconnect attempts, then resets once one succeeds", async () => {
    vi.useFakeTimers();
    try {
      const source = new FakeSource();
      source.items = [rawSlice("silo-0", "u0", "10.0.0.1")];
      const apiServerDown = { code: 500, reason: "connection refused" };
      // Deterministic jitter: draws feed back predictably (0 => no jitter added, 1 => full jitter).
      const draws = [0, 0, 1, 0];
      const random = vi.fn(() => draws.shift() ?? 0);
      const watch = new KubernetesEndpointWatch(source, { reconnectMs: 10, random });
      await watch.start();
      expect(source.watchCount).toBe(1);

      // The watch drops, and the API server stays down for the next couple of reconnect
      // attempts too (the relist itself keeps failing) — the scenario the backoff exists for.
      source.listError = apiServerDown;
      source.endWatch(apiServerDown);

      // First retry: base delay (no backoff yet), draw 0 => no jitter. It fails too (relist
      // rejects), so a second retry is scheduled.
      await vi.advanceTimersByTimeAsync(9);
      expect(source.listCount).toBe(1);
      await vi.advanceTimersByTimeAsync(1);
      expect(source.listCount).toBe(2);

      // Second consecutive failed retry: delay doubles to 20ms, draw 0 => no jitter.
      await vi.advanceTimersByTimeAsync(19);
      expect(source.listCount).toBe(2);
      await vi.advanceTimersByTimeAsync(1);
      expect(source.listCount).toBe(3);

      // The API server comes back before the third retry. Delay doubles again to 40ms, draw 1
      // => full jitter added, so the actual wait is double the base delay for that attempt
      // (80ms). This retry's relist succeeds and re-establishes the watch.
      source.listError = undefined;
      await vi.advanceTimersByTimeAsync(79);
      expect(source.watchCount).toBe(1);
      await vi.advanceTimersByTimeAsync(1);
      expect(source.watchCount).toBe(2);

      // That successful reconnection reset the backoff: the next failure — whether a clean
      // close or an error — waits the base delay again, not a continuation of the prior
      // exponential growth.
      source.endWatch(apiServerDown);
      await vi.advanceTimersByTimeAsync(9);
      expect(source.watchCount).toBe(2);
      await vi.advanceTimersByTimeAsync(1);
      expect(source.watchCount).toBe(3);

      expect(random).toHaveBeenCalledTimes(4);
    } finally {
      vi.useRealTimers();
    }
  });

  it("caps the backoff delay at a maximum instead of growing without bound", async () => {
    vi.useFakeTimers();
    try {
      const source = new FakeSource();
      const random = vi.fn(() => 0);
      const watch = new KubernetesEndpointWatch(source, {
        reconnectMs: 10_000,
        maxReconnectMs: 30_000,
        random,
      });
      await watch.start();

      // Failures keep doubling (10s, 20s) until the cap (30s) is reached and held.
      for (const expectedWatchCount of [2, 3, 4]) {
        source.endWatch({ code: 500, reason: "connection refused" });
        await vi.advanceTimersByTimeAsync(30_000);
        expect(source.watchCount).toBe(expectedWatchCount);
      }
    } finally {
      vi.useRealTimers();
    }
  });

  it("resets the backoff after a successful reconnect, not only on a clean close", async () => {
    vi.useFakeTimers();
    try {
      const source = new FakeSource();
      source.items = [rawSlice("silo-0", "u0", "10.0.0.1")];
      const apiServerDown = { code: 500, reason: "connection refused" };
      const random = vi.fn(() => 0);
      const watch = new KubernetesEndpointWatch(source, { reconnectMs: 10, random });
      await watch.start();
      expect(source.watchCount).toBe(1);

      // First failure: base delay (10ms).
      source.endWatch(apiServerDown);
      await vi.advanceTimersByTimeAsync(10);
      expect(source.watchCount).toBe(2);

      // The relist+rewatch above succeeded — a healthy reconnection, not a clean close of the
      // *watch* itself. That should reset consecutiveFailures just as a clean close would, so
      // an unrelated failure later doesn't inherit the prior incident's doubled delay.
      source.endWatch(apiServerDown);
      await vi.advanceTimersByTimeAsync(9);
      expect(source.watchCount).toBe(2);
      await vi.advanceTimersByTimeAsync(1);
      expect(source.watchCount).toBe(3);
    } finally {
      vi.useRealTimers();
    }
  });

  it("keeps retrying with backoff when the reconnect's relist itself fails (API server still down)", async () => {
    vi.useFakeTimers();
    try {
      const source = new FakeSource();
      source.items = [rawSlice("silo-0", "u0", "10.0.0.1")];
      const random = vi.fn(() => 0);
      const watch = new KubernetesEndpointWatch(source, { reconnectMs: 10, random });
      await watch.start();
      expect(source.watchCount).toBe(1);

      // The watch drops and the API server is still down when we try to relist.
      source.listError = { code: 500, reason: "connection refused" };
      source.endWatch({ code: 500, reason: "connection refused" });
      await vi.advanceTimersByTimeAsync(10);
      // The relist rejected — no watch re-established yet, but the failure must still be
      // scheduled to retry (with backoff), not silently abandoned.
      expect(source.listCount).toBe(2);
      expect(source.watchCount).toBe(1);

      // Second consecutive failure (the rejected relist counts as one): delay doubles to 20ms.
      await vi.advanceTimersByTimeAsync(19);
      expect(source.listCount).toBe(2);
      await vi.advanceTimersByTimeAsync(1);
      expect(source.listCount).toBe(3);
      expect(source.watchCount).toBe(1);

      // The API server comes back: the next scheduled relist succeeds and re-establishes
      // the watch (third attempt: delay doubles again to 40ms).
      source.listError = undefined;
      await vi.advanceTimersByTimeAsync(40);
      expect(source.watchCount).toBe(2);
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
