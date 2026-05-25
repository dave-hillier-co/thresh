import { describe, expect, it } from "vitest";
import { activeSilos } from "@tsva/core/membership";
import { SiloAddress } from "@tsva/core/silo-address";
import { readySilosFromSlices, type EndpointSlice } from "@tsva/clustering-k8s/endpoint-slice";
import {
  KubernetesMembership,
  type EndpointWatch,
} from "@tsva/clustering-k8s/kubernetes-membership";

function slice(
  endpoints: Array<{ ip: string; name: string; uid: string; ready: boolean }>,
  port = 11111,
): EndpointSlice {
  return {
    ports: [{ name: "silo", port }],
    endpoints: endpoints.map((e) => ({
      addresses: [e.ip],
      conditions: { ready: e.ready },
      targetRef: { name: e.name, uid: e.uid },
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

describe("readySilosFromSlices", () => {
  it("includes only ready endpoints, with pod name and uid from targetRef", () => {
    const silos = readySilosFromSlices([
      slice([
        { ip: "10.0.0.1", name: "silo-0", uid: "uid-0", ready: true },
        { ip: "10.0.0.2", name: "silo-1", uid: "uid-1", ready: false },
      ]),
    ]);
    expect(silos).toHaveLength(1);
    expect(silos[0]!.podName).toBe("silo-0");
    expect(silos[0]!.podUid).toBe("uid-0");
    expect(silos[0]!.endpoint).toBe("10.0.0.1:11111");
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
    expect(readySilosFromSlices([s], "silo")[0]!.endpoint).toBe("10.0.0.1:11111");
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

  it("drops a silo when its endpoint is no longer ready", () => {
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
    expect(activeSilos(membership.current()).map((s) => s.ringKey)).toEqual(["silo-0"]);
    expect(membership.current().version).toBe(2);
  });

  it("recognises a restarted pod by its new uid", () => {
    const watch = new FakeWatch();
    const membership = new KubernetesMembership(local, watch, { portName: "silo" });
    watch.emit([slice([{ ip: "10.0.0.5", name: "silo-2", uid: "uid-old", ready: true }])]);
    watch.emit([slice([{ ip: "10.0.0.6", name: "silo-2", uid: "uid-new", ready: true }])]);
    const silo2 = activeSilos(membership.current()).find((s) => s.ringKey === "silo-2");
    expect(silo2?.podUid).toBe("uid-new");
  });
});
