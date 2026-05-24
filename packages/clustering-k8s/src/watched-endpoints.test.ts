import { describe, expect, it } from "vitest";
import { activeSilos } from "@tsva/core/membership";
import { SiloAddress } from "@tsva/core/silo-address";
import type { EndpointSlice } from "@tsva/clustering-k8s/endpoint-slice";
import { KubernetesMembership } from "@tsva/clustering-k8s/kubernetes-membership";
import { WatchedEndpoints } from "@tsva/clustering-k8s/watched-endpoints";

function sliceFor(name: string, uid: string, ip: string, ready = true): EndpointSlice {
  return {
    ports: [{ name: "silo", port: 11111 }],
    endpoints: [{ addresses: [ip], conditions: { ready }, targetRef: { name, uid } }],
  };
}

describe("WatchedEndpoints", () => {
  it("aggregates ADDED slices into the current set", () => {
    const watch = new WatchedEndpoints();
    const seen: number[] = [];
    watch.subscribe((slices) => seen.push(slices.length));
    watch.apply("ADDED", "slice-a", sliceFor("silo-0", "u0", "10.0.0.1"));
    watch.apply("ADDED", "slice-b", sliceFor("silo-1", "u1", "10.0.0.2"));
    expect(seen).toEqual([1, 2]);
  });

  it("replaces a slice on MODIFIED and removes it on DELETED", () => {
    const watch = new WatchedEndpoints();
    let latest: EndpointSlice[] = [];
    watch.subscribe((slices) => (latest = slices));
    watch.apply("ADDED", "slice-a", sliceFor("silo-0", "u0", "10.0.0.1"));
    watch.apply("MODIFIED", "slice-a", sliceFor("silo-0", "u0", "10.0.0.9"));
    expect(latest[0]!.endpoints![0]!.addresses[0]).toBe("10.0.0.9");
    watch.apply("DELETED", "slice-a");
    expect(latest).toHaveLength(0);
  });

  it("emits the current set to a late subscriber", () => {
    const watch = new WatchedEndpoints();
    watch.apply("ADDED", "slice-a", sliceFor("silo-0", "u0", "10.0.0.1"));
    let latest: EndpointSlice[] = [];
    watch.subscribe((slices) => (latest = slices));
    expect(latest).toHaveLength(1);
  });

  it("drives KubernetesMembership end to end", () => {
    const watch = new WatchedEndpoints();
    const local = new SiloAddress("silo-0", "u0", "10.0.0.1:11111");
    const membership = new KubernetesMembership(local, watch, { portName: "silo" });

    watch.apply("ADDED", "slice-a", sliceFor("silo-0", "u0", "10.0.0.1"));
    watch.apply("ADDED", "slice-b", sliceFor("silo-1", "u1", "10.0.0.2"));
    expect(
      activeSilos(membership.current())
        .map((s) => s.ringKey)
        .sort(),
    ).toEqual(["silo-0", "silo-1"]);

    watch.apply("DELETED", "slice-b");
    expect(activeSilos(membership.current()).map((s) => s.ringKey)).toEqual(["silo-0"]);
  });
});
