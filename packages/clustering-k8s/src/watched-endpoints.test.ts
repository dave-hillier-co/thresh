import { describe, expect, it } from "vitest";
import { activeSilos } from "@thresh/core/membership";
import { SiloAddress } from "@thresh/core/silo-address";
import type { EndpointSlice } from "@thresh/clustering-k8s/endpoint-slice";
import { KubernetesMembership } from "@thresh/clustering-k8s/kubernetes-membership";
import { WatchedEndpoints } from "@thresh/clustering-k8s/watched-endpoints";

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

describe("WatchedEndpoints' observed revision", () => {
  const at = (slice: EndpointSlice, resourceVersion: string): EndpointSlice => ({
    ...slice,
    resourceVersion,
  });
  const list = (...entries: Array<[string, EndpointSlice]>) => new Map(entries);

  it("is the highest slice revision seen across the list and the watch events", () => {
    const watch = new WatchedEndpoints();
    const seen: Array<number | undefined> = [];
    watch.subscribe((_slices, revision) => seen.push(revision));
    watch.reset(list(["slice-a", at(sliceFor("silo-0", "u0", "10.0.0.1"), "40")]), "95");
    watch.apply("MODIFIED", "slice-a", at(sliceFor("silo-0", "u0", "10.0.0.9"), "120"));
    watch.apply("DELETED", "slice-a", at(sliceFor("silo-0", "u0", "10.0.0.9"), "130"));
    expect(seen).toEqual([40, 120, 130]);
  });

  it("is the same for two watchers that observed the same writes, whenever each listed", () => {
    // The list's own resourceVersion is the cluster's global revision, which moves
    // with writes to unrelated objects: folding it in would number one view twice.
    const early = new WatchedEndpoints();
    early.reset(list(["slice-a", at(sliceFor("silo-0", "u0", "10.0.0.1"), "40")]), "41");
    early.apply("MODIFIED", "slice-a", at(sliceFor("silo-1", "u1", "10.0.0.2"), "52"));

    const late = new WatchedEndpoints();
    late.reset(list(["slice-a", at(sliceFor("silo-1", "u1", "10.0.0.2"), "52")]), "9000");

    expect(early.observedRevision()).toBe(52);
    expect(late.observedRevision()).toBe(52);
  });

  it("moves to the list's revision for a deletion the watch missed while disconnected", () => {
    const watch = new WatchedEndpoints();
    watch.reset(
      list(
        ["slice-a", at(sliceFor("silo-0", "u0", "10.0.0.1"), "40")],
        ["slice-b", at(sliceFor("silo-1", "u1", "10.0.0.2"), "60")],
      ),
      "61",
    );
    // slice-b was deleted (at some revision this watcher never saw) while it was down.
    watch.reset(list(["slice-a", at(sliceFor("silo-0", "u0", "10.0.0.1"), "40")]), "75");
    expect(watch.observedRevision()).toBe(75);
  });

  it("reports a change it cannot date without a revision", () => {
    const watch = new WatchedEndpoints();
    const seen: Array<number | undefined> = [];
    watch.subscribe((_slices, revision) => seen.push(revision));
    watch.reset(list(["slice-a", at(sliceFor("silo-0", "u0", "10.0.0.1"), "40")]), "41");
    watch.apply("ADDED", "slice-b", sliceFor("silo-1", "u1", "10.0.0.2")); // no resourceVersion
    expect(seen).toEqual([40, undefined]);
  });
});
