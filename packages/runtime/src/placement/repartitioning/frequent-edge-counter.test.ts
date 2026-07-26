import { GrainId } from "@thresh/core/grain-id";
import { SiloAddress } from "@thresh/core/silo-address";
import { describe, expect, it } from "vitest";
import { edge, edgeVertex } from "./edge";
import { FrequentEdgeCounter } from "./frequent-edge-counter";

const zero = new SiloAddress("", "", "");
const grain = (key: string) => new GrainId("test", key);
const makeEdge = (from: string, to: string) =>
  edge(edgeVertex(grain(from), zero, true), edgeVertex(grain(to), zero, true));

describe("FrequentEdgeCounter", () => {
  it("increments the counter when a new edge is added", () => {
    const counter = new FrequentEdgeCounter(10);
    const e = makeEdge("A", "B");

    counter.add(e);

    const elements = counter.elements;
    expect(elements).toHaveLength(1);
    expect(elements[0]!.count).toBe(1);
    expect(elements[0]!.element).toBe(e);
  });

  it("updates the existing counter when the same edge is added again", () => {
    const counter = new FrequentEdgeCounter(10);
    const e = makeEdge("A", "B");

    counter.add(e);
    counter.add(e);

    const elements = counter.elements;
    expect(elements).toHaveLength(1);
    expect(elements[0]!.count).toBe(2);
  });

  it("evicts the minimum-count edge once capacity is reached", () => {
    const counter = new FrequentEdgeCounter(2);
    const edge1 = makeEdge("A", "B");
    const edge2 = makeEdge("C", "D");

    counter.add(edge1);
    counter.add(edge1);
    counter.add(edge2);

    expect(counter.count).toBe(2);

    const edge3 = makeEdge("E", "F");
    counter.add(edge3); // should evict edge2 (the minimum)

    const elements = counter.elements;
    expect(elements).toHaveLength(2);
    expect(elements.some((c) => c.element === edge2)).toBe(false);
  });

  it("removes a tracked edge", () => {
    const counter = new FrequentEdgeCounter(10);
    const e = makeEdge("A", "B");

    counter.add(e);
    counter.remove(e);

    expect(counter.elements).toHaveLength(0);
  });
});
