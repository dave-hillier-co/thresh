// Ported from dotnet/orleans test/Orleans.Placement.Tests/ActivationRepartitioningTests/FrequentEdgeCounterTests.cs @ v10.1.0 (MIT).
//
// Exercises `FrequentEdgeCounter`, the bounded-capacity communication-graph
// edge counter behind the repartitioner: incrementing an existing edge,
// evicting the minimum-count edge once capacity is reached, and explicit
// removal. Ported onto
// `@thresh/runtime/placement/repartitioning/frequent-edge-counter`
// (`FrequentEdgeCounter`, built on the generic `FrequentItemCollection`
// "Filtered Space-Saving" sketch), a faithful port of the top-K
// insert/evict/remove algorithm.
import { GrainId } from "@thresh/core/grain-id";
import { SiloAddress } from "@thresh/core/silo-address";
import { edge, edgeVertex } from "@thresh/runtime/placement/repartitioning/edge";
import { FrequentEdgeCounter } from "@thresh/runtime/placement/repartitioning/frequent-edge-counter";
import { describe, expect } from "vitest";
import { orleansTest } from "@thresh/testing/orleans-test";

const zero = new SiloAddress("", "", "");
const idA = new GrainId("A", "id-a");
const idB = new GrainId("B", "id-b");
const idC = new GrainId("C", "id-c");
const idD = new GrainId("D", "id-d");
const idE = new GrainId("E", "id-e");
const idF = new GrainId("F", "id-f");

const makeEdge = (source: GrainId, target: GrainId) =>
  edge(edgeVertex(source, zero, true), edgeVertex(target, zero, true));

describe("UnitTests.ActivationRepartitioningTests.FrequentEdgeCounterTests", () => {
  orleansTest(
    "UnitTests.ActivationRepartitioningTests.FrequentEdgeCounterTests.Add_ShouldIncrementCounter_WhenEdgeIsAdded",
    () => {
      const sink = new FrequentEdgeCounter(10);
      const testEdge = makeEdge(idA, idB);

      sink.add(testEdge);

      const counters = sink.elements;

      expect(counters).toHaveLength(1);
      expect(counters[0]!.count).toBe(1);
      expect(counters[0]!.element).toBe(testEdge);
    },
  );

  orleansTest(
    "UnitTests.ActivationRepartitioningTests.FrequentEdgeCounterTests.Add_ShouldUpdateExistingCounter_WhenSameEdgeIsAddedAgain",
    () => {
      const sink = new FrequentEdgeCounter(10);
      const testEdge = makeEdge(idA, idB);

      sink.add(testEdge);
      sink.add(testEdge);

      const counters = sink.elements;

      expect(counters).toHaveLength(1);
      expect(counters[0]!.count).toBe(2);
      expect(counters[0]!.element).toBe(testEdge);
    },
  );

  orleansTest(
    "UnitTests.ActivationRepartitioningTests.FrequentEdgeCounterTests.Add_ShouldRemoveMinCounter_WhenCapacityIsReached",
    () => {
      const sink = new FrequentEdgeCounter(2);

      const edge1 = makeEdge(idA, idB);
      const edge2 = makeEdge(idC, idD);

      sink.add(edge1);
      sink.add(edge1);
      sink.add(edge2);

      expect(sink.count).toBe(2);

      const edge3 = makeEdge(idE, idF);
      sink.add(edge3); // should remove the minimum counter (edge2) since capacity is 2

      const counters = sink.elements;

      expect(counters).toHaveLength(2);
      expect(counters.some((c) => c.element === edge2)).toBe(false);
    },
  );

  orleansTest(
    "UnitTests.ActivationRepartitioningTests.FrequentEdgeCounterTests.Remove_ShouldRemoveCounter_WhenEdgeIsRemoved",
    () => {
      const sink = new FrequentEdgeCounter(10);
      const testEdge = makeEdge(idA, idB);

      sink.add(testEdge);
      sink.remove(testEdge);

      expect(sink.elements).toHaveLength(0);
    },
  );
});
