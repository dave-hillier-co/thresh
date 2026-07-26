// Ported from dotnet/orleans test/Orleans.Placement.Tests/PlacementFilterTests/PreferredMatchSiloMetadataPlacementFilterDirectorTests.cs @ v10.1.0 (MIT).
//
// Unit-tests `PreferredMatchSiloMetadataPlacementFilterDirector` directly (no
// cluster). Upstream constructs the director from `ILocalSiloDetails` +
// `ISiloMetadataCache` test doubles; this framework's `PlacementFilter`
// interface already carries the equivalent information on the
// `PlacementContext` it is called with (`localSilo` + `siloMetadata`
// accessor), so the ported tests build that context directly instead of
// introducing parallel `ILocalSiloDetails`/`ISiloMetadataCache` abstractions.
import { describe, expect } from "vitest";
import { orleansTest } from "@thresh/testing/orleans-test";
import { SiloAddress } from "@thresh/core/silo-address";
import { PreferredMatchSiloMetadataPlacementFilterDirector } from "@thresh/runtime/placement/silo-metadata-match-filters";
import type { PlacementContext } from "@thresh/runtime/placement/placement-strategy";

const silo = (port: number) => new SiloAddress(`silo-${port}`, `uid-${port}`, `1.1.1.1:${port}`);

const contextOf = (
  localSilo: SiloAddress,
  metadata: Readonly<Record<string, Readonly<Record<string, string>>>>,
): PlacementContext => ({
  localSilo,
  siloMetadata: (s) => metadata[s.ringKey],
});

describe("UnitTests.PlacementFilterTests.PreferredMatchSiloMetadataPlacementFilterDirectorTests", () => {
  orleansTest(
    "UnitTests.PlacementFilterTests.PreferredMatchSiloMetadataPlacementFilterDirectorTests.CanBeCreated",
    () => {
      const director = new PreferredMatchSiloMetadataPlacementFilterDirector([], 1);
      expect(director).toBeDefined();
    },
  );

  orleansTest(
    "UnitTests.PlacementFilterTests.PreferredMatchSiloMetadataPlacementFilterDirectorTests.CanBeCalled",
    () => {
      const local = silo(1000);
      const director = new PreferredMatchSiloMetadataPlacementFilterDirector([], 2);
      const result = director.filter("Counter", [local], contextOf(local, { [local.ringKey]: {} }));
      expect(result).toBeDefined();
      expect(result.length).toBeGreaterThan(0);
    },
  );

  orleansTest(
    "UnitTests.PlacementFilterTests.PreferredMatchSiloMetadataPlacementFilterDirectorTests.FiltersToAllWhenNoEntry",
    () => {
      const local = silo(1000);
      const other = silo(1001);
      const director = new PreferredMatchSiloMetadataPlacementFilterDirector(["metadata.key"], 1);
      const result = director.filter(
        "Counter",
        [other],
        contextOf(local, { [other.ringKey]: {}, [local.ringKey]: { "metadata.key": "something" } }),
      );
      expect(result.length).toBeGreaterThan(0);
    },
  );

  const singleMetadataCases: ReadonlyArray<[number, number, string]> = [
    [1, 3, "no.match"],
    [2, 3, "no.match"],
    [1, 1, "one.match"],
    [2, 3, "one.match"],
    [1, 2, "two.match"],
    [2, 2, "two.match"],
    [3, 3, "two.match"],
    [1, 3, "all.match"],
    [2, 3, "all.match"],
  ];

  orleansTest.each(singleMetadataCases)(
    "UnitTests.PlacementFilterTests.PreferredMatchSiloMetadataPlacementFilterDirectorTests.FiltersOnSingleMetadata",
    ([minCandidates, expectedCount, key]) => {
      const local = silo(1000);
      const other1 = silo(1001);
      const other2 = silo(1002);
      const other3 = silo(1003);
      const localMeta = {
        "all.match": "match",
        "one.match": "match",
        "two.match": "match",
        "no.match": "match",
      };
      const other1Meta = {
        "all.match": "match",
        "one.match": "match",
        "two.match": "match",
        "no.match": "nomatch",
      };
      const other2Meta = {
        "all.match": "match",
        "one.match": "nomatch",
        "two.match": "match",
        "no.match": "nomatch",
      };
      const other3Meta = {
        "all.match": "match",
        "one.match": "nomatch",
        "two.match": "nomatch",
        "no.match": "nomatch",
      };
      const director = new PreferredMatchSiloMetadataPlacementFilterDirector([key], minCandidates);
      const result = director.filter(
        "Counter",
        [other1, other2, other3],
        contextOf(local, {
          [other1.ringKey]: other1Meta,
          [other2.ringKey]: other2Meta,
          [other3.ringKey]: other3Meta,
          [local.ringKey]: localMeta,
        }),
      );
      expect(result.length).toBeGreaterThan(0);
      expect(result.length).toBe(expectedCount);
    },
  );

  const multipleMetadataCases: ReadonlyArray<[number, number, readonly string[]]> = [
    [1, 3, ["no.match", "all.match"]],
    [1, 1, ["no.match", "one.match", "two.match"]],
    [2, 2, ["no.match", "one.match", "two.match"]],
    [3, 3, ["no.match", "one.match", "two.match"]],
    [1, 3, ["all.match", "no.match"]],
    [1, 1, ["one.match", "all.match"]],
    [2, 3, ["one.match", "all.match"]],
    [1, 1, ["no.match", "one.match", "all.match"]],
    [2, 3, ["no.match", "one.match", "all.match"]],
  ];

  orleansTest.each(multipleMetadataCases)(
    "UnitTests.PlacementFilterTests.PreferredMatchSiloMetadataPlacementFilterDirectorTests.FiltersOnMultipleMetadata",
    ([minCandidates, expectedCount, keys]) => {
      const local = silo(1000);
      const other1 = silo(1001);
      const other2 = silo(1002);
      const other3 = silo(1003);
      const localMeta = {
        "all.match": "match",
        "one.match": "match",
        "two.match": "match",
        "no.match": "match",
      };
      const other1Meta = {
        "all.match": "match",
        "one.match": "match",
        "two.match": "match",
        "no.match": "not.match",
      };
      const other2Meta = {
        "all.match": "match",
        "one.match": "nomatch",
        "two.match": "match",
        "no.match": "not.match",
      };
      const other3Meta = {
        "all.match": "match",
        "one.match": "not.match",
        "two.match": "not.match",
        "no.match": "not.match",
      };
      const director = new PreferredMatchSiloMetadataPlacementFilterDirector(keys, minCandidates);
      const result = director.filter(
        "Counter",
        [other1, other2, other3],
        contextOf(local, {
          [other1.ringKey]: other1Meta,
          [other2.ringKey]: other2Meta,
          [other3.ringKey]: other3Meta,
          [local.ringKey]: localMeta,
        }),
      );
      expect(result.length).toBeGreaterThan(0);
      expect(result.length).toBe(expectedCount);
    },
  );
});
