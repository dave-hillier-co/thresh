// Ported from dotnet/orleans test/Orleans.Placement.Tests/PlacementFilterTests/RequiredMatchSiloMetadataPlacementFilterDirectorTests.cs @ v10.1.0 (MIT).
//
// Unit-tests `RequiredMatchSiloMetadataPlacementFilterDirector` directly (no
// cluster). Upstream constructs the director from `ILocalSiloDetails` +
// `ISiloMetadataCache` test doubles; this framework's `PlacementFilter`
// interface already carries the equivalent information on the
// `PlacementContext` it is called with (`localSilo` + `siloMetadata`
// accessor), so the ported tests build that context directly instead of
// introducing parallel `ILocalSiloDetails`/`ISiloMetadataCache` abstractions.
import { describe, expect } from "vitest";
import { orleansTest } from "@thresh/testing/orleans-test";
import { SiloAddress } from "@thresh/core/silo-address";
import { RequiredMatchSiloMetadataPlacementFilterDirector } from "@thresh/runtime/placement/silo-metadata-match-filters";
import type { PlacementContext } from "@thresh/runtime/placement/placement-strategy";

const silo = (port: number) => new SiloAddress(`silo-${port}`, `uid-${port}`, `1.1.1.1:${port}`);

const contextOf = (
  localSilo: SiloAddress,
  metadata: Readonly<Record<string, Readonly<Record<string, string>>>>,
): PlacementContext => ({
  localSilo,
  siloMetadata: (s) => metadata[s.ringKey],
});

describe("UnitTests.PlacementFilterTests.RequiredMatchSiloMetadataPlacementFilterDirectorTests", () => {
  orleansTest(
    "UnitTests.PlacementFilterTests.RequiredMatchSiloMetadataPlacementFilterDirectorTests.RequiredMatchSiloMetadataPlacementFilterDirector_CanBeCreated",
    () => {
      const director = new RequiredMatchSiloMetadataPlacementFilterDirector([]);
      expect(director).toBeDefined();
    },
  );

  orleansTest(
    "UnitTests.PlacementFilterTests.RequiredMatchSiloMetadataPlacementFilterDirectorTests.RequiredMatchSiloMetadataPlacementFilterDirector_CanBeCalled",
    () => {
      const local = silo(1000);
      const director = new RequiredMatchSiloMetadataPlacementFilterDirector([]);
      const result = director.filter("Counter", [local], contextOf(local, { [local.ringKey]: {} }));
      expect(result).toBeDefined();
      expect(result.length).toBeGreaterThan(0);
    },
  );

  orleansTest(
    "UnitTests.PlacementFilterTests.RequiredMatchSiloMetadataPlacementFilterDirectorTests.RequiredMatchSiloMetadataPlacementFilterDirector_FiltersToNothingWhenNoEntry",
    () => {
      const local = silo(1000);
      const other = silo(1001);
      const director = new RequiredMatchSiloMetadataPlacementFilterDirector(["metadata.key"]);
      const result = director.filter(
        "Counter",
        [other],
        contextOf(local, { [other.ringKey]: {}, [local.ringKey]: { "metadata.key": "something" } }),
      );
      expect(result).toEqual([]);
    },
  );

  orleansTest(
    "UnitTests.PlacementFilterTests.RequiredMatchSiloMetadataPlacementFilterDirectorTests.RequiredMatchSiloMetadataPlacementFilterDirector_FiltersToNothingWhenDifferentValue",
    () => {
      const local = silo(1000);
      const other = silo(1001);
      const director = new RequiredMatchSiloMetadataPlacementFilterDirector(["metadata.key"]);
      const result = director.filter(
        "Counter",
        [other],
        contextOf(local, {
          [other.ringKey]: { "metadata.key": "other" },
          [local.ringKey]: { "metadata.key": "local" },
        }),
      );
      expect(result).toEqual([]);
    },
  );

  orleansTest(
    "UnitTests.PlacementFilterTests.RequiredMatchSiloMetadataPlacementFilterDirectorTests.RequiredMatchSiloMetadataPlacementFilterDirector_FiltersToSiloWhenMatching",
    () => {
      const local = silo(1000);
      const other = silo(1001);
      const director = new RequiredMatchSiloMetadataPlacementFilterDirector(["metadata.key"]);
      const result = director.filter(
        "Counter",
        [other],
        contextOf(local, {
          [other.ringKey]: { "metadata.key": "same" },
          [local.ringKey]: { "metadata.key": "same" },
        }),
      );
      expect(result.length).toBeGreaterThan(0);
    },
  );

  orleansTest(
    "UnitTests.PlacementFilterTests.RequiredMatchSiloMetadataPlacementFilterDirectorTests.RequiredMatchSiloMetadataPlacementFilterDirector_FiltersToMultipleSilosWhenMatching",
    () => {
      const local = silo(1000);
      const other1 = silo(1001);
      const other2 = silo(1002);
      const director = new RequiredMatchSiloMetadataPlacementFilterDirector(["metadata.key"]);
      const result = director.filter(
        "Counter",
        [other1, other2],
        contextOf(local, {
          [other1.ringKey]: { "metadata.key": "same" },
          [other2.ringKey]: { "metadata.key": "same" },
          [local.ringKey]: { "metadata.key": "same" },
        }),
      );
      expect(result.length).toBe(2);
    },
  );
});
