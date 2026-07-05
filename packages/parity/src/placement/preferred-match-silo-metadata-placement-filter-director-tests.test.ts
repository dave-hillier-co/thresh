// Ported from dotnet/orleans test/Orleans.Placement.Tests/PlacementFilterTests/PreferredMatchSiloMetadataPlacementFilterDirectorTests.cs @ v10.1.0 (MIT).
//
// Unit-tests `PreferredMatchSiloMetadataPlacementFilterDirector` directly
// (no cluster): it is constructed from `ILocalSiloDetails` +
// `ISiloMetadataCache` and, given a `PreferredMatchSiloMetadataPlacementFilterStrategy`
// (a list of metadata keys plus a `minDesiredCandidates` count), filters a
// candidate silo list down to those whose metadata matches the *local*
// silo's own metadata for every listed key — but only if doing so leaves at
// least `minDesiredCandidates` silos; otherwise it falls back to returning
// every candidate unfiltered. This framework's placement-filter mechanism
// has no equivalent: `MetadataMatchFilter` (see
// `@tsva/runtime/placement/metadata-match-filter`) matches against a fixed,
// statically-configured required-value map (not the local silo's own
// dynamically-read metadata), has no `ISiloMetadataCache`/`ILocalSiloDetails`
// abstractions, and has no minimum-desired-candidates fallback logic at all
// (GAP-PLACEMENT-FILTER-DIRECTORS, see `grain-placement-filter-tests.test.ts`).
import { describe } from "vitest";
import { orleansTest } from "@tsva/testing/orleans-test";

describe("UnitTests.PlacementFilterTests.PreferredMatchSiloMetadataPlacementFilterDirectorTests", () => {
  orleansTest.gap(
    "GAP-PLACEMENT-FILTER-DIRECTORS",
    "UnitTests.PlacementFilterTests.PreferredMatchSiloMetadataPlacementFilterDirectorTests.CanBeCreated",
  );

  orleansTest.gap(
    "GAP-PLACEMENT-FILTER-DIRECTORS",
    "UnitTests.PlacementFilterTests.PreferredMatchSiloMetadataPlacementFilterDirectorTests.CanBeCalled",
  );

  orleansTest.gap(
    "GAP-PLACEMENT-FILTER-DIRECTORS",
    "UnitTests.PlacementFilterTests.PreferredMatchSiloMetadataPlacementFilterDirectorTests.FiltersToAllWhenNoEntry",
  );

  orleansTest.gap(
    "GAP-PLACEMENT-FILTER-DIRECTORS",
    "UnitTests.PlacementFilterTests.PreferredMatchSiloMetadataPlacementFilterDirectorTests.FiltersOnSingleMetadata",
  );

  orleansTest.gap(
    "GAP-PLACEMENT-FILTER-DIRECTORS",
    "UnitTests.PlacementFilterTests.PreferredMatchSiloMetadataPlacementFilterDirectorTests.FiltersOnMultipleMetadata",
  );
});
