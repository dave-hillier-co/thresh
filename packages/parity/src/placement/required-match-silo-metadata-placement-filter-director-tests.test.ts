// Ported from dotnet/orleans test/Orleans.Placement.Tests/PlacementFilterTests/RequiredMatchSiloMetadataPlacementFilterDirectorTests.cs @ v10.1.0 (MIT).
//
// Unit-tests `RequiredMatchSiloMetadataPlacementFilterDirector` directly (no
// cluster): constructed from `ILocalSiloDetails` + `ISiloMetadataCache`, it
// filters candidate silos down to those whose metadata matches the *local*
// silo's own metadata for every key in a
// `RequiredMatchSiloMetadataPlacementFilterStrategy`, with no fallback (an
// empty result is valid). This is semantically close to this framework's
// `MetadataMatchFilter` (also an all-required-keys, no-fallback filter — see
// `@tsva/runtime/placement/metadata-match-filter` and its coverage in
// `placement.test.ts`), but the two are not the same construct:
// `MetadataMatchFilter` matches against a fixed, statically-configured
// required-value map, whereas the upstream director dynamically reads the
// *local* silo's own metadata value for each key at filter time via
// `ISiloMetadataCache`/`ILocalSiloDetails` — abstractions this framework does
// not have (GAP-PLACEMENT-FILTER-DIRECTORS, see
// `grain-placement-filter-tests.test.ts`).
import { describe } from "vitest";
import { orleansTest } from "@tsva/testing/orleans-test";

describe("UnitTests.PlacementFilterTests.RequiredMatchSiloMetadataPlacementFilterDirectorTests", () => {
  orleansTest.gap(
    "GAP-PLACEMENT-FILTER-DIRECTORS",
    "UnitTests.PlacementFilterTests.RequiredMatchSiloMetadataPlacementFilterDirectorTests.RequiredMatchSiloMetadataPlacementFilterDirector_CanBeCreated",
  );

  orleansTest.gap(
    "GAP-PLACEMENT-FILTER-DIRECTORS",
    "UnitTests.PlacementFilterTests.RequiredMatchSiloMetadataPlacementFilterDirectorTests.RequiredMatchSiloMetadataPlacementFilterDirector_CanBeCalled",
  );

  orleansTest.gap(
    "GAP-PLACEMENT-FILTER-DIRECTORS",
    "UnitTests.PlacementFilterTests.RequiredMatchSiloMetadataPlacementFilterDirectorTests.RequiredMatchSiloMetadataPlacementFilterDirector_FiltersToNothingWhenNoEntry",
  );

  orleansTest.gap(
    "GAP-PLACEMENT-FILTER-DIRECTORS",
    "UnitTests.PlacementFilterTests.RequiredMatchSiloMetadataPlacementFilterDirectorTests.RequiredMatchSiloMetadataPlacementFilterDirector_FiltersToNothingWhenDifferentValue",
  );

  orleansTest.gap(
    "GAP-PLACEMENT-FILTER-DIRECTORS",
    "UnitTests.PlacementFilterTests.RequiredMatchSiloMetadataPlacementFilterDirectorTests.RequiredMatchSiloMetadataPlacementFilterDirector_FiltersToSiloWhenMatching",
  );

  orleansTest.gap(
    "GAP-PLACEMENT-FILTER-DIRECTORS",
    "UnitTests.PlacementFilterTests.RequiredMatchSiloMetadataPlacementFilterDirectorTests.RequiredMatchSiloMetadataPlacementFilterDirector_FiltersToMultipleSilosWhenMatching",
  );
});
