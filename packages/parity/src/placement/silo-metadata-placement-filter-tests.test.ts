// Ported from dotnet/orleans test/Orleans.Placement.Tests/PlacementFilterTests/SiloMetadataPlacementFilterTests.cs @ v10.1.0 (MIT).
//
// Deploys a 3-silo cluster via `ISiloBuilder.UseSiloMetadata(...)` (each silo
// advertising a distinct "unique" metadata value) and exercises grains
// decorated with `[RequiredMatchSiloMetadataPlacementFilter]` and
// `[PreferredMatchSiloMetadataPlacementFilter(keys, minDesiredCandidates)]`
// stacked with `[RandomPlacement]`, asserting: a required-match filter always
// places on the calling silo (the only match); a preferred-match filter with
// enough matching candidates does too; and a preferred-match filter whose
// `minDesiredCandidates` cannot be satisfied by real matches (1 vs. 2, or
// vs. multiple required keys) instead falls back to spreading placement
// across *all* silos. This framework's placement-filter mechanism only
// implements a fixed-value, all-or-nothing `MetadataMatchFilter` (see
// `@tsva/runtime/placement/metadata-match-filter`): no `UseSiloMetadata`
// cluster builder API, no attribute-declared filters, and critically no
// "preferred match with minimum-desired-candidates fallback" semantics at all
// (GAP-PLACEMENT-FILTER-DIRECTORS).
import { describe } from "vitest";
import { orleansTest } from "@tsva/testing/orleans-test";

describe("UnitTests.PlacementFilterTests.SiloMetadataPlacementFilterTests", () => {
  orleansTest.gap(
    "GAP-PLACEMENT-FILTER-DIRECTORS",
    "UnitTests.PlacementFilterTests.SiloMetadataPlacementFilterTests.PlacementFilter_GrainWithoutFilterCanBeCalled",
  );

  orleansTest.gap(
    "GAP-PLACEMENT-FILTER-DIRECTORS",
    "UnitTests.PlacementFilterTests.SiloMetadataPlacementFilterTests.PlacementFilter_RequiredFilterCanBeCalled",
  );

  orleansTest.gap(
    "GAP-PLACEMENT-FILTER-DIRECTORS",
    "UnitTests.PlacementFilterTests.SiloMetadataPlacementFilterTests.PlacementFilter_PreferredFilterCanBeCalled",
  );

  orleansTest.gap(
    "GAP-PLACEMENT-FILTER-DIRECTORS",
    "UnitTests.PlacementFilterTests.SiloMetadataPlacementFilterTests.PlacementFilter_PreferredMin2FilterCanBeCalled",
  );

  orleansTest.gap(
    "GAP-PLACEMENT-FILTER-DIRECTORS",
    "UnitTests.PlacementFilterTests.SiloMetadataPlacementFilterTests.PlacementFilter_PreferredMultipleFilterCanBeCalled",
  );

  orleansTest.gap(
    "GAP-PLACEMENT-FILTER-DIRECTORS",
    "UnitTests.PlacementFilterTests.SiloMetadataPlacementFilterTests.PlacementFilter_PreferredMin2FilterCanBeCalledWithLargerCluster",
  );

  orleansTest.gap(
    "GAP-PLACEMENT-FILTER-DIRECTORS",
    "UnitTests.PlacementFilterTests.SiloMetadataPlacementFilterTests.PlacementFilter_PreferredNoMetadataFilterCanBeCalled",
  );
});
