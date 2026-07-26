// Ported from dotnet/orleans test/Orleans.Placement.Tests/PlacementFilterTests/SiloMetadataPlacementFilterTests.cs @ v10.1.0 (MIT).
import { defineGrainInterface } from "@thresh/core/grain-interface";
import type { SiloAddress } from "@thresh/core/silo-address";
import type { GrainWithIntegerKey } from "@thresh/core/key-kinds";

export interface IHostReportingGrain extends GrainWithIntegerKey {
  getHostingSilo(): Promise<SiloAddress>;
}

export const IUniqueRequiredMatchFilteredGrain = defineGrainInterface<IHostReportingGrain>(
  "UnitTests.PlacementFilterTests.IUniqueRequiredMatchFilteredGrain",
);
export type IUniqueRequiredMatchFilteredGrain = IHostReportingGrain;

export const IPreferredMatchFilteredGrain = defineGrainInterface<IHostReportingGrain>(
  "UnitTests.PlacementFilterTests.IPreferredMatchFilteredGrain",
);
export type IPreferredMatchFilteredGrain = IHostReportingGrain;

export const IPreferredMatchMin2FilteredGrain = defineGrainInterface<IHostReportingGrain>(
  "UnitTests.PlacementFilterTests.IPreferredMatchMin2FilteredGrain",
);
export type IPreferredMatchMin2FilteredGrain = IHostReportingGrain;

export const IPreferredMatchMultipleFilteredGrain = defineGrainInterface<IHostReportingGrain>(
  "UnitTests.PlacementFilterTests.IPreferredMatchMultipleFilteredGrain",
);
export type IPreferredMatchMultipleFilteredGrain = IHostReportingGrain;

export const IPreferredMatchNoMetadataFilteredGrain = defineGrainInterface<IHostReportingGrain>(
  "UnitTests.PlacementFilterTests.IPreferredMatchNoMetadataFilteredGrain",
);
export type IPreferredMatchNoMetadataFilteredGrain = IHostReportingGrain;
