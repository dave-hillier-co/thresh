// Ported from dotnet/orleans test/Orleans.Placement.Tests/PlacementFilterTests/SiloMetadataPlacementFilterTests.cs @ v10.1.0 (MIT).
import { grain } from "@thresh/core/decorators";
import { Grain } from "@thresh/core/grain";
import type { SiloAddress } from "@thresh/core/silo-address";
import {
  IPreferredMatchFilteredGrain,
  IPreferredMatchMin2FilteredGrain,
  IPreferredMatchMultipleFilteredGrain,
  IPreferredMatchNoMetadataFilteredGrain,
  IUniqueRequiredMatchFilteredGrain,
} from "@thresh/parity/grains/interfaces/silo-metadata-placement-filter-test-grain-interfaces";

export {
  IPreferredMatchFilteredGrain,
  IPreferredMatchMin2FilteredGrain,
  IPreferredMatchMultipleFilteredGrain,
  IPreferredMatchNoMetadataFilteredGrain,
  IUniqueRequiredMatchFilteredGrain,
};

@grain({
  name: "UnitTests.PlacementFilterTests.UniqueRequiredMatchFilteredGrain",
  placementFilters: [{ kind: "requiredMatchSiloMetadata", keys: ["unique"] }],
})
export class UniqueRequiredMatchFilteredGrain
  extends Grain
  implements IUniqueRequiredMatchFilteredGrain
{
  async getHostingSilo(): Promise<SiloAddress> {
    return this.runtime.localSiloAddress();
  }
}

@grain({
  name: "UnitTests.PlacementFilterTests.PreferredMatchFilteredGrain",
  placementFilters: [
    { kind: "preferredMatchSiloMetadata", keys: ["unique"], minCandidates: 1 },
  ],
})
export class PreferredMatchFilteredGrain extends Grain implements IPreferredMatchFilteredGrain {
  async getHostingSilo(): Promise<SiloAddress> {
    return this.runtime.localSiloAddress();
  }
}

@grain({
  name: "UnitTests.PlacementFilterTests.PreferredMatchMinTwoFilteredGrain",
  placementFilters: [{ kind: "preferredMatchSiloMetadata", keys: ["unique"] }],
})
export class PreferredMatchMinTwoFilteredGrain
  extends Grain
  implements IPreferredMatchMin2FilteredGrain
{
  async getHostingSilo(): Promise<SiloAddress> {
    return this.runtime.localSiloAddress();
  }
}

@grain({
  name: "UnitTests.PlacementFilterTests.PreferredMatchMultipleFilteredGrain",
  placementFilters: [
    { kind: "preferredMatchSiloMetadata", keys: ["unique", "other"], minCandidates: 2 },
  ],
})
export class PreferredMatchMultipleFilteredGrain
  extends Grain
  implements IPreferredMatchMultipleFilteredGrain
{
  async getHostingSilo(): Promise<SiloAddress> {
    return this.runtime.localSiloAddress();
  }
}

@grain({
  name: "UnitTests.PlacementFilterTests.PreferredMatchNoMetadataFilteredGrain",
  placementFilters: [{ kind: "preferredMatchSiloMetadata", keys: ["not.there"] }],
})
export class PreferredMatchNoMetadataFilteredGrain
  extends Grain
  implements IPreferredMatchNoMetadataFilteredGrain
{
  async getHostingSilo(): Promise<SiloAddress> {
    return this.runtime.localSiloAddress();
  }
}
