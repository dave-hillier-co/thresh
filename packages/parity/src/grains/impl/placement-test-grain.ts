// Ported from dotnet/orleans test/Grains/TestInternalGrains/PlacementTestGrain.cs @ v10.1.0 (MIT).
import { grain } from "@tsva/core/decorators";
import { Grain } from "@tsva/core/grain";
import type { Guid } from "@tsva/core/guid";
import {
  IPreferLocalPlacementTestGrain,
  IRandomPlacementTestGrain,
} from "@tsva/parity/grains/interfaces/placement-test-grain-interfaces";

export { IPreferLocalPlacementTestGrain, IRandomPlacementTestGrain };

@grain({ name: "UnitTests.Grains.RandomPlacementTestGrain", placement: "random" })
export class RandomPlacementTestGrain extends Grain implements IRandomPlacementTestGrain {
  async nop(): Promise<void> {}

  /**
   * Upstream calls `Nop()` on the target reference to force its activation
   * before returning. Called from within *this* activation, so
   * `PreferLocalPlacement` resolves against this grain's own host silo,
   * matching the "two hops" semantics the caller relies on.
   */
  async startPreferLocalGrain(key: Guid): Promise<Guid> {
    await this.getGrain(IPreferLocalPlacementTestGrain, key).nop();
    return key;
  }
}

@grain({ name: "UnitTests.Grains.PreferLocalPlacementTestGrain", placement: "preferLocal" })
export class PreferLocalPlacementTestGrain extends Grain implements IPreferLocalPlacementTestGrain {
  async nop(): Promise<void> {}

  async startPreferLocalGrain(key: Guid): Promise<Guid> {
    await this.getGrain(IPreferLocalPlacementTestGrain, key).nop();
    return key;
  }
}
