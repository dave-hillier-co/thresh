// Ported from dotnet/orleans test/Orleans.Placement.Tests/ActivationRebalancingTests/RebalancingTestBase.cs @ v10.1.0 (MIT).
import { grain } from "@tsva/core/decorators";
import { Grain } from "@tsva/core/grain";
import { IRebalancingTestGrain } from "@tsva/parity/grains/interfaces/rebalancing-test-grain-interfaces";

export { IRebalancingTestGrain };

@grain({ name: "UnitTests.ActivationRebalancingTests.RebalancingTestGrain" })
export class RebalancingTestGrain extends Grain implements IRebalancingTestGrain {
  async ping(): Promise<void> {}
}
