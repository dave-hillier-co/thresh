// Ported from dotnet/orleans test/Orleans.DefaultCluster.Tests/ManagementGrainTests.cs @ v10.1.0 (MIT).
import { grain } from "@tsva/core/decorators";
import { Grain } from "@tsva/core/grain";
import {
  IDumbGrain,
  IDumbWorker,
} from "@tsva/parity/grains/interfaces/management-grain-interfaces";

export { IDumbGrain, IDumbWorker };

@grain({ name: "UnitTests.OrleansRuntime.DumbGrain" })
export class DumbGrain extends Grain implements IDumbGrain {
  async doNothing(): Promise<void> {}
}

@grain({ name: "UnitTests.OrleansRuntime.DumbWorker", stateless: true })
export class DumbWorker extends Grain implements IDumbWorker {
  async doNothing(): Promise<void> {}
}
