// Ported from dotnet/orleans test/Grains/TestGrains/SiloRoleBasedPlacementGrain.cs @ v10.1.0 (MIT).
//
// Upstream's `SiloRoleBasedPlacementDirector` reads the target role from the
// grain's own primary key at placement time, so one grain type serves any
// role a caller names. This framework's `siloRole` placement fixes the role
// at grain-type registration (`@grain({ placement: "siloRole", role })`), so a
// grain type has one role for its whole life. That is enough to port the
// "role nobody advertises" failure path (no `TestCluster` silo has ANY role
// metadata, so a fixed, made-up role always fails to resolve) but not the
// "role a silo actually advertises" success path, which additionally needs a
// way to configure a silo's advertised role that `TestCluster` does not
// expose (GAP-SILO-ROLE-CONFIG).
import { grain } from "@tsva/core/decorators";
import { Grain } from "@tsva/core/grain";
import { ISiloRoleBasedPlacementGrain } from "@tsva/parity/grains/interfaces/silo-role-based-placement-grain-interfaces";

export { ISiloRoleBasedPlacementGrain };

@grain({
  name: "UnitTests.Grains.SiloRoleBasedPlacementGrain",
  placement: "siloRole",
  role: "Sibyl.Silo",
})
export class SiloRoleBasedPlacementGrain extends Grain implements ISiloRoleBasedPlacementGrain {
  async ping(): Promise<boolean> {
    return true;
  }
}
