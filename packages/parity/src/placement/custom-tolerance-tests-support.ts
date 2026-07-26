// Ported from dotnet/orleans test/Orleans.Placement.Tests/ActivationRepartitioningTests/CustomToleranceTests.cs
// (the nested grain interfaces/classes) @ v10.1.0 (MIT).
import { grain } from "@thresh/core/decorators";
import { Grain } from "@thresh/core/grain";
import { defineGrainInterface } from "@thresh/core/grain-interface";
import type { GrainWithIntegerKey } from "@thresh/core/key-kinds";
import { PLACEMENT_HINT_KEY, RequestContext } from "@thresh/core/request-context";
import type { SiloAddress } from "@thresh/core/silo-address";

export interface IE extends GrainWithIntegerKey {
  firstPing(silo2: string): Promise<void>;
  ping(): Promise<void>;
  getAddress(): Promise<SiloAddress>;
}
export const IE = defineGrainInterface<IE>(
  "UnitTests.ActivationRepartitioningTests.CustomToleranceTests.IE",
);

export interface IF extends GrainWithIntegerKey {
  ping(): Promise<void>;
  getAddress(): Promise<SiloAddress>;
}
export const IF = defineGrainInterface<IF>(
  "UnitTests.ActivationRepartitioningTests.CustomToleranceTests.IF",
);

export interface IX extends GrainWithIntegerKey {
  ping(): Promise<void>;
  getAddress(): Promise<SiloAddress>;
}
export const IX = defineGrainInterface<IX>(
  "UnitTests.ActivationRepartitioningTests.CustomToleranceTests.IX",
);

@grain({ name: "UnitTests.ActivationRepartitioningTests.CustomToleranceTests.E" })
export class E extends Grain implements IE {
  async firstPing(silo2: string): Promise<void> {
    RequestContext.set(PLACEMENT_HINT_KEY, silo2);
    await this.getGrain(IF, this.id.key as bigint).ping();
  }

  async ping(): Promise<void> {
    await this.getGrain(IF, this.id.key as bigint).ping();
  }

  async getAddress(): Promise<SiloAddress> {
    return this.runtime.localSiloAddress();
  }
}

@grain({ name: "UnitTests.ActivationRepartitioningTests.CustomToleranceTests.F" })
export class F extends Grain implements IF {
  async ping(): Promise<void> {
    return;
  }

  async getAddress(): Promise<SiloAddress> {
    return this.runtime.localSiloAddress();
  }
}

/**
 * Achieves initial balance between the 2 silos, as by default the primary
 * would otherwise have one more baseline activation than the secondary
 * (Orleans' `sys.svc.clustering.dev`; this port has no equivalent built-in
 * system-target activation, but `X` is kept — and still Immovable — for
 * fidelity with the upstream fixture and comments).
 */
@grain({
  name: "UnitTests.ActivationRepartitioningTests.CustomToleranceTests.X",
  immovable: "repartitioner",
})
export class X extends Grain implements IX {
  async ping(): Promise<void> {
    return;
  }

  async getAddress(): Promise<SiloAddress> {
    return this.runtime.localSiloAddress();
  }
}
