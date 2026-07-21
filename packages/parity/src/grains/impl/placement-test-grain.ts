// Ported from dotnet/orleans test/Grains/TestInternalGrains/PlacementTestGrain.cs @ v10.1.0 (MIT).
import { grain } from "@tsva/core/decorators";
import { Grain } from "@tsva/core/grain";
import { Guid } from "@tsva/core/guid";
import {
  IActivationCountBasedPlacementTestGrain,
  IOtherStatelessWorkerPlacementTestGrain,
  IPreferLocalPlacementTestGrain,
  IRandomPlacementTestGrain,
  IStatelessWorkerPlacementTestGrain,
  type IPlacementTestGrain,
} from "@tsva/parity/grains/interfaces/placement-test-grain-interfaces";

export {
  IActivationCountBasedPlacementTestGrain,
  IOtherStatelessWorkerPlacementTestGrain,
  IPreferLocalPlacementTestGrain,
  IRandomPlacementTestGrain,
  IStatelessWorkerPlacementTestGrain,
};

/**
 * Ported from upstream's shared `PlacementTestGrainBase`: the members common
 * to every placement-test grain, including the silo-scoped load-shedding test
 * hooks (`GrainRuntime.enableOverloadDetection`/`latchCpuUsage`/etc.) — they
 * act on whichever silo hosts THIS activation, matching upstream's
 * `OverloadDetector`/`TestHooksEnvironmentStatisticsProvider` being resolved
 * from this activation's own `IServiceProvider`.
 */
abstract class PlacementTestGrainBase extends Grain implements IPlacementTestGrain {
  /** Upstream's `_id = Guid.NewGuid().ToString()` field initializer. */
  private readonly activationGuid = Guid.newGuid().toString();

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

  async getRuntimeInstanceId(): Promise<string> {
    return this.runtime.localSiloAddress().toString();
  }

  async getActivationId(): Promise<string> {
    return this.activationGuid;
  }

  async startLocalGrains(keys: Guid[]): Promise<void> {
    // We call Nop() on the grain references to ensure that they're
    // instantiated before the promise is delivered.
    await Promise.all(
      keys.map((key) => this.getGrain(IStatelessWorkerPlacementTestGrain, key).nop()),
    );
  }

  async sampleLocalGrainEndpoint(key: Guid, sampleSize: number): Promise<string[]> {
    const grain = this.getGrain(IStatelessWorkerPlacementTestGrain, key);
    const samples: string[] = [];
    for (let i = 0; i < sampleSize; i += 1) {
      samples.push(await grain.getRuntimeInstanceId());
    }
    return samples;
  }

  async enableOverloadDetection(enabled: boolean): Promise<void> {
    this.runtime.enableOverloadDetection(enabled);
  }

  async latchOverloaded(): Promise<void> {
    await this.runtime.latchOverloaded();
  }

  async unlatchOverloaded(): Promise<void> {
    await this.runtime.unlatchOverloaded();
  }

  async latchCpuUsage(value: number): Promise<void> {
    await this.runtime.latchCpuUsage(value);
  }

  async unlatchCpuUsage(): Promise<void> {
    await this.runtime.unlatchCpuUsage();
  }
}

@grain({ name: "UnitTests.Grains.RandomPlacementTestGrain", placement: "random" })
export class RandomPlacementTestGrain
  extends PlacementTestGrainBase
  implements IRandomPlacementTestGrain {}

/**
 * Ported from `ActivationCountBasedPlacementTestGrain`
 * (`PlacementTestGrainBase` + `[ActivationCountBasedPlacement]`): placed by
 * this port's load-aware `ActivationCountPlacement` strategy
 * (`placement: "activationCount"`).
 */
@grain({
  name: "UnitTests.Grains.ActivationCountBasedPlacementTestGrain",
  placement: "activationCount",
})
export class ActivationCountBasedPlacementTestGrain
  extends PlacementTestGrainBase
  implements IActivationCountBasedPlacementTestGrain {}

@grain({ name: "UnitTests.Grains.PreferLocalPlacementTestGrain", placement: "preferLocal" })
export class PreferLocalPlacementTestGrain
  extends PlacementTestGrainBase
  implements IPreferLocalPlacementTestGrain {}

/** Ported from `StatelessWorkerPlacementTestGrain`: `[StatelessWorker(1)]`. */
@grain({
  name: "UnitTests.Grains.StatelessWorkerPlacementTestGrain",
  stateless: true,
  maxLocalWorkers: 1,
})
export class StatelessWorkerPlacementTestGrain
  extends PlacementTestGrainBase
  implements IStatelessWorkerPlacementTestGrain {}

/** Ported from `OtherStatelessWorkerPlacementTestGrain`: `[StatelessWorker(2)]`. */
@grain({
  name: "UnitTests.Grains.OtherStatelessWorkerPlacementTestGrain",
  stateless: true,
  maxLocalWorkers: 2,
})
export class OtherStatelessWorkerPlacementTestGrain
  extends PlacementTestGrainBase
  implements IOtherStatelessWorkerPlacementTestGrain {}
