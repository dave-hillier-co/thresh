// Ported from dotnet/orleans test/Orleans.DefaultCluster.Tests/Migration/MigrationTests.cs @ v10.1.0 (MIT).
import { grain, persistentState } from "@thresh/core/decorators";
import { Grain } from "@thresh/core/grain";
import type {
  DehydrationContext,
  IGrainMigrationParticipant,
  RehydrationContext,
} from "@thresh/core/grain-migration-participant";
import type { DeactivationReason } from "@thresh/core/reasons";
import type { PersistentState } from "@thresh/core/persistent-state";
import { RequestContext } from "@thresh/core/request-context";
import type { SiloAddress } from "@thresh/core/silo-address";
import {
  IMigrationTestGrain,
  IMigrationTestGrainGrainOfT,
  IMigrationTestGrainIPersistentStateOfT,
} from "@thresh/parity/grains/interfaces/migration-test-grain-interfaces";

export { IMigrationTestGrain, IMigrationTestGrainGrainOfT, IMigrationTestGrainIPersistentStateOfT };

// Upstream: [RandomPlacement] — used to test undirected migration.
@grain({
  name: "DefaultCluster.Tests.General.MigrationTestGrain",
  placement: "random",
  collectionAgeSeconds: 1,
})
export class MigrationTestGrain
  extends Grain
  implements IMigrationTestGrain, IGrainMigrationParticipant
{
  private state = 0;
  private failRehydrateNext = false;
  private migrateTargetOnDeactivate: SiloAddress | undefined;

  async setState(state: number): Promise<void> {
    this.state = state;
  }

  async getState(): Promise<number> {
    return this.state;
  }

  async migrateOnIdle(target?: SiloAddress): Promise<void> {
    this.runtime.migrateOnIdle(target);
  }

  async failNextRehydrate(): Promise<void> {
    this.failRehydrateNext = true;
  }

  async migrateDuringDeactivation(target: SiloAddress): Promise<void> {
    this.migrateTargetOnDeactivate = target;
    this.runtime.deactivateOnIdle();
  }

  override async onDeactivate(reason: DeactivationReason): Promise<void> {
    if (this.migrateTargetOnDeactivate !== undefined) {
      const target = this.migrateTargetOnDeactivate;
      this.migrateTargetOnDeactivate = undefined;
      this.runtime.migrateOnIdle(target);
    }
    await super.onDeactivate(reason);
  }

  onDehydrate(context: DehydrationContext): void {
    if (this.failRehydrateNext) {
      context.set("failRehydrate", true);
    }
    // Ambient `RequestContext` flag set by the caller before `migrateOnIdle()`
    // (Orleans `RequestContext.Set("fail_dehydrate", true)`), carried through
    // to this later call by `ActivationData.requestMigration`'s captured
    // `migrationRequestContext`. Thrown before `state` is recorded, same as
    // upstream — the target reactivates fresh, without this grain's state.
    if (RequestContext.get("fail_dehydrate") === "true") {
      throw new Error("Failing to dehydrate on-command");
    }
    context.set("state", this.state);
  }

  onRehydrate(context: RehydrationContext): void {
    if (context.get<boolean>("failRehydrate") === true) {
      throw new Error("Failing to rehydrate on-command");
    }
    const state = context.get<number>("state");
    if (state !== undefined) this.state = state;
  }
}

interface GrainOfTState {
  value: number;
}

@grain({
  name: "DefaultCluster.Tests.General.MigrationTestGrainWithMemoryStorage",
  collectionAgeSeconds: 1,
})
export class MigrationTestGrainWithMemoryStorage
  extends Grain
  implements IMigrationTestGrainGrainOfT
{
  @persistentState("value", { defaultValue: (): GrainOfTState => ({ value: 0 }) })
  private state!: PersistentState<GrainOfTState>;

  async setState(state: number): Promise<void> {
    this.state.value.value = state;
  }

  async getState(): Promise<number> {
    return this.state.value.value;
  }

  async migrateOnIdle(target?: SiloAddress): Promise<void> {
    this.runtime.migrateOnIdle(target);
  }
}

interface PersistentPartState {
  value: number;
}

@grain({
  name: "DefaultCluster.Tests.General.MigrationTestGrainWithInjectedMemoryStorage",
  collectionAgeSeconds: 1,
})
export class MigrationTestGrainWithInjectedMemoryStorage
  extends Grain
  implements IMigrationTestGrainIPersistentStateOfT
{
  @persistentState("a", { defaultValue: (): PersistentPartState => ({ value: 0 }) })
  private stateA!: PersistentState<PersistentPartState>;

  @persistentState("b", { defaultValue: (): PersistentPartState => ({ value: 0 }) })
  private stateB!: PersistentState<PersistentPartState>;

  async setState(a: number, b: number): Promise<void> {
    this.stateA.value.value = a;
    this.stateB.value.value = b;
  }

  async getState(): Promise<[number, number]> {
    return [this.stateA.value.value, this.stateB.value.value];
  }

  async migrateOnIdle(target?: SiloAddress): Promise<void> {
    this.runtime.migrateOnIdle(target);
  }
}
