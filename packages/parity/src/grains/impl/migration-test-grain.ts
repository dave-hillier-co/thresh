// Ported from dotnet/orleans test/Orleans.DefaultCluster.Tests/Migration/MigrationTests.cs @ v10.1.0 (MIT).
import { grain, persistentState } from "@tsva/core/decorators";
import { Grain } from "@tsva/core/grain";
import type {
  DehydrationContext,
  IGrainMigrationParticipant,
  RehydrationContext,
} from "@tsva/core/grain-migration-participant";
import type { DeactivationReason } from "@tsva/core/reasons";
import type { PersistentState } from "@tsva/core/persistent-state";
import type { SiloAddress } from "@tsva/core/silo-address";
import {
  IMigrationTestGrain,
  IMigrationTestGrainGrainOfT,
  IMigrationTestGrainIPersistentStateOfT,
} from "@tsva/parity/grains/interfaces/migration-test-grain-interfaces";

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
