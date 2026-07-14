// Ported from dotnet/orleans test/Orleans.Runtime.Tests/ActivationTracingTests.cs
// @ v10.1.0 (MIT) — `SimpleMigrationTracingTestGrain` (the one grain fixture
// among that file's migration-tracing grains this port doesn't already have
// an equivalent of): a grain with `migrateOnIdle` but NO migration
// participant at all (no `IGrainMigrationParticipant`, no persistent state —
// this port's `PersistentStateImpl` always implements the migration hooks,
// see `migration-test-grain.ts`'s `MigrationTestGrainWithMemoryStorage`, so a
// *truly* participant-free grain needs plain instance state instead), so the
// dehydrate/rehydrate spans' negative case is genuinely testable.
import { grain } from "@tsva/core/decorators";
import { Grain } from "@tsva/core/grain";
import type { SiloAddress } from "@tsva/core/silo-address";
import { ISimpleMigrationTracingGrain } from "@tsva/parity/grains/interfaces/migration-tracing-grain-interfaces";

export { ISimpleMigrationTracingGrain };

@grain({
  name: "UnitTests.Grains.SimpleMigrationTracingGrain",
  placement: "random",
  collectionAgeSeconds: 1,
})
export class SimpleMigrationTracingGrain extends Grain implements ISimpleMigrationTracingGrain {
  private state = 0;

  async setState(state: number): Promise<void> {
    this.state = state;
  }

  async getState(): Promise<number> {
    return this.state;
  }

  async migrateOnIdle(target?: SiloAddress): Promise<void> {
    this.runtime.migrateOnIdle(target);
  }
}
