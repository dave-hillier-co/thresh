// Ported from dotnet/orleans test/Orleans.Runtime.Tests/ActivationTracingTests.cs
// @ v10.1.0 (MIT) — `SimpleMigrationTracingTestGrain` (the one grain fixture
// among that file's migration-tracing grains this port doesn't already have
// an equivalent of): a grain with `migrateOnIdle` but NO migration
// participant at all (no `IGrainMigrationParticipant`, no persistent state —
// this port's `PersistentStateImpl` always implements the migration hooks,
// see `migration-test-grain.ts`'s `MigrationTestGrainWithMemoryStorage`, so a
// *truly* participant-free grain needs plain instance state instead), so the
// dehydrate/rehydrate spans' negative case is genuinely testable.
import { grain } from "@thresh/core/decorators";
import { Grain } from "@thresh/core/grain";
import type { GrainType } from "@thresh/core/grain-type";
import type { SiloAddress } from "@thresh/core/silo-address";
import type { PlacementFilter } from "@thresh/runtime/placement/placement-filter";
import type { PlacementContext } from "@thresh/runtime/placement/placement-strategy";
import {
  IMigrationFilterTracingGrain,
  ISimpleMigrationTracingGrain,
} from "@thresh/parity/grains/interfaces/migration-tracing-grain-interfaces";

export { IMigrationFilterTracingGrain, ISimpleMigrationTracingGrain };

/**
 * Names the custom director `MigrationPlacementFilterSpanIsParentedUnderPlaceGrainSpan`
 * registers via `builder.addPlacementFilter(...)` — the port analogue of
 * upstream's `TracingTestPlacementFilterAttribute`/`TracingTestPlacementFilterStrategy`.
 */
export const MIGRATION_TRACING_TEST_PLACEMENT_FILTER = "MigrationTracingTestPlacementFilter";

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

@grain({
  name: "UnitTests.Grains.MigrationFilterTracingGrain",
  placement: "random",
  collectionAgeSeconds: 1,
  placementFilters: [{ kind: "custom", name: MIGRATION_TRACING_TEST_PLACEMENT_FILTER, order: 1 }],
})
export class MigrationFilterTracingGrain extends Grain implements IMigrationFilterTracingGrain {
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

/**
 * A pass-through placement-filter director for
 * `MigrationPlacementFilterSpanIsParentedUnderPlaceGrainSpan` — the port
 * analogue of upstream's `TracingTestPlacementFilterDirector`. Its class
 * name is asserted verbatim as the `orleans.placement.filter.type` span tag
 * (this port tags a filter span with `filter.constructor.name`, so the class
 * is deliberately named to match upstream's `TracingTestPlacementFilterStrategy`
 * tag value).
 */
export class TracingTestPlacementFilterStrategy implements PlacementFilter {
  filter(
    _grainType: GrainType,
    candidates: readonly SiloAddress[],
    _context: PlacementContext,
  ): readonly SiloAddress[] {
    return candidates;
  }
}
