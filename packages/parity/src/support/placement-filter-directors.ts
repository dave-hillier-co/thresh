// Test-only custom placement-filter directors backing
// `grain-placement-filter-tests.test.ts` (ported from dotnet/orleans
// test/Orleans.Placement.Tests/PlacementFilterTests/GrainPlacementFilterTests.cs
// @ v10.1.0's `TestPlacementFilterDirector`/`OrderAPlacementFilterDirector`/
// `OrderBPlacementFilterDirector`). Registered on a `TestCluster` silo via
// `builder.addPlacementFilter(name, director)` (Orleans
// `AddPlacementFilter<TStrategy, TDirector>`); a grain opts in with a
// `{ kind: "custom", name, order }` `placementFilters` entry.
import type { GrainType } from "@thresh/core/grain-type";
import type { SiloAddress } from "@thresh/core/silo-address";
import type { PlacementFilter } from "@thresh/runtime/placement/placement-filter";
import type { PlacementContext } from "@thresh/runtime/placement/placement-strategy";

/**
 * Records each invocation and lets a test await the next one — the TS
 * analogue of upstream's `SemaphoreSlim Triggered` on
 * `TestPlacementFilterDirector`. Passes candidates through unchanged.
 */
export class RecordingPlacementFilterDirector implements PlacementFilter {
  private count = 0;
  private waiters: Array<() => void> = [];

  get triggerCount(): number {
    return this.count;
  }

  /** Resolves `true` once this director has run at least once since call time, or `false` on timeout. */
  waitForTrigger(timeoutMs = 1000): Promise<boolean> {
    if (this.count > 0) return Promise.resolve(true);
    return new Promise((resolve) => {
      const timer = setTimeout(() => resolve(false), timeoutMs);
      this.waiters.push(() => {
        clearTimeout(timer);
        resolve(true);
      });
    });
  }

  filter(
    _grainType: GrainType,
    candidates: readonly SiloAddress[],
    _context: PlacementContext,
  ): readonly SiloAddress[] {
    this.count += 1;
    const waiters = this.waiters;
    this.waiters = [];
    for (const resolve of waiters) resolve();
    return candidates;
  }
}

/**
 * Appends a fixed tag to a per-grain-type scratchpad list on every invocation
 * — the TS analogue of upstream's `OrderA`/`OrderBPlacementFilterDirector`
 * pushing into `GrainPlacementFilterTests.FilterScratchpad`, used to observe
 * the order several stacked filters ran in for a given grain type. Passes
 * candidates through unchanged.
 */
export class TaggingPlacementFilterDirector implements PlacementFilter {
  constructor(
    private readonly tag: string,
    private readonly scratchpad: Map<string, string[]>,
  ) {}

  filter(
    grainType: GrainType,
    candidates: readonly SiloAddress[],
    _context: PlacementContext,
  ): readonly SiloAddress[] {
    const list = this.scratchpad.get(grainType) ?? [];
    list.push(this.tag);
    this.scratchpad.set(grainType, list);
    return candidates;
  }
}
