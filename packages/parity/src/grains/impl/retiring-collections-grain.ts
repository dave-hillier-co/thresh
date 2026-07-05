// Test-scaffolding grain implementations for GAP-STATE-MACHINE-RETIREMENT.
// See retiring-collections-grain-interfaces.ts for why there are two classes
// sharing one grain type name.
import { durableDictionary, grain } from "@tsva/core/decorators";
import type { DurableDictionary } from "@tsva/core/durable-state";
import { Grain } from "@tsva/core/grain";
import {
  IRetiringCollectionsGrain,
  IRetiringCollectionsGrainPartial,
} from "@tsva/parity/grains/interfaces/retiring-collections-grain-interfaces";

export { IRetiringCollectionsGrain, IRetiringCollectionsGrainPartial };

const RETIRING_GRAIN_TYPE = "Orleans.Journaling.Tests.RetiringCollectionsGrain";

/** Both "keep" and "retire" durable dictionaries registered. */
@grain({ name: RETIRING_GRAIN_TYPE })
export class RetiringCollectionsGrain extends Grain implements IRetiringCollectionsGrain {
  @durableDictionary("keep")
  private keep!: DurableDictionary<unknown, unknown>;

  @durableDictionary("retire")
  private retire!: DurableDictionary<unknown, unknown>;

  async keepSet(key: unknown, value: unknown): Promise<void> {
    await this.keep.set(key, value);
  }

  async keepGet(key: unknown): Promise<unknown> {
    return this.keep.get(key);
  }

  async retireSet(key: unknown, value: unknown): Promise<void> {
    await this.retire.set(key, value);
  }

  async retireGet(key: unknown): Promise<unknown> {
    return this.retire.get(key);
  }

  async retireHas(key: unknown): Promise<boolean> {
    return this.retire.has(key);
  }
}

/**
 * Only the "keep" durable dictionary registered — the "retire" field was
 * removed from the grain's dependencies, the scenario
 * `StateMachineManager_AutoRetiringStateMachines` exercises. Shares
 * `RETIRING_GRAIN_TYPE` with `RetiringCollectionsGrain` so, over the same
 * journal storage, the same `GrainId` sees "retire" go from registered to
 * unregistered and back as the test swaps which of these two classes serves
 * a given activation.
 */
@grain({ name: RETIRING_GRAIN_TYPE })
export class RetiringCollectionsGrainPartial
  extends Grain
  implements IRetiringCollectionsGrainPartial
{
  @durableDictionary("keep")
  private keep!: DurableDictionary<unknown, unknown>;

  async keepSet(key: unknown, value: unknown): Promise<void> {
    await this.keep.set(key, value);
  }

  async keepGet(key: unknown): Promise<unknown> {
    return this.keep.get(key);
  }
}
