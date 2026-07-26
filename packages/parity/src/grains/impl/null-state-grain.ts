// Ported from dotnet/orleans test/Grains/TestGrains/NullStateGrain.cs @ v10.1.0 (MIT).
import { grain, persistentState } from "@thresh/core/decorators";
import { Grain } from "@thresh/core/grain";
import type { PersistentState } from "@thresh/core/persistent-state";
import {
  INullStateGrain,
  type NullableState,
} from "@thresh/parity/grains/interfaces/null-state-grain-interfaces";

export { INullStateGrain };

@grain({ name: "UnitTests.Grains.NullStateGrain" })
export class NullStateGrain extends Grain implements INullStateGrain {
  @persistentState("nullStateGrain", {
    defaultValue: (): NullableState | null => ({ name: null }),
  })
  private state!: PersistentState<NullableState | null>;

  async setStateAndDeactivate(state: NullableState | null): Promise<void> {
    this.state.value = state;
    await this.state.write();
    this.runtime.deactivateOnIdle();
  }

  async getState(): Promise<NullableState | null> {
    return this.state.value;
  }
}
