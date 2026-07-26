// Ported from dotnet/orleans test/Grains/TestGrains/ValueTypeTestGrain.cs @ v10.1.0 (MIT).
import { grain, persistentState } from "@thresh/core/decorators";
import { Grain } from "@thresh/core/grain";
import type { PersistentState } from "@thresh/core/persistent-state";
import {
  IValueTypeTestGrain,
  type ValueTypeTestData,
} from "@thresh/parity/grains/interfaces/value-type-test-grain-interfaces";

export { IValueTypeTestGrain };
export type { ValueTypeTestData };

@grain({ name: "UnitTests.Grains.ValueTypeTestGrain" })
export class ValueTypeTestGrain extends Grain implements IValueTypeTestGrain {
  @persistentState("valueTypeTestGrain", {
    defaultValue: (): ValueTypeTestData => ({ value: 0 }),
  })
  private state!: PersistentState<ValueTypeTestData>;

  async getStateData(): Promise<ValueTypeTestData> {
    return this.state.value;
  }

  async setStateData(d: ValueTypeTestData): Promise<void> {
    this.state.value = d;
    await this.state.write();
  }
}
