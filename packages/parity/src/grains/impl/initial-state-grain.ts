// Ported from dotnet/orleans test/Grains/TestGrains/InitialStateGrain.cs @ v10.1.0 (MIT).
import { grain, persistentState } from "@tsva/core/decorators";
import { Grain } from "@tsva/core/grain";
import type { PersistentState } from "@tsva/core/persistent-state";
import { IInitialStateGrain } from "@tsva/parity/grains/interfaces/initial-state-grain-interfaces";

export { IInitialStateGrain };

interface InitializedState {
  names: string[];
}

@grain({ name: "UnitTests.Grains.InitialStateGrain" })
export class InitialStateGrain extends Grain implements IInitialStateGrain {
  @persistentState("initialStateGrain", {
    defaultValue: (): InitializedState => ({ names: [] }),
  })
  private state!: PersistentState<InitializedState>;

  async getNames(): Promise<string[]> {
    return this.state.value.names;
  }

  async addName(name: string): Promise<void> {
    this.state.value.names.push(name);
    await this.state.write();
  }
}
