// Ported from dotnet/orleans test/Grains/TestGrains/SimplePersistentGrain.cs @ v10.1.0 (MIT).
import { grain, persistentState } from "@tsva/core/decorators";
import { Grain } from "@tsva/core/grain";
import { Guid } from "@tsva/core/guid";
import type { PersistentState } from "@tsva/core/persistent-state";
import { ISimplePersistentGrain } from "@tsva/parity/grains/interfaces/simple-persistent-grain-interfaces";

export { ISimplePersistentGrain };

interface SimplePersistentGrainState {
  a: number;
  b: number;
}

@grain({ name: "UnitTests.Grains.SimplePersistentGrain" })
export class SimplePersistentGrain extends Grain implements ISimplePersistentGrain {
  @persistentState("simplePersistentGrain", {
    defaultValue: (): SimplePersistentGrainState => ({ a: 0, b: 0 }),
  })
  private state!: PersistentState<SimplePersistentGrainState>;

  // Fresh per activation, never persisted: a version marker so a test can tell
  // a reactivation happened.
  private version = "";

  override async onActivate(): Promise<void> {
    this.version = Guid.newGuid().toString();
  }

  async setA(a: number): Promise<void> {
    this.state.value.a = a;
    await this.state.write();
  }

  async setADeactivating(a: number, deactivate: boolean): Promise<void> {
    if (deactivate) this.runtime.deactivateOnIdle();
    await this.setA(a);
  }

  async setB(b: number): Promise<void> {
    this.state.value.b = b;
    await this.state.write();
  }

  async incrementA(): Promise<void> {
    this.state.value.a += 1;
    await this.state.write();
  }

  async getAxB(): Promise<number> {
    return this.state.value.a * this.state.value.b;
  }

  async getAxBArgs(a: number, b: number): Promise<number> {
    return a * b;
  }

  async getA(): Promise<number> {
    return this.state.value.a;
  }

  async getVersion(): Promise<string> {
    return this.version;
  }
}
