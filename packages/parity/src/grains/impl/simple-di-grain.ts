// Ported from dotnet/orleans test/Grains/TestInternalGrains/ExplicitlyRegisteredSimpleDIGrain.cs
// @ v10.1.0 (MIT), trimmed to what GrainActivatorTests exercises: a grain whose
// string/long values are supplied by its constructor rather than read from DI,
// so a test can prove a custom `GrainActivator` (rather than `new ctor()`) built
// it. Upstream also takes an `InjectedService` constructor dependency to prove
// DI still runs inside a custom activator; this framework has no DI container
// on the construction path to exercise, so that dependency is dropped — the
// activator constructs this grain directly.
import { grain } from "@tsva/core/decorators";
import { Grain } from "@tsva/core/grain";
import { ISimpleDiGrain } from "@tsva/parity/grains/interfaces/simple-di-grain-interfaces";

export { ISimpleDiGrain };

@grain({ name: "UnitTests.Grains.ExplicitlyRegisteredSimpleDiGrain" })
export class ExplicitlyRegisteredSimpleDiGrain extends Grain implements ISimpleDiGrain {
  constructor(
    private readonly stringValue: string = "",
    private readonly longValue: bigint = 0n,
  ) {
    super();
  }

  async getStringValue(): Promise<string> {
    return this.stringValue;
  }

  async getLongValue(): Promise<bigint> {
    return this.longValue;
  }

  async doDeactivate(): Promise<void> {
    this.runtime.deactivateOnIdle();
  }
}
