// Ported from dotnet/orleans test/Grains/TestInternalGrains/ExtensionTestGrain.cs,
// test/Grains/TestInternalGrains/TestExtension.cs and
// test/Grains/TestGrains/NoOpTestGrain.cs @ v10.1.0 (MIT).
//
// `SimpleExtension`/`GenericExtensionTestGrain`/`GenericGrainWithNonGenericExtension`/
// `GenericTestExtension` are not ported: the generic-grain gap (GAP-GENERIC-GRAINS)
// blocks them, not the extension mechanism itself.
import { grain } from "@tsva/core/decorators";
import { Grain } from "@tsva/core/grain";
import {
  IExtensionTestGrain,
  INoOpTestGrain,
  ITestExtension,
  IAutoExtension,
} from "@tsva/parity/grains/interfaces/extension-test-interfaces";
import { ITestGrain } from "@tsva/parity/grains/interfaces/test-grain-interfaces";

export { IExtensionTestGrain, INoOpTestGrain, ITestExtension, IAutoExtension };

@grain({ name: "UnitTests.Grains.ExtensionTestGrain" })
export class ExtensionTestGrain extends Grain implements IExtensionTestGrain {
  extensionProperty = "";

  override async onActivate(): Promise<void> {
    this.extensionProperty = "";
  }

  async installExtension(name: string): Promise<void> {
    // Idempotent: a second install just re-labels the same bound extension
    // instance (Orleans parity — `SetComponent` only runs the first time).
    this.runtime.getOrSetExtension(ITestExtension, () => new TestExtension(this));
    this.extensionProperty = name;
  }

  /**
   * Lets the (separately-instantiated) `TestExtension` reach another grain
   * through this grain's runtime — `Grain.getGrain` is protected, so the
   * extension calls this small public forwarder instead of reaching into
   * `ExtensionTestGrain` internals directly.
   */
  async testGrainLabel(key: bigint): Promise<string> {
    return this.getGrain(ITestGrain, key).getLabel();
  }
}

/** Bound onto an `ExtensionTestGrain` activation via `installExtension`. */
export class TestExtension implements ITestExtension {
  constructor(private readonly grain: ExtensionTestGrain) {}

  async checkExtension_1(): Promise<string> {
    return this.grain.extensionProperty;
  }

  /** Proves messages can be sent from within an extension. */
  async checkExtension_2(): Promise<string> {
    return this.grain.testGrainLabel(23n);
  }
}

// `INoOpTestGrain` declares no methods (Orleans parity — it exists purely as a
// grain to cast an extension reference onto), so it's a "weak type" TS won't
// let `Grain` satisfy via `implements`; the registration below only needs the
// constructor and the interface's id, not a structural match.
@grain({ name: "UnitTests.Grains.NoOpTestGrain" })
export class NoOpTestGrain extends Grain {}

/** Auto-installed via `SiloBuilder.addGrainExtension(IAutoExtension, ...)`. */
export class AutoExtension implements IAutoExtension {
  async checkExtension(): Promise<string> {
    return "whoot!";
  }
}
