// Ported from dotnet/orleans test/Orleans.Runtime.Tests/DeactivationTracingTests.cs
// @ v10.1.0 (MIT) — `DeactivationTracingTestGrain`,
// `DeactivationWithWorkTracingTestGrain`, `DeactivationWithExceptionTracingTestGrain`,
// and `ActivationFailureDeactivationGrain`.
import { trace } from "@opentelemetry/api";
import { grain, persistentState } from "@tsva/core/decorators";
import { Grain } from "@tsva/core/grain";
import type { PersistentState } from "@tsva/core/persistent-state";
import type { DeactivationReason } from "@tsva/core/reasons";
import {
  IActivationFailureDeactivationGrain,
  IDeactivationTracingTestGrain,
  IDeactivationWithExceptionTracingTestGrain,
  IDeactivationWithWorkTracingTestGrain,
} from "@tsva/parity/grains/interfaces/deactivation-tracing-grain-interfaces";

export {
  IActivationFailureDeactivationGrain,
  IDeactivationTracingTestGrain,
  IDeactivationWithExceptionTracingTestGrain,
  IDeactivationWithWorkTracingTestGrain,
};

@grain({ name: "UnitTests.Grains.DeactivationTracingTestGrain" })
export class DeactivationTracingTestGrain extends Grain implements IDeactivationTracingTestGrain {
  async getActivityId(): Promise<string | undefined> {
    return trace.getActiveSpan()?.spanContext().spanId;
  }

  // `onDeactivate` left as the base no-op — upstream's override just returns
  // `Task.CompletedTask` too.
}

interface DeactivationWorkState {
  wasDeactivated: boolean;
}

@grain({ name: "UnitTests.Grains.DeactivationWithWorkTracingTestGrain" })
export class DeactivationWithWorkTracingTestGrain
  extends Grain
  implements IDeactivationWithWorkTracingTestGrain
{
  @persistentState("deactivationState", {
    defaultValue: (): DeactivationWorkState => ({ wasDeactivated: false }),
  })
  private state!: PersistentState<DeactivationWorkState>;

  async getActivityId(): Promise<string | undefined> {
    return trace.getActiveSpan()?.spanContext().spanId;
  }

  async wasDeactivated(): Promise<boolean> {
    return this.state.value.wasDeactivated;
  }

  override async onDeactivate(_reason: DeactivationReason): Promise<void> {
    this.state.value = { wasDeactivated: true };
    await this.state.write();
  }
}

@grain({ name: "UnitTests.Grains.DeactivationWithExceptionTracingTestGrain" })
export class DeactivationWithExceptionTracingTestGrain
  extends Grain
  implements IDeactivationWithExceptionTracingTestGrain
{
  async getActivityId(): Promise<string | undefined> {
    return trace.getActiveSpan()?.spanContext().spanId;
  }

  override onDeactivate(_reason: DeactivationReason): Promise<void> {
    throw new Error("Simulated error during deactivation");
  }
}

@grain({ name: "UnitTests.Grains.ActivationFailureDeactivationGrain" })
export class ActivationFailureDeactivationGrain
  extends Grain
  implements IActivationFailureDeactivationGrain
{
  constructor() {
    super();
    // Throw in the constructor to fail activation — mirrors upstream, which
    // throws before `OnActivateAsync` ever runs.
    throw new Error("Simulated activation failure for testing deactivation tracing");
  }

  async getActivityId(): Promise<string | undefined> {
    return trace.getActiveSpan()?.spanContext().spanId;
  }

  override async onDeactivate(_reason: DeactivationReason): Promise<void> {
    // Never called since activation fails.
  }
}
