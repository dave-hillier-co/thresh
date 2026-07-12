// Ported from dotnet/orleans test/Orleans.Runtime.Tests/ActivationTracingTests.cs
// @ v10.1.0 (MIT) — `ActivityGrain` and `PersistentStateActivityGrain`.
import { trace } from "@opentelemetry/api";
import { grain, persistentState } from "@tsva/core/decorators";
import { Grain } from "@tsva/core/grain";
import type { PersistentState } from "@tsva/core/persistent-state";
import {
  IActivityGrain,
  IPersistentStateActivityGrain,
} from "@tsva/parity/grains/interfaces/activation-tracing-grain-interfaces";

export { IActivityGrain, IPersistentStateActivityGrain };

@grain({ name: "UnitTests.Grains.ActivityGrain" })
export class ActivityGrain extends Grain implements IActivityGrain {
  async getActivityId(): Promise<string | undefined> {
    return trace.getActiveSpan()?.spanContext().spanId;
  }
}

interface PersistentStateActivityGrainState {
  value: number;
}

@grain({ name: "UnitTests.Grains.PersistentStateActivityGrain" })
export class PersistentStateActivityGrain
  extends Grain
  implements IPersistentStateActivityGrain
{
  @persistentState("state", {
    defaultValue: (): PersistentStateActivityGrainState => ({ value: 0 }),
  })
  private state!: PersistentState<PersistentStateActivityGrainState>;

  async getActivityId(): Promise<string | undefined> {
    return trace.getActiveSpan()?.spanContext().spanId;
  }

  async getStateValue(): Promise<number> {
    return this.state.value.value;
  }
}
