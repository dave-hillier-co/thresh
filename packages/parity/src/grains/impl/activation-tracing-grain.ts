// Ported from dotnet/orleans test/Orleans.Runtime.Tests/ActivationTracingTests.cs
// @ v10.1.0 (MIT) — `ActivityGrain` and `PersistentStateActivityGrain`.
import { context, propagation, trace } from "@opentelemetry/api";
import { grain, persistentState } from "@tsva/core/decorators";
import { Grain } from "@tsva/core/grain";
import type { PersistentState } from "@tsva/core/persistent-state";
import {
  IActivityGrain,
  IPersistentStateActivityGrain,
  type ActivityData,
} from "@tsva/parity/grains/interfaces/activation-tracing-grain-interfaces";

export { IActivityGrain, IPersistentStateActivityGrain };

/** Shared by `ActivityGrain`/`PersistentStateActivityGrain`'s `getActivityData()`. */
function readActivityData(): ActivityData {
  const spanContext = trace.getActiveSpan()?.spanContext();
  const baggage: Record<string, string> = {};
  for (const [key, entry] of propagation.getBaggage(context.active())?.getAllEntries() ?? []) {
    baggage[key] = entry.value;
  }
  return {
    traceId: spanContext?.traceId,
    spanId: spanContext?.spanId,
    traceState: spanContext?.traceState?.serialize(),
    baggage,
  };
}

@grain({ name: "UnitTests.Grains.ActivityGrain" })
export class ActivityGrain extends Grain implements IActivityGrain {
  async getActivityId(): Promise<string | undefined> {
    return trace.getActiveSpan()?.spanContext().spanId;
  }

  async getActivityData(): Promise<ActivityData> {
    return readActivityData();
  }
}

interface PersistentStateActivityGrainState {
  value: number;
}

@grain({ name: "UnitTests.Grains.PersistentStateActivityGrain" })
export class PersistentStateActivityGrain extends Grain implements IPersistentStateActivityGrain {
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
