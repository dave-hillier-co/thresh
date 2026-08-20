// Ported from dotnet/orleans test/Orleans.Runtime.Tests/ActivationTracingTests.cs
// @ v10.1.0 (MIT). Upstream declares `IActivityGrain`/`ActivityGrain` and
// `IPersistentStateActivityGrain`/`PersistentStateActivityGrain` inline in
// the test file itself (along with `ActivityData`, which activation tracing
// itself doesn't need here — asserted from the harness's captured spans, not
// from data the grain reports back). `getActivityData()` (below) DOES report
// `ActivityData`-shaped data back — needed by
// activity-propagation-tests.test.ts, which (unlike activation-tracing-tests)
// asserts on what the CALLED grain observes of its own trace context
// (span id, trace state, baggage), not on captured spans.
import { defineGrainInterface } from "@thresh/core/grain-interface";
import type { GrainKey } from "@thresh/core/key-kinds";

/**
 * What a grain call sees of its own OpenTelemetry span (the port analogue of
 * upstream's `ActivityData`: `Activity.Id`/`TraceStateString`/`Baggage`).
 */
export interface ActivityData {
  readonly traceId: string | undefined;
  readonly spanId: string | undefined;
  readonly traceState: string | undefined;
  readonly baggage: Readonly<Record<string, string>>;
}

export interface IActivityGrain extends GrainKey<bigint> {
  getActivityId(): Promise<string | undefined>;
  /** Reports this activation's trace id/span id/trace state/baggage — see `ActivityData`. */
  getActivityData(): Promise<ActivityData>;
}

export const IActivityGrain = defineGrainInterface<IActivityGrain>(
  "UnitTests.GrainInterfaces.IActivityGrain",
);

export interface IPersistentStateActivityGrain extends GrainKey<bigint> {
  getActivityId(): Promise<string | undefined>;
  getStateValue(): Promise<number>;
}

export const IPersistentStateActivityGrain = defineGrainInterface<IPersistentStateActivityGrain>(
  "UnitTests.GrainInterfaces.IPersistentStateActivityGrain",
);
