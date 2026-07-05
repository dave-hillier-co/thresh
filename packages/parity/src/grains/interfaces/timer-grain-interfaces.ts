// Ported from dotnet/orleans test/Grains/TestGrainInterfaces/ITimerGrain.cs @ v10.1.0 (MIT).
// Upstream also declares IPocoTimerGrain/IPocoTimerCallGrain/IPocoTimerRequestGrain/
// IPocoNonReentrantTimerCallGrain (marker interfaces, no members added) for POCO grain
// variants; this framework has one grain model (every grain extends `Grain`, there is
// no separate non-Grain-derived "POCO" activation style), so no equivalent is declared
// — the ported tests below already cover the shared behavior.
import type { Duration } from "@tsva/core/duration";
import { defineGrainInterface } from "@tsva/core/grain-interface";
import type { GrainWithIntegerKey } from "@tsva/core/key-kinds";

export interface ITimerGrain extends GrainWithIntegerKey {
  stopDefaultTimer(): Promise<void>;
  getTimerPeriod(): Promise<Duration>;
  getCounter(): Promise<number>;
  deactivate(): Promise<void>;
}

export const ITimerGrain = defineGrainInterface<ITimerGrain>(
  "UnitTests.GrainInterfaces.ITimerGrain",
);

// Upstream overloads StartTimer(name, due)/StartTimer(name, due, operationType) and
// RestartTimer(name, due)/RestartTimer(name, due, period); TS interfaces cannot
// overload across the wire (dispatch is by method name), so both take their optional
// trailing argument as an optional parameter instead.
export interface ITimerCallGrain extends GrainWithIntegerKey {
  getTickCount(): Promise<number>;
  /** The captured error's message, or `undefined` if the timer callback never threw. */
  getException(): Promise<string | undefined>;
  startTimer(name: string, dueTime: Duration, operationType?: string): Promise<void>;
  restartTimer(name: string, dueTime: Duration, period?: Duration): Promise<void>;
  stopTimer(name: string): Promise<void>;
  runSelfDisposingTimer(): Promise<void>;
}

export const ITimerCallGrain = defineGrainInterface<ITimerCallGrain>(
  "UnitTests.GrainInterfaces.ITimerCallGrain",
);

export interface INonReentrantTimerCallGrain extends GrainWithIntegerKey {
  getTickCount(): Promise<number>;
  getException(): Promise<string | undefined>;
  startTimer(name: string, delay: Duration): Promise<void>;
  stopTimer(name: string): Promise<void>;
  externalTick(name: string): Promise<void>;
}

export const INonReentrantTimerCallGrain = defineGrainInterface<INonReentrantTimerCallGrain>(
  "UnitTests.GrainInterfaces.INonReentrantTimerCallGrain",
);
