// Ported from dotnet/orleans test/Grains/TestGrains/StatelessWorkerWithMayInterleaveGrain.cs
// @ v10.1.0 (MIT).
//
// Upstream marks this grain `[StatelessWorker(1)]` purely to force every call
// onto a single local activation ("'1' to force interleaving, otherwise it
// just creates a new worker" — the upstream source comment) so the test can
// be sure every `GoFast`/`GoSlow` call lands on the SAME activation and the
// only thing gating concurrent execution is `[MayInterleave]`'s per-activation
// admission check. This framework's placement does not enforce
// `StatelessWorker` activation limits (GAP-STATELESS-WORKER), but every grain
// already activates as a single activation by default, so a plain grain here
// reproduces the exact scenario the original test exercises — the interleaving
// behavior under test is a per-activation `TurnScheduler` concern, not a
// multi-activation placement one.
import { grain, mayInterleave } from "@tsva/core/decorators";
import { Grain } from "@tsva/core/grain";
import { IMayInterleaveGrain } from "@tsva/parity/grains/interfaces/may-interleave-grain-interfaces";

export { IMayInterleaveGrain };

interface Signal {
  readonly promise: Promise<void>;
  readonly resolve: () => void;
}

function createSignal(): Signal {
  let resolve!: () => void;
  const promise = new Promise<void>((r) => {
    resolve = r;
  });
  return { promise, resolve };
}

// Test-only signaling replacing upstream's `ICallbackGrainObserver` (see the
// interfaces file for why): each tagged call resolves its "started" signal as
// soon as it begins running, and every call blocks on one shared "release"
// signal until the test lets it proceed — mirroring the original
// `CallbackObserver.WaitAsync()` / `Signal()` pair.
const started = new Map<string, Signal>();
const startedTags = new Set<string>();
let release = createSignal();

function startedSignal(tag: string): Signal {
  let signal = started.get(tag);
  if (signal === undefined) {
    signal = createSignal();
    started.set(tag, signal);
  }
  return signal;
}

function markStarted(tag: string): void {
  startedTags.add(tag);
  startedSignal(tag).resolve();
}

/** Reset signaling state between tests; call at the start of each test. */
export function resetMayInterleaveTestState(): void {
  started.clear();
  startedTags.clear();
  release = createSignal();
}

/** Resolves once the tagged call has started running. */
export function mayInterleaveCallStarted(tag: string): Promise<void> {
  return startedSignal(tag).promise;
}

/** The tags of every call that has started running so far (survives across calls, cleared by reset). */
export function mayInterleaveStartedTags(): ReadonlySet<string> {
  return startedTags;
}

/** Releases every call currently blocked awaiting release (upstream: `Signal()`). */
export function releaseMayInterleaveCalls(): void {
  release.resolve();
}

@grain({ name: "UnitTests.Grains.StatelessWorkerWithMayInterleaveGrain" })
@mayInterleave((methodName) => methodName === "goFast")
export class MayInterleaveGrain extends Grain implements IMayInterleaveGrain {
  async goFast(tag: string): Promise<void> {
    markStarted(tag);
    await release.promise;
  }

  async goSlow(tag: string): Promise<void> {
    markStarted(tag);
    await release.promise;
  }
}
