// Ported from dotnet/orleans test/Orleans.Streaming.Tests/StreamingTests/GeneratedStreamRecoveryTests.cs @ v10.1.0 (MIT).
// Runner: test/Orleans.Streaming.Tests/StreamingTests/ImplicitSubscritionRecoverableStreamTestRunner.cs @ v10.1.0 (MIT).
// Grains: test/Grains/TestGrains/StreamCheckpoint.cs,
// ImplicitSubscription_TransientError_RecoverableStream_CollectorGrain.cs,
// ImplicitSubscription_NonTransientError_RecoverableStream_CollectorGrain.cs @ v10.1.0 (MIT).
//
// Both tests drive `GeneratorPullingStreamProvider` (`packages/streams/src/generator-pulling-stream-provider.ts`)
// through `IManagementGrain.SendControlCommandToProvider` exactly like
// `controllable-stream-generator-provider-tests.test.ts`, then poll a
// `IGeneratedEventReporterGrain` for the final per-stream count. What's new
// here is the recoverable-cursor mechanism the two collector grains below
// need: a `StreamCheckpoint<int>`-equivalent (`RecoverableCheckpoint`,
// checkpointed every 10 events via `@persistentState`) plus
// `RecoverableStreamDeliveryError` (`packages/streams/src/stream-recovery.ts`),
// a special exception a `STREAM_SUBSCRIPTION_OBSERVER` handler throws to tell
// `QueuePullingAgent` "rewind the owning queue's cursor to my last checkpoint
// and redeliver everything from there" instead of the normal
// retry-then-skip-one-event behavior (`queue-pulling-agent.ts`'s `tryDeliver`).
//
// Upstream's collector grains inject faults at five distinct points
// (activation, first message, first message processed, the 33rd message,
// the last message) via a `FireOnNthTry` helper. This port injects at a
// single, representative point — the 33rd event, exactly the case the
// upstream code comments call out ("mid stream processing... but really
// depends on stream") and the one that actually exercises the rewind (a
// fault before the first or after the last checkpoint doesn't need one). The
// mechanism this proves — rewind-and-fully-recover for a transient fault vs.
// permanent-single-event-skip for a non-transient one — is the same either
// way; the other four upstream fault points are narrower variations on it
// (recovering with nothing yet checkpointed, or right at stream end) rather
// than additional mechanism, so porting only this one keeps the fixture
// tractable without losing coverage of the recoverable-cursor subsystem
// itself.
import { expect } from "vitest";
import { grain, implicitStreamSubscription, persistentState } from "@thresh/core/decorators";
import { Grain } from "@thresh/core/grain";
import { defineGrainInterface } from "@thresh/core/grain-interface";
import type { GrainWithStringKey } from "@thresh/core/key-kinds";
import { IManagementGrain } from "@thresh/core/management-grain";
import type { PersistentState } from "@thresh/core/persistent-state";
import {
  STREAM_GENERATOR_COMMAND_CONFIGURE,
  STREAM_SUBSCRIPTION_OBSERVER,
  type StreamHandler,
} from "@thresh/core/stream";
import type { GeneratedEvent } from "@thresh/streams/generator-stream-queue";
import { RecoverableStreamDeliveryError } from "@thresh/streams/stream-recovery";
import { orleansTest } from "@thresh/testing/orleans-test";
import { TestCluster } from "@thresh/testing/test-cluster";
import { waitFor } from "@thresh/testing/wait";

const STREAM_PROVIDER_NAME = "GeneratedStreamProvider";
const TOTAL_QUEUE_COUNT = 4;
const EVENTS_IN_STREAM = 100;
const INITIAL_SILOS = 1;
// Orleans `StreamCheckpoint<int>` checkpoints every 10 events.
const CHECKPOINT_INTERVAL = 10;
// The single fault point ported (see file header): mid-stream, well past the
// first checkpoint (30) and short of the last (100), so recovery genuinely
// exercises a rewind rather than "resume from nothing" or "resume at the end".
const FAULT_AT_EVENT = 33;

interface IGeneratedEventReporterGrain extends GrainWithStringKey {
  reportResult(
    streamKey: string,
    streamProvider: string,
    streamNamespace: string,
    count: number,
  ): Promise<void>;
  getReport(streamProvider: string, streamNamespace: string): Promise<Map<string, number>>;
  reset(): Promise<void>;
}
const IGeneratedEventReporterGrain = defineGrainInterface<IGeneratedEventReporterGrain>(
  "IGeneratedEventReporterGrain",
);

@grain()
class GeneratedEventReporterGrain extends Grain implements IGeneratedEventReporterGrain {
  private reports = new Map<string, Map<string, number>>();

  async reportResult(
    streamKey: string,
    streamProvider: string,
    streamNamespace: string,
    count: number,
  ): Promise<void> {
    const reportKey = `${streamProvider}/${streamNamespace}`;
    const counts = this.reports.get(reportKey) ?? new Map<string, number>();
    counts.set(streamKey, count);
    this.reports.set(reportKey, counts);
  }

  async getReport(streamProvider: string, streamNamespace: string): Promise<Map<string, number>> {
    return this.reports.get(`${streamProvider}/${streamNamespace}`) ?? new Map();
  }

  async reset(): Promise<void> {
    this.reports = new Map();
  }
}

/**
 * Orleans `StreamCheckpoint<int>`: durable recovery position + accumulator.
 * `startToken`/`lastProcessedToken` mirror `StartToken`/`LastProcessedToken`;
 * `RecoveryToken` (`LastProcessedToken ?? StartToken`) is inlined at call
 * sites below rather than as a getter, since callers need "one before that"
 * (the token to rewind the *queue* to, not the token already processed).
 */
interface RecoverableCheckpoint {
  startToken?: number | undefined;
  lastProcessedToken?: number | undefined;
  accumulator: number;
}

/** Orleans `StreamCheckpoint<TState>.IsDuplicate`. */
function isDuplicateOfCheckpoint(cp: RecoverableCheckpoint, token: number): boolean {
  if (cp.startToken === undefined) return false;
  if (cp.lastProcessedToken !== undefined) return cp.lastProcessedToken >= token;
  return cp.startToken > token;
}

/**
 * Shared recoverable-cursor mechanics for both fault-injecting collector
 * grains (Orleans `ImplicitSubscription_{Transient,NonTransient}Error_RecoverableStream_CollectorGrain`).
 * `checkpoint` is written only at a checkpoint boundary (every
 * `CHECKPOINT_INTERVAL` events, and on the final "report" event) — exactly
 * like upstream's `State.Accumulator`/`WriteStateAsync` split — so a fault
 * that deactivates this activation before the next boundary discards
 * whatever this activation counted past the last write, and a fresh
 * activation resumes counting from there.
 */
abstract class RecoverableCollectorGrainBase
  extends Grain
  implements IGeneratedEventCollectorGrain
{
  protected abstract readonly streamNamespace: string;
  protected abstract readonly reporterId: string;

  @persistentState("checkpoint", {
    defaultValue: (): RecoverableCheckpoint => ({ accumulator: 0 }),
  })
  protected checkpoint!: PersistentState<RecoverableCheckpoint>;

  // This activation's own running position, separate from `checkpoint.value`
  // (which only ever reflects the last *durable* write — see class doc).
  private runningAccumulator = 0;
  private runningLastToken: number | undefined;
  private initialized = false;

  async marker(): Promise<string> {
    return "collector";
  }

  [STREAM_SUBSCRIPTION_OBSERVER](_namespace: string, key: string): StreamHandler<GeneratedEvent> {
    return {
      onNext: async (evt, token) => this.handleEvent(evt, token.value, key),
    };
  }

  /**
   * `true` to permanently drop `token` without counting it (a redelivery of
   * an already-non-transient-faulted token — see
   * `NonTransientErrorRecoverableCollectorGrain`). The transient grain never
   * needs this: it fully recovers via the rewind instead.
   */
  protected shouldPermanentlySkip(_token: number, _key: string): boolean {
    return false;
  }

  /**
   * Called with the accumulator value this event WOULD produce, before it is
   * committed to `runningAccumulator` — mirrors upstream's fault check
   * running before `State.Accumulator++`, so an injected fault contributes
   * nothing to the count. May throw (`RecoverableStreamDeliveryError` for a
   * transient fault, a plain `Error` for a non-transient one); a no-op
   * override means "never fault".
   */
  protected maybeInjectFault(_candidateAccumulator: number, _token: number, _key: string): void {
    // no fault by default
  }

  private async handleEvent(evt: GeneratedEvent, token: number, key: string): Promise<void> {
    if (!this.initialized) {
      this.runningAccumulator = this.checkpoint.value.accumulator;
      this.runningLastToken = this.checkpoint.value.lastProcessedToken;
      this.initialized = true;
    }

    if (this.shouldPermanentlySkip(token, key)) return;

    // Redelivered from an earlier rewind/at-least-once retry that the
    // durable checkpoint already covers.
    if (isDuplicateOfCheckpoint(this.checkpoint.value, token)) return;
    // A same-token retry within this activation's own in-flight progress —
    // already counted, no-op.
    if (this.runningLastToken !== undefined && token <= this.runningLastToken) return;

    if (this.checkpoint.value.startToken === undefined) {
      this.checkpoint.value.startToken = token;
      await this.checkpoint.write();
    }

    const candidateAccumulator = this.runningAccumulator + 1;
    // May throw — deliberately BEFORE committing `candidateAccumulator`, so
    // a faulted event contributes nothing to the count (Orleans checks
    // `State.Accumulator == 32` before `State.Accumulator++`, not after).
    this.maybeInjectFault(candidateAccumulator, token, key);

    this.runningAccumulator = candidateAccumulator;
    this.runningLastToken = token;

    if (evt.eventType !== "report") {
      if (this.runningAccumulator % CHECKPOINT_INTERVAL !== 0) return;
      await this.persistCheckpoint();
      return;
    }

    await this.persistCheckpoint();
    const reporter = this.getGrain(IGeneratedEventReporterGrain, this.reporterId);
    await reporter.reportResult(
      key,
      STREAM_PROVIDER_NAME,
      this.streamNamespace,
      this.runningAccumulator,
    );
  }

  private async persistCheckpoint(): Promise<void> {
    this.checkpoint.value.accumulator = this.runningAccumulator;
    this.checkpoint.value.lastProcessedToken = this.runningLastToken;
    await this.checkpoint.write();
  }
}

interface IGeneratedEventCollectorGrain extends GrainWithStringKey {
  marker(): Promise<string>;
}
const IGeneratedEventCollectorGrain = defineGrainInterface<IGeneratedEventCollectorGrain>(
  "IGeneratedEventCollectorGrain",
);

const TRANSIENT_NAMESPACE = "TransientError_RecoverableStream";
const TRANSIENT_REPORTER_ID = "transient-recoverable-stream-reporter";
// Per-stream "has this stream's one fault already fired" — a module-level
// tracker (Orleans' `static ConcurrentDictionary`) so it survives this
// activation deactivating and a fresh one taking over.
const transientFaultsFired = new Set<string>();

@grain()
@implicitStreamSubscription(TRANSIENT_NAMESPACE)
class TransientErrorRecoverableCollectorGrain extends RecoverableCollectorGrainBase {
  protected readonly streamNamespace = TRANSIENT_NAMESPACE;
  protected readonly reporterId = TRANSIENT_REPORTER_ID;

  protected override maybeInjectFault(
    candidateAccumulator: number,
    _token: number,
    key: string,
  ): void {
    if (candidateAccumulator !== FAULT_AT_EVENT || transientFaultsFired.has(key)) return;
    transientFaultsFired.add(key);
    // Deactivate first (Orleans `DeactivateOnIdle` before throwing): the next
    // delivery to this stream reaches a fresh activation that resumes from
    // its own persisted checkpoint.
    this.runtime.deactivateOnIdle();
    const cp = this.checkpoint.value;
    const resumeToken =
      cp.lastProcessedToken ?? (cp.startToken !== undefined ? cp.startToken - 1 : 0);
    throw new RecoverableStreamDeliveryError(
      `injected transient fault for stream ${key} at event ${FAULT_AT_EVENT}`,
      resumeToken,
    );
  }
}

const NON_TRANSIENT_NAMESPACE = "NonTransientError_RecoverableStream";
const NON_TRANSIENT_REPORTER_ID = "nontransient-recoverable-stream-reporter";
// Per-stream: the exact token that was permanently skipped (once faulted, a
// redelivery of THAT token must still be dropped without being counted —
// see `shouldPermanentlySkip`).
const nonTransientSkippedTokens = new Map<string, number>();

@grain()
@implicitStreamSubscription(NON_TRANSIENT_NAMESPACE)
class NonTransientErrorRecoverableCollectorGrain extends RecoverableCollectorGrainBase {
  protected readonly streamNamespace = NON_TRANSIENT_NAMESPACE;
  protected readonly reporterId = NON_TRANSIENT_REPORTER_ID;

  protected override shouldPermanentlySkip(token: number, key: string): boolean {
    return nonTransientSkippedTokens.get(key) === token;
  }

  protected override maybeInjectFault(
    candidateAccumulator: number,
    token: number,
    key: string,
  ): void {
    if (candidateAccumulator !== FAULT_AT_EVENT || nonTransientSkippedTokens.has(key)) return;
    nonTransientSkippedTokens.set(key, token);
    // No deactivation: this activation stays alive. The pulling agent
    // retries this token once more (see `queue-pulling-agent.ts`'s
    // `tryDeliver`), which `shouldPermanentlySkip` now silently drops, so
    // the event is dropped for good (99/100) and counting resumes normally
    // from the next token.
    throw new Error(`injected non-transient fault for stream ${key} at event ${FAULT_AT_EVENT}`);
  }
}

async function generateEvents(cluster: TestCluster, streamNamespace: string): Promise<void> {
  const mgmt = cluster.getGrain(IManagementGrain, 0n);
  const results = await mgmt.sendControlCommandToProvider(
    "PersistentStreamProvider",
    STREAM_PROVIDER_NAME,
    STREAM_GENERATOR_COMMAND_CONFIGURE,
    { streamNamespace, eventsInStream: EVENTS_IN_STREAM },
  );
  expect(results.length).toBe(INITIAL_SILOS);
  for (const result of results) expect(result).toBe(true);
}

async function checkCounters(
  reporter: IGeneratedEventReporterGrain,
  streamNamespace: string,
  expectedEventsPerStream: number,
): Promise<boolean> {
  const report = await reporter.getReport(STREAM_PROVIDER_NAME, streamNamespace);
  if (report.size !== TOTAL_QUEUE_COUNT) return false;
  return [...report.values()].every((count) => count === expectedEventsPerStream);
}

orleansTest(
  "UnitTests.StreamingTests.GeneratedImplicitSubscriptionStreamRecoveryTests.Recoverable100EventStreamsWithTransientErrorsTest",
  async () => {
    const cluster = await TestCluster.start({
      initialSilos: INITIAL_SILOS,
      grains: [
        {
          ctor: TransientErrorRecoverableCollectorGrain,
          interfaces: [IGeneratedEventCollectorGrain],
        },
        { ctor: GeneratedEventReporterGrain, interfaces: [IGeneratedEventReporterGrain] },
      ],
      configureSilo: (builder) => {
        builder.addGeneratorStreams(
          STREAM_PROVIDER_NAME,
          { streamNamespace: TRANSIENT_NAMESPACE, eventsInStream: 0 },
          { queueCount: TOTAL_QUEUE_COUNT, pollIntervalMs: 5 },
        );
      },
    });
    const reporter = cluster.getGrain(IGeneratedEventReporterGrain, TRANSIENT_REPORTER_ID);
    try {
      await generateEvents(cluster, TRANSIENT_NAMESPACE);
      await waitFor(() => checkCounters(reporter, TRANSIENT_NAMESPACE, EVENTS_IN_STREAM), {
        timeoutMs: 30_000,
      });

      const report = await reporter.getReport(STREAM_PROVIDER_NAME, TRANSIENT_NAMESPACE);
      expect(report.size).toBe(TOTAL_QUEUE_COUNT);
      for (const count of report.values()) {
        expect(count).toBe(EVENTS_IN_STREAM);
      }
    } finally {
      await reporter.reset();
      await cluster.dispose();
    }
  },
  35_000,
);

orleansTest(
  "UnitTests.StreamingTests.GeneratedImplicitSubscriptionStreamRecoveryTests.Recoverable100EventStreamsWith1NonTransientErrorTest",
  async () => {
    const cluster = await TestCluster.start({
      initialSilos: INITIAL_SILOS,
      grains: [
        {
          ctor: NonTransientErrorRecoverableCollectorGrain,
          interfaces: [IGeneratedEventCollectorGrain],
        },
        { ctor: GeneratedEventReporterGrain, interfaces: [IGeneratedEventReporterGrain] },
      ],
      configureSilo: (builder) => {
        builder.addGeneratorStreams(
          STREAM_PROVIDER_NAME,
          { streamNamespace: NON_TRANSIENT_NAMESPACE, eventsInStream: 0 },
          { queueCount: TOTAL_QUEUE_COUNT, pollIntervalMs: 5 },
        );
      },
    });
    const reporter = cluster.getGrain(IGeneratedEventReporterGrain, NON_TRANSIENT_REPORTER_ID);
    try {
      await generateEvents(cluster, NON_TRANSIENT_NAMESPACE);
      // One event permanently skipped: eventsInStream - 1.
      await waitFor(() => checkCounters(reporter, NON_TRANSIENT_NAMESPACE, EVENTS_IN_STREAM - 1), {
        timeoutMs: 90_000,
      });

      const report = await reporter.getReport(STREAM_PROVIDER_NAME, NON_TRANSIENT_NAMESPACE);
      expect(report.size).toBe(TOTAL_QUEUE_COUNT);
      for (const count of report.values()) {
        expect(count).toBe(EVENTS_IN_STREAM - 1);
      }
    } finally {
      await reporter.reset();
      await cluster.dispose();
    }
  },
  95_000,
);
