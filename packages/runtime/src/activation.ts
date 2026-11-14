import { AsyncLocalStorage } from "node:async_hooks";
import { newActivationId, type ActivationId } from "@thresh/core/activation-id";
import {
  broadcastChannelObserver,
  BroadcastConsumerInterface,
  type BroadcastChannelHandler,
} from "@thresh/core/broadcast-channel";
import type { Duration } from "@thresh/core/duration";
import { isRemotelyCallable } from "@thresh/core/remote-method-surface";
import {
  DurableJobConsumerInterface,
  durableJobHandler,
  type JobRunContext,
} from "@thresh/core/durable-job";
import {
  GrainCallError,
  GrainCallTimeoutError,
  GrainExtensionNotInstalledException,
  InconsistentStateError,
  RejectionError,
} from "@thresh/core/errors";
import {
  mapCancellationValues,
  type CancellationValue,
} from "@thresh/core/cancellation-value-walk";
import {
  CancellationTokenPlaceholder,
  GrainCancellationToken,
} from "@thresh/core/grain-cancellation-token";
import type { Grain } from "@thresh/core/grain";
import type { GrainContext } from "@thresh/core/grain-context";
import type { GrainId } from "@thresh/core/grain-id";
import { getGrainMetadata, type GrainConstructor } from "@thresh/core/grain-metadata";
import { MigrationBag } from "@thresh/core/grain-migration-participant";
import type { GrainRuntime } from "@thresh/core/grain-runtime";
import type { GrainTimer, TimerOptions } from "@thresh/core/grain-timer";
import type { InvokeMethodOptions } from "@thresh/core/invoke-options";
import { type Logger, defaultLogger } from "@thresh/core/logger";
import {
  formatDeactivationReason,
  type ActivationReason,
  type DeactivationReason,
} from "@thresh/core/reasons";
import type { GrainAddress } from "@thresh/core/grain-address";
import type { SiloAddress } from "@thresh/core/silo-address";
import { migrationParticipantsOf } from "@thresh/runtime/migration-participants";
import { getGrainInterface } from "@thresh/core/grain-interface";
import type { CancellationSourcesExtension } from "@thresh/runtime/cancellation-extension";
import {
  cancellationExtensionFactory,
  ICancellationSourcesExtension,
} from "@thresh/runtime/cancellation-extension";
import {
  grainIncomingFilter,
  runCallFilters,
  type IncomingGrainCallContext,
  type IncomingGrainCallFilter,
} from "@thresh/core/grain-call-filter";
import { combineSignals } from "@thresh/core/abort";
import { getPersistentFields } from "@thresh/core/persistent-state-metadata";
import { guardPersistentStateForReadOnly } from "@thresh/core/persistent-state-readonly-guard";
import { getReducerFields } from "@thresh/core/reducer-state-metadata";
import { guardReducerStateForReadOnly } from "@thresh/core/reducer-state-readonly-guard";
import { getDurableFields } from "@thresh/core/durable-state-metadata";
import { guardDurableFieldForReadOnly } from "@thresh/core/durable-state-readonly-guard";
import {
  guardTransactionalStateForReadOnly,
  unguardedTransactionalState,
} from "@thresh/core/transactional-state-readonly-guard";
import type { InvocationRequest } from "@thresh/core/request";
import {
  type CallChainReentrantGrainContext,
  PLACEMENT_HINT_KEY,
  RequestContext,
  requestContextStore,
  runWithCallChain,
  runWithRequestContext,
} from "@thresh/core/request-context";
import {
  implicitStreamObserver,
  SequenceToken,
  StreamConsumerInterface,
  type StreamHandler,
} from "@thresh/core/stream";
import { blocksDeactivation, type TransactionParticipant } from "@thresh/core/transaction-info";
import { TransactionResourceInterface } from "@thresh/core/transaction-resource";
import { getTransactionalFields } from "@thresh/core/transactional-state-metadata";
import { deadlineSignal } from "@thresh/runtime/ambient-signal";
import type { InvokeCallOptions } from "@thresh/runtime/dispatcher";
import { GrainTimerImpl } from "@thresh/runtime/grain-timer-impl";
import { invocationContext, type InvocationContext } from "@thresh/runtime/invocation-context";
import type { TimeProvider, TimerHandle } from "@thresh/runtime/time-provider";
import { TurnScheduler, type Turn } from "@thresh/runtime/turn-scheduler";
import {
  withDehydrateSpan,
  withOnDeactivateSpan,
  withRehydrateSpan,
} from "@thresh/observability/activation-tracing";

/**
 * The activation lifecycle (Orleans `ActivationState`, plus `failed` for an
 * activation whose `onActivate` threw):
 *
 * - `creating`: constructed; its activate operation has not been scheduled.
 * - `activating`: the activate operation (state binding or rehydration, then
 *   `onActivate`) is queued or running. Work that arrives waits.
 * - `valid`: serving. The only state that admits work.
 * - `failed`: activation threw. The deactivation pipeline is finishing it;
 *   work is rejected with the original error.
 * - `deactivating`: a deactivation was requested (idle collection,
 *   `deactivateOnIdle`, an escaped `InconsistentStateError`, a migration, a
 *   silo stop). Work that arrives is held and rerouted once the pipeline has
 *   finished, or dropped if it is a timer tick. Its migrating sub-state is a
 *   deactivation carrying a migration request (`isMigrating`), which hands the
 *   state to another silo before finalizing.
 * - `invalid`: finalized (unregistered, removed, disposed) or abandoned as
 *   stuck. Terminal.
 *
 * {@link ACTIVATION_TRANSITIONS} lists every legal move; any other throws.
 */
export type ActivationState =
  | "creating"
  | "activating"
  | "valid"
  | "failed"
  | "deactivating"
  | "invalid";

/**
 * The legal lifecycle transitions, from each state to the states it may move
 * to. `valid -> invalid` is abandonment of a stuck activation (Orleans
 * `DeactivateStuckActivation`); every other route to `invalid` passes through
 * the deactivation pipeline.
 */
export const ACTIVATION_TRANSITIONS: Readonly<Record<ActivationState, readonly ActivationState[]>> =
  {
    creating: ["activating", "deactivating"],
    activating: ["valid", "failed", "deactivating"],
    valid: ["deactivating", "invalid"],
    failed: ["invalid"],
    deactivating: ["invalid"],
    invalid: [],
  };

/** Throw unless `from -> to` is in {@link ACTIVATION_TRANSITIONS}. */
export function assertActivationTransition(from: ActivationState, to: ActivationState): void {
  if (!ACTIVATION_TRANSITIONS[from].includes(to)) {
    throw new Error(`illegal activation state transition: ${from} -> ${to}`);
  }
}

/**
 * Turn-scheduler and deactivation knobs (Orleans `SchedulingOptions` +
 * `CollectionOptions.DeactivationTimeout`). Every field is opt-in
 * (`undefined` keeps prior unbounded/untimed behaviour) so a caller that
 * builds an `ActivationData` directly — every test in this package does —
 * keeps working unchanged; the silo builder is what actually turns these on
 * with Orleans-like defaults for a hosted silo.
 */
export interface ActivationOptions {
  /** Orleans `WorkItemGroup`: warn once the turn queue reaches this length. */
  maxEnqueuedRequestsSoftLimit?: number;
  /** Orleans `WorkItemGroup`: reject a newly scheduled turn once the queue is at this length. */
  maxEnqueuedRequestsHardLimit?: number;
  /** Orleans `MaxRequestProcessingTime`: warn once a running turn exceeds this many ms. */
  maxRequestProcessingTimeMs?: number;
  /**
   * Orleans `CollectionOptions.DeactivationTimeout`: force deactivation to
   * proceed once this long has passed since it was requested, even if
   * `onDeactivate` hasn't settled — see
   * {@link ActivationData.awaitWithDeactivationTimeout}.
   */
  deactivationTimeoutMs?: number;
  /** Sink for soft-limit/stuck-turn/deactivation-timeout warnings; defaults to discarding them. */
  logger?: Logger;
  /**
   * Called once this activation has deactivated itself because its blocking
   * turn is stuck (`maxRequestProcessingTimeMs` exceeded — Orleans
   * `DeactivateStuckActivation`/`ActivationUnresponsive`). By the time this
   * fires `state` is already `"invalid"` and the `deactivationSteps`'
   * unregister and removal steps have run, so the next call for this grain
   * id activates a fresh one instead of finding this one again. A
   * notification only: the `Catalog` does not need it.
   */
  onStuck?: (activation: ActivationData) => void;
  /**
   * The parts of a deactivation owned outside the activation — supplied by the
   * `Catalog` (and, through it, the `ClusterNode`) and run by the activation's
   * own pipeline ({@link ActivationData.deactivate}) exactly once, in Orleans'
   * `FinishDeactivating` order. Absent for an activation built on its own.
   */
  deactivationSteps?: DeactivationSteps;
}

/**
 * What {@link ActivationData.deactivate}'s pipeline delegates (Orleans: the
 * `IActivationMigrationManager`, the `GrainLocator` and the catalog's
 * `UnregisterMessageTarget`). Each step is contained: one that throws is
 * logged and the pipeline carries on.
 */
export interface DeactivationSteps {
  /**
   * Dehydrate `activation` and hand it to `target`, or to a placement-chosen
   * silo when `target` is undefined (Orleans `StartMigrationAsync`, with
   * `ForwardingAddress` as the explicit target). Resolves to whether the target
   * accepted.
   */
  migrate?: (activation: ActivationData, target: SiloAddress | undefined) => Promise<boolean>;
  /** Unregister from the grain directory and drop any cached address. */
  unregister?: (activation: ActivationData) => void;
  /** Remove from the catalog, so the next lookup activates afresh. */
  remove?: (activation: ActivationData) => void;
  /** Dispose the grain instance and unbind its state. */
  dispose?: (activation: ActivationData) => void | Promise<void>;
}

/** Options for {@link ActivationData.deactivate}. */
export interface DeactivateOptions {
  /** Migrate to this silo instead of only deactivating (Orleans `ForwardingAddress`). */
  migrateTo?: SiloAddress;
  /** Passed through to `onDeactivate`. */
  signal?: AbortSignal;
}

/** How a deactivation ended; every caller that joined it sees the same outcome. */
export interface DeactivationOutcome {
  /** Whether the activation's state was handed to another silo that accepted it. */
  migrated: boolean;
}

/**
 * Orleans `ActivationLifetime.OnStopAsync`'s bound: a deactivation waits at
 * most this long for a transactional resource's lock or undecided prepared
 * record to clear, then proceeds.
 */
const DEACTIVATION_BLOCK_MAX_WAIT_MS = 5_000;
/** How often that wait re-checks the block (Orleans polls every 10 ms). */
const DEACTIVATION_BLOCK_POLL_MS = 10;

/** Split a `namespace/key` string; with no slash the whole string is the namespace and key is empty. */
function parseNamespaceKey(key: string): [namespace: string, key: string] {
  const slash = key.indexOf("/");
  return slash < 0 ? [key, ""] : [key.slice(0, slash), key.slice(slash + 1)];
}

/**
 * Runs a function in the async context this module was loaded in — the empty
 * root, with no `AsyncLocalStorage` store set — whatever context the caller is
 * in. The JS analogue of Orleans' `ExecutionContextSuppressor`, used so a
 * grain timer tick never inherits any async-local state from the call that
 * registered the timer (see `registerTimer`).
 */
const runInRootAsyncContext = AsyncLocalStorage.snapshot();

/**
 * The rejection a queued turn gets when the activation stops being valid
 * before the turn could start (see `ActivationData.admit`): a call or stream
 * delivery is then held for a reroute, a timer tick dropped. Never escapes.
 */
const EVICTED: unique symbol = Symbol("evicted");

/** The lifecycle operations (Orleans' `Command` types run by `ProcessOperationsAsync`). */
type OperationName = "activate" | "deactivate" | "dehydrate";

interface PendingOperation {
  readonly name: OperationName;
  /** Runs the operation; settles once it has finished, never rejects. */
  readonly run: () => Promise<void>;
}

/** How calls held on a deactivation that refuses them are answered (Orleans `DeactivationException`). */
interface Refusal {
  readonly error: () => unknown;
  /** The activation itself failed (its `onActivate` threw). */
  readonly activationFailed: boolean;
}

/**
 * One deactivation (Orleans' `Command.Deactivate` plus the activation's
 * deactivation fields): created by the first request, joined by every later
 * one.
 */
interface Deactivation {
  readonly reason: DeactivationReason;
  /** The state it began from (Orleans `PreviousState`): `onDeactivate` runs only from `valid`. */
  readonly previousState: ActivationState;
  readonly signal: AbortSignal | undefined;
  /** When it began (Orleans `DeactivationStartTime`), for stuck-deactivation detection. */
  readonly startedAtMs: number;
  /**
   * The migrating sub-state (Orleans `DehydrationContext`): set when a
   * migration was requested before or during the deactivation; `target` is
   * the explicit `ForwardingAddress`, undefined to let placement choose.
   */
  migration: { readonly target: SiloAddress | undefined } | undefined;
  /** Held calls are rejected with this instead of rerouted. */
  refusal: Refusal | undefined;
  /** `DeactivationTimeout`, armed when the deactivation was requested (Orleans `CancelAfter`). */
  readonly timeout: { readonly elapsed: Promise<void>; readonly handle: TimerHandle } | undefined;
  readonly completion: Promise<DeactivationOutcome>;
}

/**
 * The runtime's per-grain bookkeeping object and the grain's `GrainContext`.
 * It owns the turn scheduler and drives the activation lifecycle.
 */
export class ActivationData implements GrainContext {
  readonly id: GrainId;
  readonly activationId: ActivationId;
  readonly scheduler: TurnScheduler;
  /**
   * This activation as Orleans' `ICallChainReentrantGrainContext`: the target
   * of a `RequestContext.allowCallChainReentrancy` section opened by one of
   * its turns. Ambient for every turn (see `runWithCallChain`), so a section
   * always opens on the activation whose turn opened it.
   */
  private readonly callChainContext: CallChainReentrantGrainContext = {
    onEnterReentrantSection: (id) => this.scheduler.enterReentrantSection(id),
    onExitReentrantSection: (id) => this.scheduler.exitReentrantSection(id),
  };

  private _instance!: Grain;
  runtime!: GrainRuntime;
  private _state: ActivationState = "creating";

  /** Where this activation is in its lifecycle; see {@link ActivationState}. */
  get state(): ActivationState {
    return this._state;
  }

  /** Observes every lifecycle transition, after it has happened. Diagnostic only. */
  onStateChanged: ((from: ActivationState, to: ActivationState) => void) | undefined;

  get instance(): Grain {
    return this._instance;
  }

  /**
   * Binds the grain instance and, in the same step, resolves its
   * `mayInterleave` metadata (set by `@mayInterleave()`, absent by default)
   * onto the scheduler. Metadata is only known once the instance exists —
   * after the scheduler itself was constructed — so this late-binds it rather
   * than threading it through the constructor like `reentrant`.
   */
  set instance(value: Grain) {
    this._instance = value;
    this.scheduler.setMayInterleave(
      getGrainMetadata(value.constructor as GrainConstructor)?.mayInterleave,
    );
  }

  /** True when this activation failed to activate (its `onActivate` threw). */
  get activationFailed(): boolean {
    return this.deactivation?.refusal?.activationFailed === true;
  }

  /**
   * Resolved once the deactivation pipeline has finished, for every call held
   * while this activation was deactivating — see `rerouteOrReject` and
   * `awaitFinalized`.
   */
  private finalizationWaiters: Array<() => void> = [];

  /**
   * The deactivation under way or finished (Orleans' deactivation command and
   * completion source): set by the first request, joined by every later one.
   */
  private deactivation: Deactivation | undefined;
  private readonly steps: DeactivationSteps | undefined;

  /**
   * Lifecycle operations waiting to run (Orleans `_pendingOperations`), in
   * order, each only once no request turn is running.
   */
  private readonly operations: PendingOperation[] = [];
  /** The operation running now, if any; no request turn starts meanwhile. */
  private runningOperation: OperationName | undefined;

  /** Runs once before `onActivate` (e.g. read persistent state); set by the catalog. */
  preActivate: (() => Promise<void>) | undefined;

  /** Incoming grain-call filters wrapping each grain-method dispatch; set by the catalog. */
  incomingCallFilters: readonly IncomingGrainCallFilter[] = [];

  /**
   * Dev-mode `@readOnly` mutation guard (`GAP-READONLY-ENFORCEMENT`), opt-in
   * and off by default — set by the catalog from the silo builder's flag.
   * When `true`, every `@persistentState`, `@reducerState`, durable-journaling
   * (`@durableState`/`@durableDictionary`/`@durableList`/`@durableQueue`/
   * `@durableSet`) and `@transactionalState` field is swapped for a guarded
   * wrapper (`guardPersistentStateForReadOnly`, `guardReducerStateForReadOnly`,
   * `guardDurableFieldForReadOnly`, `guardTransactionalStateForReadOnly`) for
   * the duration of a turn whose `InvokeMethodOptions.readOnly` is `true`, so
   * a mutation attempt throws `ReadOnlyStateViolationError` instead of
   * silently succeeding; the real facets are restored once the turn ends.
   * Zero overhead when `false` (the default): no field swap, no proxy,
   * nothing to check.
   */
  readOnlyStateGuard = false;

  /** When migrating: the directed target silo (undefined lets placement choose). */
  migrationTarget: SiloAddress | undefined;
  /**
   * When migrating with no explicit `migrationTarget`: the caller's ambient
   * `RequestContext` placement hint (Orleans `IPlacementDirector.PlacementHintKey`),
   * captured at `requestMigration` time since it must be read while the
   * triggering call's `RequestContext` is still in scope — by the time the
   * deactivation pipeline migrates, that scope is long gone. Resolved against
   * live candidates at migration time (see `ClusterNode.migrateActivation`).
   */
  migrationHint: string | undefined;
  /**
   * The full ambient `RequestContext` bag in scope at `requestMigration` time
   * (Orleans `IGrainManagementExtension.MigrateOnIdle`'s
   * `RequestContext.CallContextData`), restored around `onDehydrate` in
   * {@link dehydrate} — captured for the same reason as `migrationHint`
   * (the pipeline that dehydrates runs after the triggering call's
   * `RequestContext` scope has ended), but carrying every key so a
   * migration participant can read app-specific ambient data such as a
   * caller-set "fail this dehydration" flag, not just the placement hint.
   */
  migrationRequestContext: Record<string, string> | undefined;
  /** State captured on the source silo to restore on the target; set by the catalog. */
  rehydrationBag: Record<string, unknown> | undefined;

  private lastActiveMs: number;
  private keepAliveUntilMs = 0;
  private readonly timers = new Set<GrainTimerImpl>();
  /** Handlers for pulling-agent stream delivery, keyed by `namespace/key`. */
  private readonly streamHandlers = new Map<string, StreamHandler<unknown>>();
  private readonly broadcastHandlers = new Map<string, BroadcastChannelHandler<unknown>>();
  /** Bound `GrainExtension` instances, keyed by interface id; see `getOrSetExtension`. */
  private readonly extensions = new Map<number, object>();
  /**
   * Auto-install factories for extension interfaces not yet bound, keyed by
   * interface id — set by the catalog (later task); unset here means an
   * un-bound extension call always throws
   * `GrainExtensionNotInstalledException` rather than auto-installing.
   */
  private extensionFactories?: Map<number, (activation: ActivationData) => object>;

  private readonly deactivationTimeoutMs: number | undefined;
  private readonly logger: Logger;
  private readonly onStuckHook: ((activation: ActivationData) => void) | undefined;
  /**
   * Every "noActivation" rejection handed to a call that never ran here —
   * held and released by `rerouteOrReject`, or rejected as queued behind a
   * stuck turn — so `isRerouteRejection` can tell them apart from a same-kind
   * rejection a call's own body threw (from a nested call).
   */
  private readonly rerouteRejections = new WeakSet<RejectionError>();
  private readonly maxRequestProcessingTimeMs: number | undefined;
  /** Timer that declares a deactivation stuck while calls are held on it; see `watchForStuckDeactivation`. */
  private stuckDeactivationTimer: TimerHandle | undefined;

  constructor(
    id: GrainId,
    private readonly time: TimeProvider,
    private readonly collectionAgeMs: number,
    reentrant: boolean,
    activationId: ActivationId = newActivationId(),
    options: ActivationOptions = {},
  ) {
    this.id = id;
    this.activationId = activationId;
    this.deactivationTimeoutMs = options.deactivationTimeoutMs;
    this.logger = options.logger ?? defaultLogger;
    this.onStuckHook = options.onStuck;
    this.steps = options.deactivationSteps;
    this.maxRequestProcessingTimeMs = options.maxRequestProcessingTimeMs;
    // Requests start only while this activation is valid and no lifecycle
    // operation is pending or running (Orleans `ProcessPendingRequests` checks
    // `State` first, and its message loop runs `ProcessOperationsAsync` only
    // when nothing is executing): so even on a fully reentrant grain no
    // request interleaves with `onActivate`, `onDeactivate` or a dehydration.
    this.scheduler = new TurnScheduler({
      reentrant,
      admissionGate: () =>
        this._state === "valid" &&
        this.runningOperation === undefined &&
        this.operations.length === 0,
      onDrained: () => this.processOperations(),
      time,
      logger: this.logger,
      grainId: id.toString(),
      ...(options.maxEnqueuedRequestsSoftLimit !== undefined
        ? { maxEnqueuedRequestsSoftLimit: options.maxEnqueuedRequestsSoftLimit }
        : {}),
      ...(options.maxEnqueuedRequestsHardLimit !== undefined
        ? { maxEnqueuedRequestsHardLimit: options.maxEnqueuedRequestsHardLimit }
        : {}),
      ...(options.maxRequestProcessingTimeMs !== undefined
        ? {
            maxRequestProcessingTimeMs: options.maxRequestProcessingTimeMs,
            onStuck: (turn) => this.handleStuckTurn(turn),
          }
        : {}),
    });
    this.lastActiveMs = time.now();
  }

  /**
   * Orleans `ActivationData.DeactivateStuckActivation`: the blocking turn has
   * been running past `maxRequestProcessingTimeMs` and cannot be preempted
   * (JS has no thread to interrupt — see `Turn.signal`'s doc), so this
   * activation is unrecoverable. Marks it `invalid` directly, WITHOUT
   * scheduling the `onDeactivate` hook — scheduling it would only queue
   * another turn behind the wedged one, which would never run either
   * (`docs/design-notes-parity-gaps.md`'s "(B) Stuck-turn watcher" already
   * settled on this outcome: "marked invalid and re-created"). `abandon`
   * removes this activation from the catalog and directory so the next call
   * activates a fresh one, and its move to `invalid` takes every queued turn
   * out to reroute. Returns the rejection the `TurnScheduler` keeps for any
   * turn scheduled on it directly from now on.
   */
  private handleStuckTurn(turn: Turn<unknown>): unknown {
    // Stop this orphaned activation's timers: every tick would only be
    // rejected by the scheduler (and logged) forever, keeping it alive.
    this.disposeTimers();
    this.logger.warn("activation is stuck processing a request; deactivating", {
      grainId: this.id.toString(),
      ...(turn.method !== undefined ? { method: turn.method } : {}),
    });
    const rejection = new RejectionError(
      `activation ${this.id.toString()} is stuck and has been deactivated`,
      "noActivation",
    );
    this.rerouteRejections.add(rejection);
    this.abandon();
    return rejection;
  }

  /**
   * Give this activation up as stuck (a wedged turn, or a wedged
   * `onDeactivate`): mark it invalid and run the pipeline's unregister and
   * removal steps now, once, so the next call for this grain activates a fresh
   * one, then release every held call to reroute. The disposal step is
   * skipped — it assumes the grain's code ran to an end, which it did not —
   * and a deactivation still awaiting the wedged hook finds this activation
   * already finalized when it returns, so nothing runs twice.
   */
  private abandon(): void {
    this.clearStuckDeactivationTimer();
    this.transition("invalid");
    void this.runStep("unregister", this.steps?.unregister);
    void this.runStep("remove", this.steps?.remove);
    this.onStuckHook?.(this);
    this.releaseFinalizationWaiters();
  }

  /**
   * Orleans `ProcessRequestsToInvalidActivation`'s Deactivating branch: a call
   * is being held on this deactivating activation (see `rerouteOrReject`), so
   * once the deactivation has run past `maxRequestProcessingTimeMs` it is
   * declared stuck (`IsStuckDeactivating`) and abandoned. Held calls sit
   * outside the turn scheduler (#91), so its own stuck-turn detection never
   * sees them waiting on a wedged `onDeactivate`; this is that check for them.
   * No-op without `maxRequestProcessingTimeMs`, or with neither
   * `deactivationSteps` nor an `onStuck` hook to act on it.
   */
  private watchForStuckDeactivation(): void {
    if (this.maxRequestProcessingTimeMs === undefined) return;
    if (this.onStuckHook === undefined && this.steps === undefined) return;
    if (this._state !== "deactivating" || this.deactivation === undefined) return;
    if (this.stuckDeactivationTimer !== undefined) return;
    const remainingMs =
      this.deactivation.startedAtMs + this.maxRequestProcessingTimeMs - this.time.now();
    if (remainingMs <= 0) {
      this.abandonStuckDeactivation();
      return;
    }
    this.stuckDeactivationTimer = this.time.setTimer(() => {
      this.stuckDeactivationTimer = undefined;
      if (this._state === "deactivating" && this.finalizationWaiters.length > 0) {
        this.abandonStuckDeactivation();
      }
    }, remainingMs);
  }

  /**
   * Orleans `AbandonStuckDeactivatingActivation`: give up on a deactivation
   * whose `onDeactivate` is wedged — mark this activation invalid, unregister
   * and remove it (`abandon`, as for a stuck turn), and release every held
   * call to reroute to a fresh activation. The wedged hook
   * keeps running in the background; whoever started the deactivation still
   * awaits it, and finds this activation already abandoned when it returns.
   */
  private abandonStuckDeactivation(): void {
    this.logger.warn("activation is stuck deactivating; abandoning it", {
      grainId: this.id.toString(),
      maxRequestProcessingTimeMs: this.maxRequestProcessingTimeMs,
    });
    this.abandon();
  }

  private clearStuckDeactivationTimer(): void {
    if (this.stuckDeactivationTimer !== undefined)
      this.time.clearTimer(this.stuckDeactivationTimer);
    this.stuckDeactivationTimer = undefined;
  }

  /**
   * True when `error` is a rejection this activation handed a call it never
   * ran — queued behind a turn judged stuck, or held while deactivating and
   * released once that finished (`rerouteOrReject`). Such a call is safe to
   * re-deliver to a fresh activation; any other error, including a
   * "noActivation" rejection a call's own body threw, is not (a resend would
   * run it twice).
   */
  isRerouteRejection(error: unknown): boolean {
    return error instanceof RejectionError && this.rerouteRejections.has(error);
  }

  /**
   * Move to `to`, which {@link ACTIVATION_TRANSITIONS} must allow. Leaving the
   * states that may still admit work (`creating`, `activating`, `valid`)
   * takes every queued turn out of the scheduler (Orleans
   * `ProcessRequestsToInvalidActivation`): a queued call or stream delivery
   * is held for a reroute or refused, a queued timer tick dropped — see
   * `admit`.
   */
  private transition(to: ActivationState): void {
    const from = this._state;
    assertActivationTransition(from, to);
    this._state = to;
    this.onStateChanged?.(from, to);
    if (!this.mayAdmitWork) this.scheduler.evictQueued(() => EVICTED);
  }

  /** Whether work may still be admitted (it waits until `valid` if not there yet). */
  private get mayAdmitWork(): boolean {
    return this._state === "creating" || this._state === "activating" || this._state === "valid";
  }

  /**
   * The one admission gate for every kind of work on this activation — grain
   * calls, system and extension calls and stream deliveries (`request`), and
   * grain timer ticks (`tick`). Work reaching an activation that may still
   * admit it is queued on the scheduler, which starts it only once the
   * activation is valid and no lifecycle operation is pending or running
   * (Orleans `ProcessPendingRequests`). Otherwise, and for work that was
   * still queued when the activation stopped being valid, the outcome is the
   * same for every kind: a request is held and rerouted once the
   * deactivation has finished (or refused, see `rerouteOrReject`), never
   * occupying the scheduler; a tick is dropped without running grain code.
   */
  private admit<R>(kind: "request" | "tick", turn: Turn<R>): Promise<R | undefined> {
    const refused = (): Promise<undefined> =>
      kind === "tick" ? Promise.resolve(undefined) : this.rerouteOrReject();
    if (!this.mayAdmitWork) return refused();
    return this.scheduler.schedule(turn).catch((error: unknown) => {
      if (error === EVICTED) return refused();
      throw error;
    });
  }

  /**
   * Queue a lifecycle operation (Orleans `ScheduleOperation`). Operations run
   * one at a time, in order, each only once no request turn is running, and
   * no request turn starts while one is pending or running — on a reentrant
   * grain too.
   */
  private scheduleOperation<T>(name: OperationName, body: () => Promise<T>): Promise<T> {
    return new Promise<T>((resolve, reject) => {
      this.operations.push({
        name,
        run: () => Promise.resolve().then(body).then(resolve, reject),
      });
      this.processOperations();
    });
  }

  /**
   * Orleans `ProcessOperationsAsync`, reached whenever nothing is executing:
   * run the next pending operation, and once the queue is empty let the
   * scheduler admit requests again (if the activation is valid by then).
   */
  private processOperations(): void {
    if (this.runningOperation !== undefined) return;
    // A request turn is running: the scheduler's `onDrained` resumes here.
    if (this.scheduler.isExecuting) return;
    const next = this.operations.shift();
    if (next === undefined) return;
    this.runningOperation = next.name;
    void next.run().finally(() => {
      this.runningOperation = undefined;
      this.processOperations();
      if (this.runningOperation === undefined) this.scheduler.reevaluate();
    });
  }

  /**
   * Queue the activate operation (Orleans `Command.Activate`): state binding
   * or rehydration (`preActivate`), then `onActivate`, before any work runs.
   */
  beginActivate(reason: ActivationReason): void {
    this.transition("activating");
    void this.scheduleOperation("activate", () => this.activate(reason));
  }

  private async activate(reason: ActivationReason): Promise<void> {
    try {
      if (this.preActivate !== undefined) await this.preActivate();
      // No call-chain id of its own (the call that triggered this activation
      // is a different chain), but a section `onActivate` opens lands on this
      // activation.
      await runWithCallChain(undefined, this.callChainContext, () =>
        this.instance.onActivate(reason),
      );
    } catch (error) {
      this.failActivation(error);
      return;
    }
    // A deactivation that began while activating (a silo stop, or the grain
    // asking to go away from `onActivate`) owns the state from here, as in
    // Orleans: only an undisturbed activation goes valid.
    if (this._state === "activating") this.transition("valid");
  }

  /**
   * Activation failed: the first call that cannot proceed — and every one
   * queued behind it — surfaces the ORIGINAL error (Orleans parity) rather than
   * a generic one, and the activation goes through the deactivation pipeline
   * (Orleans `Deactivate(ActivationFailed)`, no `OnDeactivateAsync` since it
   * never became valid) so it is unregistered, removed and disposed now.
   */
  private failActivation(error: unknown): void {
    const refusal: Refusal = { error: () => error, activationFailed: true };
    // A deactivation that began during activation finishes the job; its held
    // calls now get the activation's error too.
    if (this.deactivation !== undefined) {
      this.deactivation.refusal = refusal;
      return;
    }
    void this.startDeactivation(
      { code: "runtime-requested", description: "activation failed" },
      undefined,
      refusal,
      "failed",
    );
  }

  invoke(req: InvocationRequest, opts?: InvokeCallOptions): Promise<unknown> {
    this.touch();
    // Composed once per call: the caller's own `opts.signal` plus whatever
    // `AbortSignal` a `req.deadline` (ambient or freshly set — see
    // `InvokeCallOptions.deadlineMs`) derives, on THIS activation's own clock
    // (`deadlineSignal`, fake-clock-driven in tests). `dispose()` clears the
    // deadline timer once the turn settles either way, so a call that
    // finishes before its deadline never leaks one.
    const deadline =
      req.deadline === undefined ? undefined : deadlineSignal(req.deadline, this.time);
    const signal = combineSignals([opts?.signal, deadline?.signal]);
    return this.admit("request", {
      options: req.options,
      ...(req.reentrancyId !== undefined ? { reentrancyId: req.reentrancyId } : {}),
      method: req.method,
      args: req.args,
      ...(signal !== undefined ? { signal } : {}),
      run: () => {
        // A wire-arrived request whose caller has already stopped waiting
        // (its time-to-live ran out while it sat in this activation's
        // queue) is not run at all -- Orleans `InsideRuntimeClient.Invoke`
        // drops an expired message at exactly this point (issue #90).
        if (req.expiresAt !== undefined && this.time.now() >= req.expiresAt) {
          throw new GrainCallTimeoutError(
            `request ${req.method} to ${this.id.toString()} expired before it ran`,
          );
        }
        // A fresh, mutable copy of the incoming headers so `requestContext.set`
        // during the turn does not mutate the caller's bag but does flow to
        // downstream calls. Scoped via `runWithRequestContext` (the SAME
        // ambient store a non-grain/client caller uses), nested inside
        // `invocationContext.run` so both are in scope for the turn.
        const restoreReadOnlyGuard =
          this.readOnlyStateGuard && req.options?.readOnly === true
            ? this.applyReadOnlyGuard()
            : undefined;
        const invocation = runWithRequestContext(
          req.headers !== undefined ? { ...req.headers } : {},
          () =>
            // The request's call-chain id (if its chain opted in) stays
            // ambient so this turn's own calls carry it onward.
            runWithCallChain(req.reentrancyId, this.callChainContext, () =>
              invocationContext.run(
                {
                  senderId: req.sender,
                  ownerId: this.id,
                  transaction: req.transaction,
                  deadline: req.deadline,
                  signal,
                },
                () => this.callMethod(req),
              ),
            ),
        ).catch((error: unknown) => {
          this.deactivateOnInconsistentState(error);
          throw error;
        });
        return restoreReadOnlyGuard === undefined
          ? invocation
          : invocation.finally(restoreReadOnlyGuard);
      },
    }).finally(() => {
      this.touch();
      deadline?.dispose();
    });
  }

  /**
   * The handler for a delivered stream event, keyed by `namespace/key`. Falls
   * back to the grain's implicit-subscription observer (and caches its result)
   * when no handler was registered via `subscribe` — so an implicitly subscribed
   * grain (Orleans' `[ImplicitStreamSubscription]`) receives events without ever
   * calling `subscribe`. The fan-out only routes a stream to a grain that is an
   * explicit or declared-implicit subscriber, so an absent observer means drop.
   */
  private streamHandlerFor(streamKey: string): StreamHandler<unknown> | undefined {
    const existing = this.streamHandlers.get(streamKey);
    if (existing !== undefined) return existing;
    const observe = implicitStreamObserver(this.instance);
    if (observe === undefined) return undefined;
    const [namespace, key] = parseNamespaceKey(streamKey);
    const handler = observe(namespace, key);
    // A decline (`undefined`) is not cached: `probeStreamSubscription` needs
    // `observe` re-invoked so it can tell "declined" apart from "no observer
    // at all" (see its doc comment), and a declined admin subscription is
    // removed by its caller anyway, so there is nothing to cache toward.
    if (handler !== undefined) this.streamHandlers.set(streamKey, handler);
    return handler;
  }

  /**
   * Resolve (and cache) this activation's `STREAM_SUBSCRIPTION_OBSERVER` for
   * `streamKey`, without delivering anything — the confirmation step of an
   * administrative subscription (Orleans `IStreamSubscriptionObserver.OnSubscribed`).
   * `true` when the grain accepts (or never declared an observer at all, so
   * there is nothing to decline); `false` when it declared one and explicitly
   * declined (returned `undefined`) — the caller then drops the subscription.
   */
  private probeStreamSubscription(streamKey: string): boolean {
    if (implicitStreamObserver(this.instance) === undefined) return true;
    return this.streamHandlerFor(streamKey) !== undefined;
  }

  /**
   * The handler for a delivered broadcast-channel item, keyed by `namespace/key`,
   * resolved (and cached) from the grain's `BROADCAST_CHANNEL_OBSERVER` member.
   * An absent observer means the grain declared the implicit subscription but
   * exposes no handler, so the item is dropped — mirroring `streamHandlerFor`.
   */
  private broadcastHandlerFor(channelKey: string): BroadcastChannelHandler<unknown> | undefined {
    const existing = this.broadcastHandlers.get(channelKey);
    if (existing !== undefined) return existing;
    const observe = broadcastChannelObserver(this.instance);
    if (observe === undefined) return undefined;
    const [namespace, key] = parseNamespaceKey(channelKey);
    const handler = observe(namespace, key);
    this.broadcastHandlers.set(channelKey, handler);
    return handler;
  }

  /**
   * Get this activation's bound `GrainExtension` instance for `interfaceId`,
   * lazily creating it via `factory` on first use. Idempotent: a second call
   * for the same interface id returns the SAME instance and never re-runs
   * `factory` (see `GrainRuntime.getOrSetExtension`).
   */
  getOrSetExtension<T extends object>(interfaceId: number, factory: () => T): T {
    const existing = this.extensions.get(interfaceId);
    if (existing !== undefined) return existing as T;
    const created = factory();
    this.extensions.set(interfaceId, created);
    return created;
  }

  /**
   * Set the catalog's per-interface auto-install factories, used by
   * `callMethod` to bind an extension on first call rather than requiring the
   * grain to have called `getOrSetExtension` beforehand. Unset by default
   * (wired by a later silo-builder task); an un-bound extension call with no
   * factory configured throws `GrainExtensionNotInstalledException`.
   */
  setExtensionFactories(factories: Map<number, (activation: ActivationData) => object>): void {
    this.extensionFactories = factories;
  }

  /** Bind a pulling-agent stream handler so a delivered `StreamConsumer` turn reaches it. */
  setStreamHandler(streamKey: string, handler: StreamHandler<unknown>): void {
    this.streamHandlers.set(streamKey, handler);
  }

  clearStreamHandler(streamKey: string): void {
    this.streamHandlers.delete(streamKey);
  }

  /**
   * Run a stream delivery callback as a turn, through the same admission gate
   * as a grain call: a delivery reaching this activation once it is
   * deactivating is held and then rejected with a reroute rejection, never
   * run here, so the stream redelivers it to the next activation.
   */
  runStreamTurn(callback: () => Promise<void>): Promise<unknown> {
    return this.admit("request", { options: {}, run: callback });
  }

  /** Register a non-durable timer that fires as a turn; cancelled on deactivation. */
  registerTimer(
    callback: () => Promise<void>,
    due: Duration,
    period?: Duration,
    options?: TimerOptions,
  ): GrainTimer {
    // An interleaving timer's turns carry `alwaysInterleave` so the callback can
    // run while a non-reentrant call awaits it (Orleans' Interleave option);
    // otherwise the turn is exclusive like any grain call.
    const turnOptions: InvokeMethodOptions = options?.interleave ? { alwaysInterleave: true } : {};
    const timer = new GrainTimerImpl(
      this.time,
      // Through the same admission gate as every other kind of work: a tick
      // that comes due, or is still queued, once this activation is no longer
      // valid is dropped without running grain code.
      (cb) =>
        this.admit("tick", {
          options: turnOptions,
          // Orleans creates the timer under `ExecutionContextSuppressor`
          // ("Avoid capturing async locals", GrainTimer.cs:42-46) so a tick
          // never inherits the registering call's ambient state. `setTimeout`
          // otherwise carries the enclosing turn's `AsyncLocalStorage`
          // snapshot into every fire, so without this a tick would run under
          // the ORIGINAL registering call's call-chain id, deadline,
          // transaction and RequestContext headers rather than its own. Give
          // every tick a fresh, empty RequestContext and an `InvocationContext`
          // scoped to this activation with no inherited transaction, deadline
          // or signal — the same shape a self-initiated call would get.
          // Resetting only Thresh's own two stores is not enough: any other
          // async-local context (OpenTelemetry's active span, a host's
          // logging scope) would still leak in, so the tick first returns to
          // the empty root async context, then scopes Thresh's stores fresh.
          run: () => {
            const tick = runInRootAsyncContext(() =>
              runWithRequestContext({}, () =>
                // No call-chain id (Orleans asserts `ReentrancyId` is empty in
                // a tick, `GrainTimerTests`).
                runWithCallChain(undefined, this.callChainContext, () =>
                  invocationContext.run({ senderId: undefined, ownerId: this.id }, cb),
                ),
              ),
            );
            // Orleans' `IsKeepAlive` (GrainTimer.cs:81) resets the idle timer
            // when a keep-alive tick's message completes
            // (`ActivationData.OnCompletedRequest`, ActivationData.cs:1486);
            // an ordinary (non-keep-alive) tick does not touch it, so by
            // itself it never postpones collection (see `TimerOptions.keepAlive`).
            const settled = tick.catch((error: unknown) => {
              this.deactivateOnInconsistentState(error);
              throw error;
            });
            if (options?.keepAlive === true) return settled.finally(() => this.touch());
            return settled;
          },
        }),
      callback,
      due,
      period,
      // Mirrors Orleans' `TimerQueueTimer.TimerTick` catch-and-log: a timer
      // callback throw (or a scheduler admission rejection) is logged and
      // the timer keeps ticking rather than crashing the silo or going
      // silent, matching how `onDeactivate` failures are handled below.
      (error) =>
        this.logger.warn("grain timer callback failed", {
          grainId: this.id.toString(),
          error,
        }),
    );
    this.timers.add(timer);
    return timer;
  }

  /**
   * Deactivate this activation — the ONE entry point for every deactivation
   * (Orleans `ActivationData.Deactivate`): idle collection, an explicit
   * `deactivateOnIdle`, a migration, a failed or lost activation, silo
   * shutdown. The first call starts the pipeline; every later call joins it
   * and resolves with the same outcome once it has finished (Orleans'
   * deactivation completion source), so no caller ever finalizes under
   * another caller's running hook.
   *
   * Deactivation is a state transition the moment it is requested (Orleans
   * `Deactivate` sets `Deactivating` at once): admission stops, queued and
   * arriving calls are held to be rerouted once it has finished, and the
   * pipeline itself is a lifecycle operation that starts once the running
   * request turns have drained — so it never interleaves with one, on a
   * reentrant grain too.
   *
   * The pipeline runs Orleans' `FinishDeactivating` order: timers disposed,
   * then `onDeactivate` (only if the activation was valid when deactivation
   * began), then — if a migration was requested, before or during the hook,
   * or `migrateTo` names a target — the dehydrate and hand-off
   * (`DeactivationSteps.migrate`), then directory unregister, catalog
   * removal, and instance disposal with state unbinding, exactly once.
   * `migrateTo` on a call that joins a deactivation already under way
   * changes nothing: that deactivation's outcome is returned.
   */
  deactivate(
    reason: DeactivationReason,
    options: DeactivateOptions = {},
  ): Promise<DeactivationOutcome> {
    return this.startDeactivation(
      reason,
      options.migrateTo !== undefined ? { target: options.migrateTo } : undefined,
      undefined,
      "deactivating",
      options.signal,
    );
  }

  private startDeactivation(
    reason: DeactivationReason,
    migration: Deactivation["migration"],
    refusal: Refusal | undefined,
    to: "deactivating" | "failed",
    signal?: AbortSignal,
  ): Promise<DeactivationOutcome> {
    if (this.deactivation !== undefined) return this.deactivation.completion;
    // Stuck and abandoned: nothing left to run (Orleans: `Deactivate` on an
    // invalid activation returns).
    if (this._state === "invalid") return Promise.resolve({ migrated: false });
    let complete!: (outcome: DeactivationOutcome) => void;
    const deactivation: Deactivation = {
      reason,
      previousState: this._state,
      signal,
      startedAtMs: this.time.now(),
      migration,
      refusal,
      timeout: this.armDeactivationTimeout(),
      completion: new Promise<DeactivationOutcome>((resolve) => (complete = resolve)),
    };
    this.deactivation = deactivation;
    this.transition(to);
    void this.scheduleOperation("deactivate", () => this.runDeactivation(deactivation)).then(
      complete,
    );
    return deactivation.completion;
  }

  private armDeactivationTimeout(): Deactivation["timeout"] {
    const timeoutMs = this.deactivationTimeoutMs;
    if (timeoutMs === undefined) return undefined;
    let handle!: TimerHandle;
    const elapsed = new Promise<void>((resolve) => {
      handle = this.time.setTimer(resolve, timeoutMs);
    });
    return { elapsed, handle };
  }

  /** The deactivate operation (Orleans `FinishDeactivating`); see `deactivate`. */
  private async runDeactivation(deactivation: Deactivation): Promise<DeactivationOutcome> {
    this.disposeTimers();
    let migrated = false;
    try {
      // Orleans calls `OnDeactivateAsync` only when the previous state was
      // Valid: a failed activation, or one stopped before it went valid, gets
      // no hook.
      if (deactivation.previousState === "valid") {
        await this.awaitWithDeactivationTimeout(
          this.invokeDeactivateHook(deactivation.reason, deactivation.signal).catch(
            (error: unknown) => this.reportHookFailure(error),
          ),
          deactivation,
        );
      }
      await this.awaitDeactivationBlockCleared();
      if (
        deactivation.migration !== undefined &&
        deactivation.refusal === undefined &&
        this._state === "deactivating" &&
        this.steps?.migrate !== undefined
      ) {
        migrated = await this.steps.migrate(this, deactivation.migration.target).catch(() => false);
      }
    } catch (error) {
      this.logger.warn("deactivation failed; finalizing anyway", {
        grainId: this.id.toString(),
        error,
      });
    }
    if (deactivation.timeout !== undefined) this.time.clearTimer(deactivation.timeout.handle);
    await this.finishDeactivation();
    return { migrated };
  }

  /**
   * Orleans `ActivationLifetime.OnStopAsync`: while a transactional-state
   * facet blocks deactivation (`isDeactivationBlocked`), wait for it to clear
   * — polled every {@link DEACTIVATION_BLOCK_POLL_MS} on this activation's
   * clock, as Orleans polls every 10 ms — but never longer than
   * {@link DEACTIVATION_BLOCK_MAX_WAIT_MS}, then proceed regardless. Runs after
   * `onDeactivate` and before the migration hand-off, where Orleans stops the
   * grain lifecycle. Synchronous checks on each tick, so a fake clock drives
   * it deterministically.
   */
  private awaitDeactivationBlockCleared(): Promise<void> {
    if (!this.isDeactivationBlocked) return Promise.resolve();
    const startedAtMs = this.time.now();
    return new Promise<void>((resolve) => {
      const poll = (): void => {
        const waitedMs = this.time.now() - startedAtMs;
        if (!this.isDeactivationBlocked || waitedMs >= DEACTIVATION_BLOCK_MAX_WAIT_MS) {
          resolve();
          return;
        }
        this.time.setTimer(
          poll,
          Math.min(DEACTIVATION_BLOCK_POLL_MS, DEACTIVATION_BLOCK_MAX_WAIT_MS - waitedMs),
        );
      };
      poll();
    });
  }

  /** Orleans `DisposeTimers`: no grain timer ticks once a deactivation has begun. */
  private disposeTimers(): void {
    for (const timer of this.timers) timer.dispose();
    this.timers.clear();
  }

  /**
   * Log rather than silently discard: an `onDeactivate` hook that throws must
   * not stop deactivation (the activation still needs to be finalized and
   * freed).
   */
  private reportHookFailure(error: unknown): void {
    this.logger.warn("onDeactivate hook failed", { grainId: this.id.toString(), error });
  }

  /** `onDeactivate` itself, inside its span. */
  private invokeDeactivateHook(
    reason: DeactivationReason,
    signal: AbortSignal | undefined,
  ): Promise<void> {
    return withOnDeactivateSpan(
      {
        grainId: this.id.toString(),
        grainType: this.id.type,
        siloId: this.localSiloId(),
        activationId: this.activationId,
        reason: formatDeactivationReason(reason),
      },
      () =>
        runWithCallChain(undefined, this.callChainContext, () =>
          this.instance.onDeactivate(reason, signal),
        ),
    );
  }

  /**
   * Await `hookPromise` (the `onDeactivate` call), but never past
   * `deactivation.timeout` — `deactivationTimeoutMs` counted from when the
   * deactivation was requested (Orleans `CollectionOptions.DeactivationTimeout`,
   * enforced upstream via `cts.CancelAfter(DeactivationTimeout)` in
   * `Deactivate` and `OnDeactivateAsync(...).WaitAsync(ct)`). JS has no way to
   * abort an in-flight `async` function without a cooperating cancellation
   * signal, so a hung hook keeps running in the background after this returns
   * — but the pipeline proceeds to force-invalidate the activation (directory
   * unregister, cleanup) instead of blocking forever on it. Waits
   * indefinitely when no timeout is configured.
   */
  private async awaitWithDeactivationTimeout(
    hookPromise: Promise<void>,
    deactivation: Deactivation,
  ): Promise<void> {
    if (deactivation.timeout === undefined) {
      await hookPromise;
      return;
    }
    const timedOut = await Promise.race([
      hookPromise.then(() => false),
      deactivation.timeout.elapsed.then(() => true),
    ]);
    if (timedOut) {
      this.logger.warn("onDeactivate exceeded DeactivationTimeout; forcing deactivation", {
        grainId: this.id.toString(),
        deactivationTimeoutMs: this.deactivationTimeoutMs,
      });
    }
  }

  /**
   * The local silo's address, for the `OnDeactivate` span's `orleans.silo.id`
   * tag — best-effort: some lightweight test harnesses build an
   * `ActivationData` with no local-silo address configured on its runtime
   * (`GrainRuntimeImpl.localSiloAddress` throws in that case), and the
   * deactivate hook must still run rather than being swallowed by
   * deactivation pipeline's hook failure handler.
   */
  private localSiloId(): string {
    try {
      return this.runtime.localSiloAddress().toString();
    } catch {
      return "unknown";
    }
  }

  /**
   * The pipeline's last stage (Orleans: `GrainLocator.Unregister`,
   * `UnregisterMessageTarget`, `DisposeAsync`): move to `invalid`, then
   * unregister, remove and dispose — unless an abandonment as stuck already
   * moved it there and did the unregister and removal — and only then release
   * the calls held for a reroute, so they re-resolve against a catalog that no
   * longer holds this activation. Unregister and removal run synchronously
   * with the move to invalid.
   */
  private async finishDeactivation(): Promise<void> {
    if (this._state !== "invalid") {
      this.transition("invalid");
      this.clearStuckDeactivationTimer();
      // A timer registered after the pipeline began (by the hook itself)
      // never outlives the activation.
      this.disposeTimers();
      const unregistering = this.runStep("unregister", this.steps?.unregister);
      const removing = this.runStep("remove", this.steps?.remove);
      await unregistering;
      await removing;
      await this.runStep("dispose", this.steps?.dispose);
    }
    this.clearStuckDeactivationTimer();
    this.releaseFinalizationWaiters();
  }

  /**
   * Run one externally owned deactivation step, containing a failure: a user
   * `disposeInstance`/`deactivateState`/`onDeactivated` hook that throws is
   * logged and the pipeline carries on, so one bad grain never stops a sweep.
   * The step's synchronous part runs before this returns.
   */
  private runStep(
    name: string,
    step: ((activation: ActivationData) => void | Promise<void>) | undefined,
  ): Promise<void> {
    if (step === undefined) return Promise.resolve();
    const report = (error: unknown): void => {
      this.logger.warn("activation disposal failed during deactivation", {
        grainId: this.id.toString(),
        step: name,
        error,
      });
    };
    try {
      return Promise.resolve(step(this)).catch(report);
    } catch (error) {
      report(error);
      return Promise.resolve();
    }
  }

  private releaseFinalizationWaiters(): void {
    const waiters = this.finalizationWaiters;
    this.finalizationWaiters = [];
    for (const resolve of waiters) resolve();
  }

  /**
   * Resolves once this activation is fully torn down (the deactivation pipeline
   * has run) — immediately if that has already happened. Used by
   * `rerouteOrReject` to hold a call reaching a deactivating activation until
   * there is something settled to reroute it against.
   */
  private awaitFinalized(): Promise<void> {
    if (this._state === "invalid") return Promise.resolve();
    const finalized = new Promise<void>((resolve) => this.finalizationWaiters.push(resolve));
    this.watchForStuckDeactivation();
    return finalized;
  }

  /**
   * What a call or stream delivery gets when it reaches this activation once
   * it is no longer valid, or was still queued when it stopped being valid. A
   * deactivation that refuses its calls — a failed activation, or one that
   * asked to go away from its own `onActivate` and so never went valid
   * (Orleans answers those with "Forwarding failed") — rejects outright:
   * there is nothing to hold for or reroute to. Otherwise this HOLDS (Orleans
   * `ProcessRequestsToInvalidActivation`/`RerouteAllQueuedMessages`,
   * forwarding to `ForwardingAddress` on migration) until the deactivation
   * pipeline finishes, then signals the dispatcher to re-resolve — a fresh
   * directory lookup/placement by then reflects either the migrated
   * activation's new host or nothing at all, both of which `LocalDispatcher`
   * (see `isRerouteRejection`) and `DistributedDispatcher` (see
   * `isStaleActivationRejection`) retry against — rather than surfacing this
   * as an application error.
   */
  private rerouteOrReject(): Promise<never> {
    const refusal = this.deactivation?.refusal;
    if (refusal !== undefined) return Promise.reject(refusal.error());
    return this.awaitFinalized().then((): never => {
      const rejection = new RejectionError(
        `activation unavailable: ${this.id.toString()}`,
        "noActivation",
      );
      this.rerouteRejections.add(rejection);
      throw rejection;
    });
  }

  /**
   * Deactivate as soon as the running turns have drained (Orleans
   * `DeactivateOnIdle`, and the runtime's own requests such as an escaped
   * `InconsistentStateError`): the activation moves to `deactivating` now, so
   * nothing more is admitted, and the pipeline starts once nothing is
   * executing. Asked from `onActivate`, the activation never goes valid and
   * the calls waiting on it are refused rather than rerouted (Orleans
   * parity: `DeactivateOnIdleWhileActivate`). Joins a deactivation already
   * under way; a no-op once invalid.
   */
  requestDeactivation(reason?: DeactivationReason): void {
    const duringActivation = this._state === "activating";
    void this.startDeactivation(
      reason ?? {
        code: "application-requested",
        description: duringActivation
          ? "deactivateOnIdle requested during activation"
          : "deactivateOnIdle requested",
      },
      undefined,
      duringActivation
        ? {
            error: () => new GrainCallError(`activation unavailable: ${this.id.toString()}`),
            activationFailed: false,
          }
        : undefined,
      "deactivating",
    );
  }

  /**
   * Mirrors Orleans' `ActivationData.DelayDeactivation` (ActivationData.cs:486-509):
   * a non-positive `byMs` cancels any active keep-alive and reverts to normal
   * collection, rather than being folded in via `Math.max` (which could only
   * ever extend, never shorten or cancel, a previously requested keep-alive).
   * A positive `byMs` REPLACES whatever keep-alive was in effect — later,
   * shorter calls narrow it, matching upstream's unconditional assignment.
   */
  delayDeactivation(byMs: number): void {
    this.keepAliveUntilMs = byMs <= 0 ? 0 : this.time.now() + byMs;
  }

  /**
   * Migrate this activation once the running turns have drained (Orleans
   * `MigrateOnIdle` / `TryStartMigration`): it moves to `deactivating` with a
   * migration request now, so later calls are held and rerouted to wherever
   * it lands. Requested while a deactivation is already under way (from
   * `onDeactivate`, say), that deactivation migrates instead. `target`, when
   * given, is the directed destination; otherwise placement chooses, honouring
   * the caller's ambient placement hint.
   */
  requestMigration(target?: SiloAddress): void {
    if (this._state === "invalid" || this._state === "failed") return;
    this.migrationTarget = target;
    // No explicit target: capture the caller's ambient placement hint now,
    // while it's still in scope — the pipeline that actually migrates runs
    // after this call's RequestContext scope has ended.
    this.migrationHint = target === undefined ? RequestContext.get(PLACEMENT_HINT_KEY) : undefined;
    const store = requestContextStore();
    this.migrationRequestContext = store !== undefined ? { ...store } : undefined;
    if (this.deactivation !== undefined) {
      // Placement resolves `migrationTarget` (see `DeactivationSteps.migrate`).
      this.deactivation.migration ??= { target: undefined };
      return;
    }
    void this.startDeactivation(
      { code: "migrating", description: "migrating to another silo" },
      { target: undefined },
      undefined,
      "deactivating",
    );
  }

  /** Whether this activation is deactivating in order to migrate (the migrating sub-state). */
  get wantsMigration(): boolean {
    return this.deactivation?.migration !== undefined;
  }

  /**
   * Gather the activation's in-memory state into a transport-safe bag by running
   * each migration participant's `onDehydrate`. Inside a deactivation's
   * migration step (Orleans: `OnDehydrate` within `FinishDeactivating`) this
   * runs as part of that operation; called on its own it is queued as a
   * lifecycle operation. Either way no request turn runs alongside it.
   *
   * Wrapped in a `Dehydrate` span (Lifecycle source) ONLY when the activation
   * actually has migration participants — a grain with none (or one that
   * doesn't opt into migration at all) gets no span, matching upstream: an
   * activity is only started around `OnDehydrate` when a participant exists
   * (`ActivationData.OnDehydrate`, Orleans v10.1.0). `target`, when given, is
   * recorded as the `orleans.migration.target.silo` tag.
   */
  async dehydrate(target?: SiloAddress): Promise<Record<string, unknown>> {
    const participants = migrationParticipantsOf(this.instance);
    const gather = async (): Promise<Record<string, unknown>> => {
      const bag = new MigrationBag();
      const snapshot = (): Promise<void> => {
        runWithRequestContext(this.migrationRequestContext ?? {}, () => {
          for (const participant of participants) {
            try {
              participant.onDehydrate(bag);
            } catch {
              // Orleans (`ActivationData.OnDehydrate`): log-and-continue — a
              // participant that fails to dehydrate simply contributes no
              // state to the bag; migration proceeds and the target
              // reactivates fresh rather than the whole move aborting.
            }
          }
        });
        return Promise.resolve();
      };
      if (this.runningOperation === "deactivate") await snapshot();
      else await this.scheduleOperation("dehydrate", snapshot);
      return bag.data;
    };
    if (participants.length === 0) return gather();
    return withDehydrateSpan(
      {
        grainId: this.id.toString(),
        siloId: this.localSiloId(),
        activationId: this.activationId,
        ...(target !== undefined ? { targetSilo: target.toString() } : {}),
      },
      gather,
    );
  }

  /**
   * Restore migrated state into the (freshly bound) participants on the
   * target. Wrapped in a `Rehydrate` span (Lifecycle source) ONLY when the
   * activation actually has migration participants, mirroring `dehydrate`'s
   * guard. `previousRegistration`, when given, is recorded as the
   * `orleans.rehydrate.previousRegistration` tag (the source silo's prior
   * directory entry for this activation).
   *
   * A participant that throws is logged and the activation carries on without
   * the rest of the carried state, as Orleans' `RehydrateInternal` does
   * (log-and-continue, mirroring `dehydrate`): the migrated activation comes
   * up fresh on the target rather than failing.
   */
  async applyRehydration(previousRegistration?: GrainAddress): Promise<void> {
    try {
      await this.restoreRehydrationBag(previousRegistration);
    } catch (error) {
      this.logger.warn("rehydrating a migrated activation failed; continuing without its state", {
        grainId: this.id.toString(),
        error,
      });
    }
  }

  private async restoreRehydrationBag(previousRegistration?: GrainAddress): Promise<void> {
    if (this.rehydrationBag === undefined) return;
    const participants = migrationParticipantsOf(this.instance);
    const ctx = new MigrationBag(this.rehydrationBag);
    const restore = (): void => {
      for (const participant of participants) participant.onRehydrate(ctx);
    };
    if (participants.length === 0) {
      restore();
      return;
    }
    await withRehydrateSpan(
      {
        grainId: this.id.toString(),
        siloId: this.localSiloId(),
        activationId: this.activationId,
        ...(previousRegistration !== undefined
          ? {
              previousRegistration: `${previousRegistration.silo.toString()}/${previousRegistration.activationId}`,
            }
          : {}),
      },
      async () => restore(),
    );
  }

  /**
   * `ageLimitOverrideMs`, when given, replaces `collectionAgeMs` for this
   * check — the management grain's `ForceActivationCollection(ageLimit)`
   * (Orleans parity) sweeps with a caller-chosen age (e.g. zero, to collect
   * every currently-idle activation) rather than each activation's own
   * configured collection age.
   */
  isStale(ageLimitOverrideMs?: number): boolean {
    if (this._state !== "valid") return false;
    if (this.scheduler.busy) return false;
    const now = this.time.now();
    if (now < this.keepAliveUntilMs) return false;
    return now - this.lastActiveMs >= (ageLimitOverrideMs ?? this.collectionAgeMs);
  }

  /**
   * Whether a transactional-state facet holds a transaction lock or a prepared
   * record whose outcome is unknown (Orleans `ActivationLifetime.BlockDeactivation`).
   * It does not keep the activation out of collection or migration: the
   * deactivation pipeline waits a bounded time for it to clear
   * (`awaitDeactivationBlockCleared`).
   */
  get isDeactivationBlocked(): boolean {
    const instance = this._instance as Grain | undefined;
    if (instance === undefined) return false;
    const record = instance as unknown as Record<string, unknown>;
    return getTransactionalFields(instance).some((field) =>
      blocksDeactivation(unguardedTransactionalState(record[field.fieldName])),
    );
  }

  /**
   * Orleans deactivates an activation that lets an inconsistent-state
   * exception escape a call or timer tick (`InsideRuntimeClient.cs:326`),
   * rather than leave a stale or duplicate activation stuck, so the next call
   * gets a fresh one. Only the activation the error originated in: the marker
   * is cleared before the error travels on, so a caller that merely
   * propagates it is not deactivated too (`IsSourceActivation`,
   * InsideRuntimeClient.cs:326-329). Routed through `requestDeactivation`,
   * like `deactivateOnIdle()`: the activation stops admitting work now, the
   * pipeline runs once the running turns have drained, and every call that
   * arrives meanwhile is held and rerouted to a fresh activation rather than
   * served stale or rejected by this one mid-teardown.
   */
  private deactivateOnInconsistentState(error: unknown): void {
    if (!(error instanceof InconsistentStateError) || !error.isSourceActivation) return;
    error.isSourceActivation = false;
    this.requestDeactivation({ code: "application-error", description: error.message });
  }

  private touch(): void {
    this.lastActiveMs = this.time.now();
  }

  /**
   * Swap every `@persistentState`, `@reducerState`, durable-journaling and
   * `@transactionalState` field on the instance for a guarded wrapper,
   * returning a closure that puts the real facets back. Called only when
   * `readOnlyStateGuard` is enabled AND the turn is `readOnly` — see
   * `invoke()` — so a plain call, or any call on a silo that never opted in,
   * never allocates a proxy. Transaction-resource system calls (`prepare`/
   * `commit`/`abort`/`status`, routed through `invokeTransactionResource`)
   * are always-interleave, so one may run while this guard is in place;
   * `invokeTransactionResource` unwraps the guard to reach the real facet.
   */
  private applyReadOnlyGuard(): () => void {
    const record = this.instance as unknown as Record<string, unknown>;
    const saved: Array<readonly [string, unknown]> = [];

    for (const field of getPersistentFields(this.instance)) {
      const original = record[field.fieldName];
      if (original === undefined) continue;
      saved.push([field.fieldName, original]);
      record[field.fieldName] = guardPersistentStateForReadOnly(
        original as Parameters<typeof guardPersistentStateForReadOnly>[0],
        field.stateName,
      );
    }
    for (const field of getReducerFields(this.instance)) {
      const original = record[field.fieldName];
      if (original === undefined) continue;
      saved.push([field.fieldName, original]);
      record[field.fieldName] = guardReducerStateForReadOnly(
        original as Parameters<typeof guardReducerStateForReadOnly>[0],
        field.stateName,
      );
    }
    for (const field of getDurableFields(this.instance)) {
      const original = record[field.fieldName];
      if (original === undefined) continue;
      saved.push([field.fieldName, original]);
      record[field.fieldName] = guardDurableFieldForReadOnly(field.kind, original, field.stateName);
    }
    for (const field of getTransactionalFields(this.instance)) {
      const original = record[field.fieldName];
      if (original === undefined) continue;
      saved.push([field.fieldName, original]);
      record[field.fieldName] = guardTransactionalStateForReadOnly(
        original as Parameters<typeof guardTransactionalStateForReadOnly>[0],
        field.stateName,
      );
    }

    if (saved.length === 0) return () => undefined;
    return () => {
      for (const [fieldName, original] of saved) record[fieldName] = original;
    };
  }

  private async callMethod(req: InvocationRequest): Promise<unknown> {
    // Stream delivery is a system extension, not a grain method: route it to the
    // handler the grain registered when it subscribed. Already on a turn here.
    if (req.interfaceId === StreamConsumerInterface.id) {
      if (req.method === "confirmStreamSubscription") {
        const [streamKey] = req.args as [string];
        return this.probeStreamSubscription(streamKey);
      }
      const [streamKey, event, token] = req.args as [string, unknown, number];
      const handler = this.streamHandlerFor(streamKey);
      if (handler !== undefined) {
        await this.runSystemExtensionDelivery(req, () =>
          handler.onNext(event, new SequenceToken(token)),
        );
      }
      return undefined;
    }
    // Broadcast-channel delivery is likewise a system extension: route the
    // published item to the grain's broadcast observer.
    if (req.interfaceId === BroadcastConsumerInterface.id) {
      const [channelKey, item] = req.args as [string, unknown];
      const handler = this.broadcastHandlerFor(channelKey);
      if (handler !== undefined) {
        await this.runSystemExtensionDelivery(req, () =>
          Promise.resolve(handler.onPublished(item)),
        );
      }
      return undefined;
    }
    // Durable-job delivery is a system extension: run one attempt of
    // the job on the grain's `DURABLE_JOB_HANDLER`, as a turn here. A grain with
    // no handler throws, which the executor catches as `Failed` (the retry policy
    // then drops it) — rather than treating a missing handler as completion, and
    // without putting a non-serializable Error object in the cross-silo result.
    if (req.interfaceId === DurableJobConsumerInterface.id) {
      const [job] = req.args as [JobRunContext];
      const handler = durableJobHandler(this.instance);
      if (handler === undefined) {
        throw new GrainCallError(`grain ${this.id.toString()} has no durable job handler`);
      }
      return await handler(job);
    }
    // Transaction-resource extension: drive a named transactional state's
    // prepare/commit/abort for the agent on another silo, found by state name.
    if (req.interfaceId === TransactionResourceInterface.id) {
      return await this.invokeTransactionResource(req);
    }
    // GrainExtension dispatch: an interface marked `extension` in the registry
    // routes to a per-activation object bound via `getOrSetExtension`
    // (auto-installed from `extensionFactories` if not yet bound), NOT to
    // `this.instance` — an orthogonal method surface, dispatched by interface.
    // Runs through the SAME incoming call-filter pipeline as the grain-method
    // path below (Orleans parity: filters wrap extension calls too).
    const iface = getGrainInterface(req.interfaceId);
    if (iface?.extension === true) {
      return await this.invokeExtension(req, iface.name);
    }
    // Only the grain type's declared method surface is dispatched: a runtime
    // hook, a framework base-class member or an Object.prototype name is
    // refused without touching the instance (see `remote-method-surface`).
    if (!isRemotelyCallable(this.instance, req.method)) {
      throw new GrainCallError(`grain ${this.id.toString()} has no method ${req.method}`);
    }
    const fn = (this.instance as unknown as Record<string, unknown>)[req.method];
    if (typeof fn !== "function") {
      throw new GrainCallError(`grain ${this.id.toString()} has no method ${req.method}`);
    }
    const method = fn as (...args: unknown[]) => unknown;
    // Bind any wire-arrived cancellation-token placeholders to live tokens,
    // in place on `req.args`, BEFORE the method runs — once, here, so both the
    // no-filter and filtered dispatch paths below see the bound arguments
    // (`context.args` is a copy of `req.args` taken after this call).
    this.bindCancellationTokens(req.args);
    // The grain's own filter (if it declares one) runs innermost — after the
    // silo-wide filters, just before the method (Orleans parity).
    const own = grainIncomingFilter(this.instance);
    const filters =
      own === undefined ? this.incomingCallFilters : [...this.incomingCallFilters, own];
    if (filters.length === 0) {
      return await method.apply(this.instance, req.args);
    }
    // Run the grain-method dispatch through the incoming call-filter pipeline;
    // filters see the context and proceed via `invoke()` (Orleans parity).
    // `headers` is the SAME object `invocationContext`'s AsyncLocalStorage
    // store holds for this turn (installed by `invoke()`) rather than a fresh
    // copy of `req.headers` — so a filter's write is visible to the grain
    // method body via `requestContext.get()` (Orleans parity: RequestContext
    // set in a filter flows into the handler it wraps).
    const context: IncomingGrainCallContext = {
      target: this.id,
      source: req.sender,
      interfaceId: req.interfaceId,
      interfaceName: getGrainInterface(req.interfaceId)?.name ?? "",
      methodName: req.method,
      args: [...req.args],
      result: undefined,
      headers: requestContextStore() ?? {},
      grain: this.instance,
      invoke: () => Promise.resolve(),
    };
    return await runCallFilters(filters, context, () =>
      Promise.resolve(method.apply(this.instance, context.args)),
    );
  }

  /**
   * Deliver a system-extension event (a stream `onNext`, a broadcast-channel
   * `onPublished`) through the SAME incoming call-filter pipeline as an
   * ordinary grain method (Orleans parity: an `IIncomingGrainCallFilter` wraps
   * stream and broadcast deliveries too, so a grain that filters its own calls
   * — e.g. `StreamInterceptionGrain` — can observe and react to a delivery it
   * just processed). A grain with no filters (the common case) delivers
   * directly, so this is behaviour-neutral for every non-filtering consumer.
   */
  private runSystemExtensionDelivery(
    req: InvocationRequest,
    deliver: () => Promise<void>,
  ): Promise<void> {
    return this.deliverThroughIncomingFilters(
      { interfaceId: req.interfaceId, methodName: req.method, source: req.sender, args: req.args },
      deliver,
    );
  }

  /**
   * Run a memory-stream `onNext` delivery (which reaches this activation as a
   * scheduler turn, not a `callMethod` dispatch — see
   * `ActivationStreamProvider`) through the incoming call-filter pipeline, so a
   * grain subscribed to a memory stream sees the same filter wrapping as one
   * fed by a pulling-agent provider (whose delivery flows through `callMethod`'s
   * `StreamConsumerInterface` branch). Behaviour-neutral without filters.
   */
  runStreamDelivery(deliver: () => Promise<void>): Promise<unknown> {
    return this.runStreamTurn(() =>
      this.deliverThroughIncomingFilters(
        {
          interfaceId: StreamConsumerInterface.id,
          methodName: "onNext",
          source: this.id,
          args: [],
        },
        deliver,
      ),
    );
  }

  private async deliverThroughIncomingFilters(
    seed: {
      interfaceId: number;
      methodName: string;
      source: GrainId | undefined;
      args: readonly unknown[];
    },
    deliver: () => Promise<void>,
  ): Promise<void> {
    const own = grainIncomingFilter(this.instance);
    const filters =
      own === undefined ? this.incomingCallFilters : [...this.incomingCallFilters, own];
    if (filters.length === 0) {
      await deliver();
      return;
    }
    const context: IncomingGrainCallContext = {
      target: this.id,
      source: seed.source,
      interfaceId: seed.interfaceId,
      interfaceName: getGrainInterface(seed.interfaceId)?.name ?? "",
      methodName: seed.methodName,
      args: [...seed.args],
      result: undefined,
      headers: requestContextStore() ?? {},
      grain: this.instance,
      invoke: () => Promise.resolve(),
    };
    await runCallFilters(filters, context, () => Promise.resolve(deliver()));
  }

  /**
   * Replace each `CancellationTokenPlaceholder` in `args` (produced by
   * `decodeValue`, which has no activation context to bind a live signal) with
   * a real `GrainCancellationToken` whose `signal` is this activation's own
   * cancellation extension's controller for that `tokenId` — so a later
   * `cancelRemoteToken(tokenId)` call (routed to the SAME extension instance
   * via `getOrSetExtension`'s idempotent binding) fires it. A placeholder that
   * arrived already-cancelled (`cancelled: true`, set when `cancel()` ran
   * before the call was even sent) pre-aborts the controller here, so the
   * grain method sees an already-fired signal instead of racing one.
   *
   * Also records each bound token's signal onto this turn's
   * `invocationContext.tokenSignals` (Orleans has no analogue — see
   * `docs/deviations.md`), so `GrainRuntime.getCancellationSignal()` observes
   * it too — composed there with the ambient `signal`, but deliberately kept
   * OUT of `signal` itself: `GrainCancellationToken` is purely cooperative,
   * so it must never propagate onto a downstream call's `InvokeCallOptions
   * .signal` and start preemptively rejecting THAT call's admission the way a
   * genuine ambient signal/deadline does (see `InvocationContext
   * .tokenSignals`'s doc).
   */
  private bindCancellationTokens(args: unknown[]): void {
    const store = invocationContext.getStore();
    // Every cancellation value in the argument graph, not only one occupying its
    // own slot: the caller-side conversion (`GrainFactory`) reaches the same
    // depth, so an argument record carrying a signal must be unwrapped here or
    // the method would receive a token where its signature declares a signal.
    for (let i = 0; i < args.length; i++) {
      args[i] = mapCancellationValues(args[i], (found) => this.bindCancellationValue(found, store));
    }
  }

  /** Bind one cancellation value found by `bindCancellationTokens`'s walk. */
  private bindCancellationValue(
    found: CancellationValue,
    store: InvocationContext | undefined,
  ): unknown {
    // A SAME-SILO call never serializes its arguments, so an `asSignal` token
    // arrives as the token object itself; the method still declared an
    // `AbortSignal`, so unwrap it here too - otherwise what the callee
    // receives would depend on placement.
    if (found instanceof GrainCancellationToken) {
      return found.asSignal ? found.signal : found;
    }
    // A bare `AbortSignal` (a same-silo caller that bypassed the factory) is
    // already the shape the method declared.
    if (!(found instanceof CancellationTokenPlaceholder)) return found;
    // Auto-install through the catalog's registered factory (which wires
    // this silo's real cascade canceller — see `Catalog`/`ClusterNode`)
    // rather than calling `cancellationExtensionFactory` bare; a standalone
    // activation with no catalog-set factories falls back to a no-op
    // canceller, matching the pre-cascade single-hop behaviour.
    const ext = this.getOrSetExtension(
      ICancellationSourcesExtension.id,
      () =>
        (this.extensionFactories?.get(ICancellationSourcesExtension.id)?.(this) ??
          cancellationExtensionFactory(async () => {})) as CancellationSourcesExtension,
    );
    const controller = ext.getOrCreateController(found.tokenId);
    if (found.cancelled) controller.abort();
    const tokenId = found.tokenId;
    // The caller passed a plain `AbortSignal` in this slot (the grain-factory
    // converted it to a token so it could cross the wire at all), so the
    // method is owed an `AbortSignal` back, not a token.
    if (found.asSignal) return controller.signal;
    const token = new GrainCancellationToken({
      tokenId,
      signal: controller.signal,
      // If the grain forwards this token on to another grain call, the
      // grain-factory dispatch hook (`recordCancellationTarget`) calls this
      // so a later `cancelRemoteToken(tokenId)` reaching THIS activation
      // cascades on to that further target too.
      onDispatchToTarget: (target) => ext.recordForwardTarget(tokenId, target),
    });
    if (store !== undefined) {
      (store.tokenSignals ??= []).push(controller.signal);
    }
    return token;
  }

  /**
   * Route a call targeting an extension interface to its bound instance,
   * auto-installing from `extensionFactories` if not yet bound (see
   * `setExtensionFactories`). Throws `GrainExtensionNotInstalledException`
   * when neither a bound instance nor a factory exists.
   *
   * Runs through the same incoming call-filter pipeline as an ordinary
   * grain-method call (Orleans parity: `GrainCallFilter_GrainExtension`) —
   * additive: with no silo-wide filters and no grain-level filter, this stays
   * a direct invoke of the extension method, same as before filters wrapped
   * extension calls at all.
   */
  private async invokeExtension(req: InvocationRequest, interfaceName: string): Promise<unknown> {
    let ext = this.extensions.get(req.interfaceId);
    if (ext === undefined) {
      const factory = this.extensionFactories?.get(req.interfaceId);
      if (factory !== undefined) {
        ext = factory(this);
        this.extensions.set(req.interfaceId, ext);
      }
    }
    if (ext === undefined) {
      throw new GrainExtensionNotInstalledException(
        `extension ${interfaceName} is not installed on ${this.id.toString()}`,
      );
    }
    const fn = (ext as Record<string, unknown>)[req.method];
    if (typeof fn !== "function") {
      throw new GrainCallError(`extension ${interfaceName} has no method ${req.method}`);
    }
    const method = fn as (...args: unknown[]) => unknown;
    const own = grainIncomingFilter(this.instance);
    const filters =
      own === undefined ? this.incomingCallFilters : [...this.incomingCallFilters, own];
    if (filters.length === 0) {
      return await method.apply(ext, req.args);
    }
    const context: IncomingGrainCallContext = {
      target: this.id,
      source: req.sender,
      interfaceId: req.interfaceId,
      interfaceName,
      methodName: req.method,
      args: [...req.args],
      result: undefined,
      headers: requestContextStore() ?? {},
      // Orleans parity: `IGrainCallContext.Grain` is always the grain instance
      // (`GrainMethodInvoker.Grain => grainContext.GrainInstance`), never the
      // extension component itself, even when the call targets an extension
      // interface — so the grain's own `INCOMING_CALL_FILTER` (looked up via
      // `this.instance` above, not the extension) applies here too.
      grain: this.instance,
      invoke: () => Promise.resolve(),
    };
    return await runCallFilters(filters, context, () =>
      Promise.resolve(method.apply(ext, context.args)),
    );
  }

  /** Route a `TransactionResource` system call to the named transactional state. */
  private async invokeTransactionResource(req: InvocationRequest): Promise<unknown> {
    const [stateName, ...rest] = req.args as [string, ...unknown[]];
    const field = getTransactionalFields(this.instance).find((f) => f.stateName === stateName);
    // Unguarded: an always-interleave protocol call may land mid-`@readOnly` turn.
    const resource =
      field === undefined
        ? undefined
        : (unguardedTransactionalState(
            (this.instance as unknown as Record<string, unknown>)[field.fieldName],
          ) as TransactionParticipant | undefined);
    if (resource === undefined) {
      throw new GrainCallError(
        `grain ${this.id.toString()} has no transactional state "${stateName}"`,
      );
    }
    const fn = (resource as unknown as Record<string, unknown>)[req.method] as
      | ((...a: unknown[]) => unknown)
      | undefined;
    if (typeof fn !== "function") {
      throw new GrainCallError(`transaction resource has no method ${req.method}`);
    }
    return await fn.apply(resource, rest);
  }
}
