import { GrainCallAbortedError } from "@thresh/core/errors";
import type { InvokeMethodOptions } from "@thresh/core/invoke-options";
import type { MayInterleavePredicate } from "@thresh/core/grain-metadata";
import { LimitExceededException } from "@thresh/core/errors";
import { type Logger, noopLogger } from "@thresh/core/logger";
import {
  systemTimeProvider,
  type TimeProvider,
  type TimerHandle,
} from "@thresh/core/time-provider";

/**
 * One unit of work admitted to a grain's activation. A turn is the whole
 * `async` `run()` including its continuations: no new exclusive turn starts
 * until a running one's promise settles.
 */
export interface Turn<R> {
  readonly options: InvokeMethodOptions;
  /** Call-chain reentrancy id, propagated along a chain of grain calls. */
  readonly reentrancyId?: string;
  /** The grain method this turn dispatches, if any (system turns have none). */
  readonly method?: string;
  readonly args?: readonly unknown[];
  /**
   * Ambient cancellation for this turn (Orleans has no analogue — JS-only
   * cooperative cancellation, see `docs/deviations.md`). Checked only at
   * admission: a turn whose signal has fired before it would start is
   * rejected with `GrainCallAbortedError` instead of running at all. Once a
   * turn HAS started it always runs to completion — there is no thread to
   * preempt it with — so firing `signal` after that point has no effect here
   * (the grain method body may still observe it cooperatively via
   * `GrainRuntime.getCancellationSignal()`).
   */
  readonly signal?: AbortSignal;
  run(): Promise<R>;
}

export interface TurnSchedulerOptions {
  /** A fully reentrant grain: every turn may interleave. */
  reentrant?: boolean;
  /**
   * The grain's `[MayInterleave]`-equivalent admission predicate, if it
   * declared one via `@mayInterleave()`. `undefined` (the default) means the
   * grain declared no predicate — admission ignores it entirely.
   */
  mayInterleave?: MayInterleavePredicate;
  /**
   * Nothing — not even a reentrant/alwaysInterleave/matching-reentrancy-id
   * turn — may run concurrently with the very first turn scheduled, until it
   * settles. Orleans guarantees `OnActivateAsync` (here: the activation turn,
   * which also runs state binding) fully completes before any request is
   * dispatched, even for a fully reentrant grain; the generic admission rules
   * below are otherwise unaware that a "first turn" is special, so the
   * activation layer opts into this explicitly.
   */
  barrierFirstTurn?: boolean;
  /**
   * Warn (via `logger`) once the queue reaches this many WAITING turns
   * (running turns don't count) — Orleans `WorkItemGroup`
   * (`SchedulingOptions.MaxEnqueuedRequestsSoftLimit`). Advisory only: the
   * turn is still admitted. `undefined` (the default) disables the check.
   */
  maxEnqueuedRequestsSoftLimit?: number;
  /**
   * Reject a newly scheduled turn with `LimitExceededException` once the
   * queue already holds this many WAITING turns (Orleans `WorkItemGroup`
   * `SchedulingOptions.MaxEnqueuedRequestsHardLimit`) — a bounded
   * per-activation mailbox so one stuck grain can't grow memory without
   * limit. `undefined` (the default) disables the check: the queue stays
   * unbounded.
   */
  maxEnqueuedRequestsHardLimit?: number;
  /**
   * Warn once the running BLOCKING turn (see the class doc) is still in
   * flight past this many ms (Orleans `ActivationData`'s
   * `MaxRequestProcessingTime` stuck-turn detection). JS has no
   * thread-interruption primitive, so this never aborts the wedged turn
   * itself — Orleans can't force-kill a thread either, and leaves it
   * "dangling, stuck processing the current request until it eventually
   * completes" (`DeactivateStuckActivation`'s comment). When `onStuck` is
   * also given, the scheduler additionally acts on it (see `onStuck`).
   * `undefined` (the default) disables the watchdog entirely.
   */
  maxRequestProcessingTimeMs?: number;
  /**
   * Called once, synchronously, the moment the activation is judged stuck —
   * Orleans `ActivationData.DeactivateStuckActivation`, which upstream reaches
   * only from `ProcessPendingRequests`: a waiting request cannot be admitted
   * AND the blocking turn has run past `maxRequestProcessingTimeMs`. So an
   * overdue turn with nothing waiting behind it (or only requests that may
   * interleave with it, e.g. on a fully reentrant grain) never triggers it;
   * the first blocked arrival after the limit (or a request already waiting
   * when the limit passes) does. Its return value is used to
   * reject every turn currently QUEUED behind the wedged one (Orleans
   * `RerouteAllQueuedMessages`) and every turn `schedule()`d from this point
   * on (this activation cannot recover, so nothing should ever queue behind
   * it again) — only the already-running blocking turn is left alone, since
   * it cannot be preempted. Never called for a long-running turn that is NOT
   * the blocking one (an interleaved turn running long is merely diagnostic
   * upstream too — `AnalyzeWorkload` — not grounds for deactivation).
   * Ignored when `maxRequestProcessingTimeMs` is unset.
   */
  onStuck?: (turn: Turn<unknown>) => unknown;
  /** Clock the stuck-turn watchdog schedules against; defaults to the system clock. */
  time?: TimeProvider;
  /** Sink for soft-limit/stuck-turn warnings; defaults to discarding them. */
  logger?: Logger;
  /** Included on every warning log line to identify the owning activation. */
  grainId?: string;
}

interface QueuedTurn {
  turn: Turn<unknown>;
  resolve: (value: unknown) => void;
  reject: (err: unknown) => void;
}

interface RunningTurn {
  options: InvokeMethodOptions;
  reentrancyId: string | undefined;
  method: string | undefined;
  args: readonly unknown[] | undefined;
  turn: Turn<unknown>;
  /** Set once this turn, as the blocking turn, has run past `maxRequestProcessingTimeMs`. */
  overdue: boolean;
}

/**
 * Per-activation scheduler enforcing single-threaded turns with Orleans-style
 * reentrancy admission (mirrors `ActivationData.MayInvokeRequest`):
 *
 * - nothing running          -> admit
 * - fully reentrant grain     -> admit
 * - `alwaysInterleave`        -> admit
 * - no blocking turn running (only `alwaysInterleave` turns are running) -> admit
 * - `readOnly` and the blocking turn is also `readOnly` -> admit
 * - reentrancy id is an active call-chain section -> admit
 * - a configured `mayInterleave` predicate matches the incoming turn OR the
 *   blocking turn -> admit
 * - otherwise                 -> queue (FIFO for exclusive turns)
 *
 * "The blocking turn" is the one running turn (there is at most one, absent a
 * matching `mayInterleave`/reentrancy admission) that is not `alwaysInterleave`
 * — Orleans' `_blockingRequest` (`ActivationData.RecordRunning`/`MayInvokeRequest`).
 * A running `alwaysInterleave` turn never becomes the blocking turn, so it
 * never blocks anything else from being admitted.
 */
export class TurnScheduler {
  private readonly reentrant: boolean;
  private mayInterleavePredicate: MayInterleavePredicate | undefined;
  private readonly queue: QueuedTurn[] = [];
  private readonly running = new Set<RunningTurn>();
  /** Orleans' `_blockingRequest`: the one running turn that isn't `alwaysInterleave`. */
  private blockingTurn: RunningTurn | undefined;
  private readonly reentrantSections = new Map<string, number>();
  private readonly barrierFirstTurn: boolean;
  private firstTurnSeen = false;
  private firstTurnSettled = false;
  private readonly softLimit: number | undefined;
  private readonly hardLimit: number | undefined;
  private readonly maxRequestProcessingTimeMs: number | undefined;
  private readonly onStuck: ((turn: Turn<unknown>) => unknown) | undefined;
  /**
   * Set once `onStuck` has fired (Orleans' activation going `Invalid`): every
   * turn still queued was rejected with this at that moment, and every turn
   * `schedule()`d afterward is rejected with it immediately instead of
   * queuing behind a blocking turn that will never finish being waited on.
   */
  private stuck: { readonly rejection: unknown } | undefined;
  private readonly time: TimeProvider;
  private readonly logger: Logger;
  private readonly grainId: string | undefined;

  constructor(options: TurnSchedulerOptions = {}) {
    this.reentrant = options.reentrant ?? false;
    this.mayInterleavePredicate = options.mayInterleave;
    this.barrierFirstTurn = options.barrierFirstTurn ?? false;
    this.softLimit = options.maxEnqueuedRequestsSoftLimit;
    this.hardLimit = options.maxEnqueuedRequestsHardLimit;
    this.maxRequestProcessingTimeMs = options.maxRequestProcessingTimeMs;
    this.onStuck = options.onStuck;
    this.time = options.time ?? systemTimeProvider;
    this.logger = options.logger ?? noopLogger;
    this.grainId = options.grainId;
  }

  /**
   * Late-bind (or clear) the grain's `mayInterleave` predicate. Exists because
   * the predicate is resolved from the grain's own metadata, which is only
   * known once its instance is constructed — after the scheduler itself.
   */
  setMayInterleave(predicate: MayInterleavePredicate | undefined): void {
    this.mayInterleavePredicate = predicate;
  }

  /** True while any turn is running or queued. */
  get busy(): boolean {
    return this.running.size > 0 || this.queue.length > 0;
  }

  /**
   * Current concurrency load (running + queued turns) — used by the catalog
   * to pick the least-loaded stateless-worker activation to queue an
   * over-capacity call onto, once `maxLocalWorkers` local activations already
   * exist and all are busy.
   */
  get load(): number {
    return this.running.size + this.queue.length;
  }

  schedule<R>(turn: Turn<R>): Promise<R> {
    if (this.stuck !== undefined) {
      // This activation has already been judged stuck and deactivated
      // (Orleans: `ProcessRequestsToInvalidActivation` treats every message,
      // waiting or newly arriving, the same way once invalid) — never queue
      // another turn behind a blocking one that will never finish.
      return Promise.reject(this.stuck.rejection);
    }
    if (this.hardLimit !== undefined && this.queue.length >= this.hardLimit) {
      return Promise.reject(
        new LimitExceededException(
          "MaxEnqueuedRequestsHardLimit",
          this.queue.length,
          this.hardLimit,
        ),
      );
    }
    if (this.softLimit !== undefined && this.queue.length >= this.softLimit) {
      this.logger.warn("activation turn queue exceeded MaxEnqueuedRequestsSoftLimit", {
        ...(this.grainId !== undefined ? { grainId: this.grainId } : {}),
        queueLength: this.queue.length,
        softLimit: this.softLimit,
      });
    }
    return new Promise<R>((resolve, reject) => {
      const item: QueuedTurn = {
        turn: turn as Turn<unknown>,
        resolve: resolve as (value: unknown) => void,
        reject,
      };
      this.queue.push(item);
      this.pump();
      // Orleans runs its MaxRequestProcessingTime check from
      // `ProcessPendingRequests`, for a waiting request `MayInvokeRequest`
      // refuses: a request that has to wait behind an already-overdue
      // blocking turn is what declares the activation stuck.
      if (this.queue.includes(item)) this.deactivateIfStuck();
    });
  }

  private pump(): void {
    // Scan from the head admitting every admissible turn. Exclusive turns stay
    // FIFO because they are only admissible when nothing is running; interleavers
    // (read-only / alwaysInterleave / active reentrancy id) may jump ahead.
    for (let i = 0; i < this.queue.length; ) {
      const item = this.queue[i]!;
      if (this.mayAdmit(item.turn)) {
        this.queue.splice(i, 1);
        this.start(item);
      } else {
        i++;
      }
    }
  }

  private mayAdmit(turn: Turn<unknown>): boolean {
    if (this.running.size === 0) return true;
    if (this.barrierFirstTurn && !this.firstTurnSettled) return false;
    if (this.reentrant) return true;
    if (turn.options.alwaysInterleave) return true;
    // No blocking turn running (only `alwaysInterleave` turns, if any) -> admit.
    if (this.blockingTurn === undefined) return true;
    if (turn.options.readOnly && this.blockingTurn.options.readOnly) return true;
    if (turn.reentrancyId !== undefined && this.reentrantSections.has(turn.reentrancyId)) {
      return true;
    }
    if (
      this.mayInterleavePredicate !== undefined &&
      (this.matchesMayInterleave(turn.method, turn.args) ||
        this.matchesMayInterleave(this.blockingTurn.method, this.blockingTurn.args))
    ) {
      return true;
    }
    return false;
  }

  /**
   * True when the grain's `mayInterleave` predicate admits `method`/`args`.
   * Orleans evaluates `[MayInterleave]` on the incoming request OR the
   * currently-blocking request (`canInterleave.MayInterleave(incoming) ||
   * canInterleave.MayInterleave(_blockingRequest)`) — either side declaring
   * itself safe to interleave is enough.
   */
  private matchesMayInterleave(
    method: string | undefined,
    args: readonly unknown[] | undefined,
  ): boolean {
    return method !== undefined && this.mayInterleavePredicate!(method, args ?? []);
  }

  private start(item: QueuedTurn): void {
    // Admission-time-only cancellation check (see `Turn.signal`'s doc): a
    // turn that never got to run is simply rejected here, never added to
    // `running` and never counted as the "first turn" for `barrierFirstTurn`.
    if (item.turn.signal?.aborted === true) {
      item.reject(new GrainCallAbortedError());
      return;
    }
    const running: RunningTurn = {
      options: item.turn.options,
      reentrancyId: item.turn.reentrancyId,
      method: item.turn.method,
      args: item.turn.args,
      turn: item.turn,
      overdue: false,
    };
    const isFirstTurn = !this.firstTurnSeen;
    this.firstTurnSeen = true;
    this.running.add(running);
    if (running.reentrancyId !== undefined) this.enterSection(running.reentrancyId);
    // Orleans' RecordRunning: the first non-`alwaysInterleave` turn to start
    // while none is blocking becomes `_blockingRequest`, and stays so until it
    // (specifically) completes — even if other turns interleave after it.
    if (this.blockingTurn === undefined && !running.options.alwaysInterleave) {
      this.blockingTurn = running;
    }
    const watchdog = this.armStuckTurnWatchdog(item.turn, running);

    Promise.resolve()
      .then(() => item.turn.run())
      .then(item.resolve, item.reject)
      .finally(() => {
        if (watchdog !== undefined) this.time.clearTimer(watchdog);
        this.running.delete(running);
        if (running.reentrancyId !== undefined) this.leaveSection(running.reentrancyId);
        if (this.blockingTurn === running) this.blockingTurn = undefined;
        if (isFirstTurn) this.firstTurnSettled = true;
        this.pump();
      });
  }

  /**
   * Schedule a one-shot warning if `turn` is still running once
   * `maxRequestProcessingTimeMs` elapses (Orleans' `MaxRequestProcessingTime`
   * stuck-turn detection), and — only when `turn` is the BLOCKING turn (see
   * the class doc) — mark it overdue, so that a request waiting behind it now
   * or arriving later deactivates the activation (`deactivateIfStuck`). There is no way to actually abort a running
   * `async` function from outside it in JS — Orleans itself can't force-kill
   * a thread either, hence upstream leaving the blocking request "dangling"
   * — so the wedged turn itself keeps running to completion regardless.
   */
  private armStuckTurnWatchdog(turn: Turn<unknown>, running: RunningTurn): TimerHandle | undefined {
    if (this.maxRequestProcessingTimeMs === undefined) return undefined;
    const startedAtMs = this.time.now();
    return this.time.setTimer(() => {
      this.logger.warn("activation turn exceeded MaxRequestProcessingTime", {
        ...(this.grainId !== undefined ? { grainId: this.grainId } : {}),
        ...(turn.method !== undefined ? { method: turn.method } : {}),
        elapsedMs: this.time.now() - startedAtMs,
        maxRequestProcessingTimeMs: this.maxRequestProcessingTimeMs,
      });
      if (this.blockingTurn !== running) return; // only the blocking turn triggers deactivation
      running.overdue = true;
      // Anything still queued at this point is waiting on the blocking turn
      // (`pump` admits every admissible turn), so it is already stuck.
      if (this.queue.length > 0) this.deactivateIfStuck();
    }, this.maxRequestProcessingTimeMs);
  }

  /**
   * Orleans `DeactivateStuckActivation`, reached (as upstream) only when a
   * waiting request cannot be admitted and the blocking turn has run past
   * `maxRequestProcessingTimeMs`: evict and reject every queued turn with
   * `onStuck`'s return value, and reject every later `schedule()` the same way.
   * A long turn nobody is waiting on, or one every arrival can interleave
   * with (a fully reentrant grain, read-only alongside read-only), is left alone.
   */
  private deactivateIfStuck(): void {
    if (this.onStuck === undefined || this.stuck !== undefined) return;
    const blocking = this.blockingTurn;
    if (blocking === undefined || !blocking.overdue || this.queue.length === 0) return;
    const rejection = this.onStuck(blocking.turn);
    this.stuck = { rejection };
    const queued = this.queue.splice(0, this.queue.length);
    for (const item of queued) item.reject(rejection);
  }

  private enterSection(id: string): void {
    this.reentrantSections.set(id, (this.reentrantSections.get(id) ?? 0) + 1);
  }

  private leaveSection(id: string): void {
    const next = (this.reentrantSections.get(id) ?? 0) - 1;
    if (next <= 0) this.reentrantSections.delete(id);
    else this.reentrantSections.set(id, next);
  }
}
