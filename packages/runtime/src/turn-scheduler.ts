import { GrainCallAbortedError } from "@tsva/core/errors";
import type { InvokeMethodOptions } from "@tsva/core/invoke-options";
import type { MayInterleavePredicate } from "@tsva/core/grain-metadata";
import { LimitExceededException } from "@tsva/core/errors";
import { type Logger, noopLogger } from "@tsva/core/logger";
import { systemTimeProvider, type TimeProvider, type TimerHandle } from "@tsva/core/time-provider";

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
   * Log a warning if a running turn is still in flight past this many ms
   * (Orleans `ActivationData`'s `MaxRequestProcessingTime` stuck-turn
   * detection). Advisory only, like upstream: JS has no thread-interruption
   * primitive, so this cannot abort the turn — it only flags it so an
   * operator (or a future cancellation-aware caller) notices. `undefined`
   * (the default) disables the watchdog.
   */
  maxRequestProcessingTimeMs?: number;
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
}

/**
 * Per-activation scheduler enforcing single-threaded turns with Orleans-style
 * reentrancy admission (mirrors `ActivationData.MayInvokeRequest`):
 *
 * - nothing running          -> admit
 * - fully reentrant grain     -> admit
 * - `alwaysInterleave`        -> admit
 * - `readOnly` and all running read-only -> admit
 * - reentrancy id is an active call-chain section -> admit
 * - a configured `mayInterleave` predicate matches -> admit
 * - otherwise                 -> queue (FIFO for exclusive turns)
 */
export class TurnScheduler {
  private readonly reentrant: boolean;
  private mayInterleavePredicate: MayInterleavePredicate | undefined;
  private readonly queue: QueuedTurn[] = [];
  private readonly running = new Set<RunningTurn>();
  private readonly reentrantSections = new Map<string, number>();
  private readonly barrierFirstTurn: boolean;
  private firstTurnSeen = false;
  private firstTurnSettled = false;
  private readonly softLimit: number | undefined;
  private readonly hardLimit: number | undefined;
  private readonly maxRequestProcessingTimeMs: number | undefined;
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
      this.queue.push({
        turn: turn as Turn<unknown>,
        resolve: resolve as (value: unknown) => void,
        reject,
      });
      this.pump();
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
    if (turn.options.readOnly && this.allRunningReadOnly()) return true;
    if (turn.reentrancyId !== undefined && this.reentrantSections.has(turn.reentrancyId)) {
      return true;
    }
    if (this.mayInterleavePredicate !== undefined && this.matchesMayInterleave(turn)) {
      return true;
    }
    return false;
  }

  private allRunningReadOnly(): boolean {
    for (const r of this.running) {
      if (!r.options.readOnly) return false;
    }
    return true;
  }

  /**
   * True when the grain's `mayInterleave` predicate admits the *incoming* turn
   * (Orleans evaluates `[MayInterleave]` on the arriving request only — the
   * request itself declares whether it is safe to interleave with whatever is
   * running, not the other way around).
   */
  private matchesMayInterleave(turn: Turn<unknown>): boolean {
    return turn.method !== undefined && this.mayInterleavePredicate!(turn.method, turn.args ?? []);
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
    };
    const isFirstTurn = !this.firstTurnSeen;
    this.firstTurnSeen = true;
    this.running.add(running);
    if (running.reentrancyId !== undefined) this.enterSection(running.reentrancyId);
    const watchdog = this.armStuckTurnWatchdog(item.turn);

    Promise.resolve()
      .then(() => item.turn.run())
      .then(item.resolve, item.reject)
      .finally(() => {
        if (watchdog !== undefined) this.time.clearTimer(watchdog);
        this.running.delete(running);
        if (running.reentrancyId !== undefined) this.leaveSection(running.reentrancyId);
        if (isFirstTurn) this.firstTurnSettled = true;
        this.pump();
      });
  }

  /**
   * Schedule a one-shot warning if `turn` is still running once
   * `maxRequestProcessingTimeMs` elapses (Orleans' `MaxRequestProcessingTime`
   * stuck-turn detection). There is no way to actually abort a running
   * `async` function from outside it in JS — Orleans itself can't
   * force-kill a thread either — so this only ever logs; the caller is
   * responsible for interrupting long-running work cooperatively (e.g. via a
   * cancellation token).
   */
  private armStuckTurnWatchdog(turn: Turn<unknown>): TimerHandle | undefined {
    if (this.maxRequestProcessingTimeMs === undefined) return undefined;
    const startedAtMs = this.time.now();
    return this.time.setTimer(() => {
      this.logger.warn("activation turn exceeded MaxRequestProcessingTime", {
        ...(this.grainId !== undefined ? { grainId: this.grainId } : {}),
        ...(turn.method !== undefined ? { method: turn.method } : {}),
        elapsedMs: this.time.now() - startedAtMs,
        maxRequestProcessingTimeMs: this.maxRequestProcessingTimeMs,
      });
    }, this.maxRequestProcessingTimeMs);
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
