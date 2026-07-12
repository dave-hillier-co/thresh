import type { InvokeMethodOptions } from "@tsva/core/invoke-options";
import type { MayInterleavePredicate } from "@tsva/core/grain-metadata";

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

  constructor(options: TurnSchedulerOptions = {}) {
    this.reentrant = options.reentrant ?? false;
    this.mayInterleavePredicate = options.mayInterleave;
    this.barrierFirstTurn = options.barrierFirstTurn ?? false;
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
    const running: RunningTurn = {
      options: item.turn.options,
      reentrancyId: item.turn.reentrancyId,
    };
    const isFirstTurn = !this.firstTurnSeen;
    this.firstTurnSeen = true;
    this.running.add(running);
    if (running.reentrancyId !== undefined) this.enterSection(running.reentrancyId);

    Promise.resolve()
      .then(() => item.turn.run())
      .then(item.resolve, item.reject)
      .finally(() => {
        this.running.delete(running);
        if (running.reentrancyId !== undefined) this.leaveSection(running.reentrancyId);
        if (isFirstTurn) this.firstTurnSettled = true;
        this.pump();
      });
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
