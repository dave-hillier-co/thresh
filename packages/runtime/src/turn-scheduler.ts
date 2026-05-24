import type { InvokeMethodOptions } from "@tsva/core/invoke-options";

/**
 * One unit of work admitted to a grain's activation. A turn is the whole
 * `async` `run()` including its continuations: no new exclusive turn starts
 * until a running one's promise settles.
 */
export interface Turn<R> {
  readonly options: InvokeMethodOptions;
  /** Call-chain reentrancy id, propagated along a chain of grain calls. */
  readonly reentrancyId?: string;
  run(): Promise<R>;
}

export interface TurnSchedulerOptions {
  /** A fully reentrant grain: every turn may interleave. */
  reentrant?: boolean;
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
 * - otherwise                 -> queue (FIFO for exclusive turns)
 */
export class TurnScheduler {
  private readonly reentrant: boolean;
  private readonly queue: QueuedTurn[] = [];
  private readonly running = new Set<RunningTurn>();
  private readonly reentrantSections = new Map<string, number>();

  constructor(options: TurnSchedulerOptions = {}) {
    this.reentrant = options.reentrant ?? false;
  }

  /** True while any turn is running or queued. */
  get busy(): boolean {
    return this.running.size > 0 || this.queue.length > 0;
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
    if (this.reentrant) return true;
    if (turn.options.alwaysInterleave) return true;
    if (turn.options.readOnly && this.allRunningReadOnly()) return true;
    if (turn.reentrancyId !== undefined && this.reentrantSections.has(turn.reentrancyId)) {
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

  private start(item: QueuedTurn): void {
    const running: RunningTurn = {
      options: item.turn.options,
      reentrancyId: item.turn.reentrancyId,
    };
    this.running.add(running);
    if (running.reentrancyId !== undefined) this.enterSection(running.reentrancyId);

    Promise.resolve()
      .then(() => item.turn.run())
      .then(item.resolve, item.reject)
      .finally(() => {
        this.running.delete(running);
        if (running.reentrancyId !== undefined) this.leaveSection(running.reentrancyId);
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
