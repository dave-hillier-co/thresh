import type { TimeProvider, TimerHandle } from "@thresh/core/time-provider";

/**
 * Runs a unit of asynchronous work one pass at a time, again whenever it has
 * been notified since the current pass began (Orleans `BatchWorker`).
 * `notify(at)` asks for a pass no earlier than `at` on the injected clock; the
 * earliest such request wins. A disposed worker runs nothing further.
 */
export class BatchWorker {
  private running = false;
  private again = false;
  private timer: TimerHandle | undefined;
  private timerDue = Number.POSITIVE_INFINITY;
  private disposed = false;

  constructor(
    private readonly work: () => Promise<void>,
    private readonly time: TimeProvider,
  ) {}

  notify(at?: number): void {
    if (this.disposed) return;
    if (at === undefined || at <= this.time.now()) {
      this.again = true;
      if (!this.running) void this.run();
      return;
    }
    if (this.timer !== undefined && this.timerDue <= at) return;
    this.clearTimer();
    this.timerDue = at;
    this.timer = this.time.setTimer(
      () => {
        this.timer = undefined;
        this.timerDue = Number.POSITIVE_INFINITY;
        this.notify();
      },
      Math.max(0, at - this.time.now()),
    );
  }

  dispose(): void {
    this.disposed = true;
    this.clearTimer();
  }

  private clearTimer(): void {
    if (this.timer === undefined) return;
    this.time.clearTimer(this.timer);
    this.timer = undefined;
    this.timerDue = Number.POSITIVE_INFINITY;
  }

  private async run(): Promise<void> {
    this.running = true;
    try {
      // Never run inside the notifier's own call stack.
      await Promise.resolve();
      while (this.again && !this.disposed) {
        this.again = false;
        try {
          await this.work();
        } catch {
          // The work handles its own failures; a stray one must not stop the worker.
        }
      }
    } finally {
      this.running = false;
    }
  }
}
