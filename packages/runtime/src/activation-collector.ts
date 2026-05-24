import type { Catalog } from "@tsva/runtime/catalog";
import type { TimeProvider, TimerHandle } from "@tsva/runtime/time-provider";

/** Periodically sweeps the catalog, deactivating idle (stale) activations. */
export class ActivationCollector {
  private handle: TimerHandle | undefined;
  private stopped = false;

  constructor(
    private readonly catalog: Catalog,
    private readonly time: TimeProvider,
    private readonly intervalMs: number,
  ) {}

  start(): void {
    this.stopped = false;
    this.scheduleNext();
  }

  stop(): void {
    this.stopped = true;
    if (this.handle !== undefined) this.time.clearTimer(this.handle);
  }

  private scheduleNext(): void {
    this.handle = this.time.setTimer(() => {
      if (this.stopped) return;
      // Reschedule synchronously so periodic sweeps don't depend on the async
      // collection finishing first.
      this.scheduleNext();
      void this.catalog.collectIdle();
    }, this.intervalMs);
  }
}
