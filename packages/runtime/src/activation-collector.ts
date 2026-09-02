import { type Logger, noopLogger } from "@thresh/core/logger";
import type { Catalog } from "@thresh/runtime/catalog";
import type { TimeProvider, TimerHandle } from "@thresh/runtime/time-provider";

/** Periodically sweeps the catalog, deactivating idle (stale) activations. */
export class ActivationCollector {
  private handle: TimerHandle | undefined;
  private stopped = false;
  private readonly logger: Logger;

  constructor(
    private readonly catalog: Catalog,
    private readonly time: TimeProvider,
    private readonly intervalMs: number,
    logger: Logger = noopLogger,
  ) {
    this.logger = logger;
  }

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
      // `collectIdle` itself contains per-activation disposal failures (see
      // `Catalog.disposeCollected`), so this should never reject — but this
      // call is fire-and-forget with nothing else awaiting it, so a rejection
      // that slipped through anyway would otherwise become an unhandled
      // rejection repeated every sweep. Belt-and-suspenders: catch and log.
      this.catalog.collectIdle().catch((error) => {
        this.logger.warn("idle-collection sweep failed", { error });
      });
    }, this.intervalMs);
  }
}
