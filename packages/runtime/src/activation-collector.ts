import { type Logger, defaultLogger } from "@thresh/core/logger";
import type { Catalog } from "@thresh/runtime/catalog";
import type { TimeProvider, TimerHandle } from "@thresh/runtime/time-provider";

/** Periodically sweeps the catalog, deactivating idle (stale) activations. */
export class ActivationCollector {
  private handle: TimerHandle | undefined;
  private stopped = false;
  /** Set while a sweep is in flight; a tick that finds it set starts nothing. */
  private sweeping = false;
  /** A tick landed mid-sweep: run one more sweep once the current one ends. */
  private tickMissed = false;
  private readonly logger: Logger;

  constructor(
    private readonly catalog: Catalog,
    private readonly time: TimeProvider,
    private readonly intervalMs: number,
    logger: Logger = defaultLogger,
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
      // Reschedule synchronously so the period does not drift by the sweep's
      // duration.
      this.scheduleNext();
      this.sweep();
    }, this.intervalMs);
  }

  /**
   * Sweeps never overlap. Orleans' collection loop awaits each sweep before
   * it waits on its `PeriodicTimer` again (`RunActivationCollectionLoop`), and
   * a `PeriodicTimer` coalesces the ticks that passed meanwhile into one: so
   * a tick that lands mid-sweep starts nothing, and at most one further sweep
   * runs once the current one has finished.
   */
  private sweep(): void {
    if (this.sweeping) {
      this.tickMissed = true;
      return;
    }
    this.sweeping = true;
    this.tickMissed = false;
    // `collectIdle` contains per-activation failures (the deactivation
    // pipeline logs a failing step and carries on), so this should never
    // reject — but nothing else awaits it, so a rejection that slipped
    // through would otherwise become an unhandled rejection repeated every
    // sweep. Belt-and-suspenders: catch and log.
    this.catalog
      .collectIdle()
      .catch((error: unknown) => {
        this.logger.warn("idle-collection sweep failed", { error });
      })
      .finally(() => {
        this.sweeping = false;
        if (this.tickMissed && !this.stopped) this.sweep();
      });
  }
}
