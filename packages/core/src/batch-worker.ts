// Ported from dotnet/orleans src/Orleans.Core/Async/BatchWorker.cs @ v10.1.0 (MIT).
//
// Not ported: the timed `Notify(DateTime)` overload and the cancellation token it alone consumes.

/**
 * The general pattern for an asynchronous worker that performs a work cycle when notified, to
 * service queued work. Each work cycle handles ALL the queued work; a notify that arrives while a
 * cycle runs schedules exactly one more cycle after it, however many notifies arrive. Cycles never
 * overlap.
 *
 * Inside a grain this is the single writer: callers enqueue into a structure the grain owns and
 * call {@link notify}; the cycle drains it. The cycle starts in a later microtask, never inside
 * `notify`, and inherits the notifying turn's async context.
 */
export abstract class BatchWorker {
  /** The current work cycle, or undefined when idle. */
  #currentWorkCycle: Promise<void> | undefined;

  /** Set when more work arrived during the current cycle. */
  #moreWork = false;

  /** Resolves with the next cycle once it starts; defined only while someone waits for it. */
  #nextWorkCycleSignal: ((cycle: Promise<void>) => void) | undefined;
  #nextWorkCycle: Promise<void> | undefined;

  /** One work cycle. */
  protected abstract work(): Promise<void>;

  /** Notifies the worker that there is more work. */
  notify(): void {
    if (this.#currentWorkCycle !== undefined) {
      // Lets the current work cycle know that there is more work.
      this.#moreWork = true;
    } else {
      this.#start();
    }
  }

  #start(): Promise<void> {
    // Queue the cycle rather than run it inside the caller, as the source queues it on the
    // current scheduler.
    const cycle = Promise.resolve().then(() => this.work());
    this.#currentWorkCycle = cycle;

    // Check for more work once the cycle ends, however it ends.
    const check = (): void => this.#checkForMoreWork();
    cycle.then(check, check);
    return cycle;
  }

  #checkForMoreWork(): void {
    if (!this.#moreWork) {
      this.#currentWorkCycle = undefined;
      return;
    }
    this.#moreWork = false;

    // Take any waiter's signal for the next cycle, then start that cycle.
    const signal = this.#nextWorkCycleSignal;
    this.#nextWorkCycleSignal = undefined;
    this.#nextWorkCycle = undefined;

    const next = this.#start();
    signal?.(next);
  }

  /** Whether no work cycle is running. */
  isIdle(): boolean {
    return this.#currentWorkCycle === undefined;
  }

  /**
   * Waits for the current work cycle, and also the next one if there is currently unserviced
   * work. Rejects if the cycle it waits for rejects.
   */
  waitForCurrentWorkToBeServiced(): Promise<void> {
    if (!this.#moreWork) {
      // Just wait for the current work cycle.
      return this.#currentWorkCycle ?? Promise.resolve();
    }
    // The next cycle does not exist yet: wait on a promise that adopts it when it starts.
    return this.#nextWorkCycle ?? this.#createNextWorkCyclePromise();
  }

  #createNextWorkCyclePromise(): Promise<void> {
    this.#nextWorkCycle = new Promise<Promise<void>>((resolve) => {
      this.#nextWorkCycleSignal = resolve;
    }).then((cycle) => cycle);
    return this.#nextWorkCycle;
  }

  /**
   * Notifies the worker that there is more work, and waits for the current work cycle, and also
   * the next one if there is currently unserviced work.
   */
  notifyAndWaitForWorkToBeServiced(): Promise<void> {
    if (this.#currentWorkCycle !== undefined) {
      this.#moreWork = true;
      return this.#nextWorkCycle ?? this.#createNextWorkCyclePromise();
    }
    return this.#start();
  }
}

/** A {@link BatchWorker} whose work cycle is the given function. */
export class BatchWorkerFromDelegate extends BatchWorker {
  readonly #work: () => Promise<void>;

  constructor(work: () => Promise<void>) {
    super();
    this.#work = work;
  }

  protected work(): Promise<void> {
    return this.#work();
  }
}
