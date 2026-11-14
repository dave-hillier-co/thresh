// Port of Orleans.Core's LifecycleSubject / ILifecycleObservable / ILifecycleObserver
// (src/Orleans.Core/Lifecycle/LifecycleSubject.cs @ v10.1.0, MIT).
//
// A staged pub/sub lifecycle: observers subscribe at a numbered stage; OnStart
// runs stages in ascending order (all observers at a stage before moving on),
// OnStop runs stages in descending order starting from the highest stage that
// was reached. A failure during OnStart records the failed stage as the
// highest reached and rethrows without advancing further — it does NOT call
// OnStop itself; callers are expected to call OnStop afterwards (as Orleans'
// silo/grain activation lifecycles always do), which then unwinds everything
// started so far, including the failed stage, in reverse order. Failures
// during OnStop are swallowed so stopping continues for the remaining stages
// (matching upstream's "stop always continues" contract).

/**
 * The stage numbers of Orleans' `ServiceLifecycleStage`
 * (src/Orleans.Core/Lifecycle/ServiceLifecycleStage.cs @ v10.1.0): start runs ascending, stop
 * descending.
 */
export const ServiceLifecycleStage = {
  First: -2147483648,
  RuntimeInitialize: 2000,
  RuntimeServices: 4000,
  RuntimeStorageServices: 6000,
  RuntimeGrainServices: 8000,
  AfterRuntimeGrainServices: 8100,
  ApplicationServices: 10000,
  BecomeActive: 19999,
  Active: 20000,
  Last: 2147483647,
} as const;

/** A failure of one observer, reported through {@link LifecycleSubjectOptions.onError}. */
export interface LifecycleFailure {
  readonly phase: "start" | "stop";
  readonly observerName: string;
  readonly stage: number;
  readonly error: unknown;
}

export interface LifecycleSubjectOptions {
  /**
   * Told of every observer that fails, in either phase. A failed stop is otherwise swallowed (the
   * remaining stages must still stop), so this is how it is made visible. A throwing callback is
   * ignored: reporting must never change the lifecycle's course.
   */
  onError?: (failure: LifecycleFailure) => void;
  /**
   * Gives up on an observer whose stop has not settled after this many milliseconds, reports it
   * through `onError` as a failed stop, and goes on to the stages below. Also bounds how long a
   * stop waits for a start still in flight. Unset, a stop waits as long as the observer takes.
   */
  stopTimeoutMs?: number;
}

/** A participant in a staged lifecycle. */
export interface LifecycleObserver {
  onStart(ct?: AbortSignal): Promise<void>;
  onStop(ct?: AbortSignal): Promise<void>;
  /** Overrides {@link LifecycleSubjectOptions.stopTimeoutMs} for this observer's stop. */
  readonly stopTimeoutMs?: number;
}

/** Something observers can subscribe to at a given numbered stage. */
export interface LifecycleObservable {
  subscribe(observerName: string, stage: number, observer: LifecycleObserver): Disposable;
}

interface Disposable {
  dispose(): void;
}

interface OrderedObserver {
  readonly name: string;
  readonly stage: number;
  observer: LifecycleObserver | undefined;
}

/** Staged pub/sub lifecycle subject. Single use — does not support restart. */
export class LifecycleSubject implements LifecycleObservable {
  private readonly subscribers: OrderedObserver[] = [];
  private highStage: number | undefined;
  private stopRequested = false;
  private stopping: Promise<void> | undefined;
  private starting: Promise<void> | undefined;
  /** The stage whose start is running right now, if any. */
  private inFlightStage: number | undefined;
  /** A stop gave up waiting for this stage's start; the start loop stops it when it completes. */
  private deferredStage: number | undefined;

  constructor(private readonly options: LifecycleSubjectOptions = {}) {}

  subscribe(observerName: string, stage: number, observer: LifecycleObserver): Disposable {
    if (this.highStage !== undefined) {
      throw new Error("Lifecycle has already been started.");
    }
    const orderedObserver: OrderedObserver = { name: observerName, stage, observer };
    this.subscribers.push(orderedObserver);
    return {
      dispose: () => {
        orderedObserver.observer = undefined;
      },
    };
  }

  onStart(): Promise<void> {
    if (this.highStage !== undefined || this.starting !== undefined) {
      return Promise.reject(new Error("Lifecycle has already been started."));
    }
    this.starting = this.runStart();
    return this.starting;
  }

  private async runStart(): Promise<void> {
    const stages = ascendingStages(this.subscribers);
    for (const stage of stages) {
      // A stop that arrived between stages halts the start: no later stage starts.
      if (this.stopRequested) throw new Error("Lifecycle was stopped while it was starting.");
      this.highStage = stage;
      this.inFlightStage = stage;
      const failures = await this.runStage(this.observersAt(stage), "start");
      this.inFlightStage = undefined;
      if (this.deferredStage === stage) {
        // The stop that ran meanwhile gave up waiting for this stage; stop it now it has settled.
        this.deferredStage = undefined;
        await this.runStage(this.observersAt(stage), "stop");
      }
      if (failures.length > 0) throw failures[0];
    }
    if (this.stopRequested) throw new Error("Lifecycle was stopped while it was starting.");
  }

  /**
   * Stop, descending from the highest stage reached. A stop issued while a start is in flight
   * halts that start (no later stage begins), waits for the stage in flight to settle (bounded
   * by `stopTimeoutMs`), then stops everything that started. Idempotent: every caller gets the
   * same promise.
   */
  onStop(): Promise<void> {
    if (this.stopping !== undefined) return this.stopping;
    if (this.highStage === undefined && this.starting === undefined) return Promise.resolve();
    this.stopRequested = true;
    this.stopping = this.runStop();
    return this.stopping;
  }

  private async runStop(): Promise<void> {
    if (this.inFlightStage !== undefined && this.starting !== undefined) {
      const inFlight = this.inFlightStage;
      const settled = await this.settleWithin(this.starting.catch(() => undefined));
      if (!settled) this.deferredStage = inFlight;
    }
    if (this.highStage === undefined) return;
    const deferred = this.deferredStage;
    const stages = descendingStages(this.subscribers).filter(
      (s) => s <= this.highStage! && s !== deferred,
    );
    for (const stage of stages) {
      await this.runStage(this.observersAt(stage), "stop");
    }
  }

  private observersAt(stage: number): OrderedObserver[] {
    return this.subscribers.filter((o) => o.stage === stage && o.observer !== undefined);
  }

  /** True if the work settled within `stopTimeoutMs`; with no timeout configured, waits. */
  private async settleWithin(work: Promise<unknown>): Promise<boolean> {
    const timeoutMs = this.options.stopTimeoutMs;
    if (timeoutMs === undefined) {
      await work;
      return true;
    }
    let timer: ReturnType<typeof setTimeout> | undefined;
    const timedOut = new Promise<false>((resolve) => {
      timer = setTimeout(() => resolve(false), timeoutMs);
    });
    try {
      return await Promise.race([work.then(() => true as const), timedOut]);
    } finally {
      clearTimeout(timer);
    }
  }

  /**
   * Run the observers at one stage, returning the errors. A start runs them concurrently, as
   * Orleans does. A stop runs them one at a time in the reverse of subscription order, so within
   * a stage too the stop order is the reverse of the order they were registered.
   */
  private async runStage(
    observers: OrderedObserver[],
    phase: "start" | "stop",
  ): Promise<unknown[]> {
    const runOne = async (o: OrderedObserver): Promise<void> => {
      const timeoutMs =
        phase === "stop" ? (o.observer!.stopTimeoutMs ?? this.options.stopTimeoutMs) : undefined;
      const work = (async () =>
        phase === "start" ? o.observer!.onStart() : o.observer!.onStop())();
      if (timeoutMs === undefined) return work;
      let timer: ReturnType<typeof setTimeout> | undefined;
      const timeout = new Promise<never>((_, reject) => {
        timer = setTimeout(
          () => reject(new Error(`stop timed out after ${timeoutMs}ms`)),
          timeoutMs,
        );
      });
      try {
        return await Promise.race([work, timeout]);
      } finally {
        clearTimeout(timer);
      }
    };
    let results: PromiseSettledResult<void>[];
    let ordered = observers;
    if (phase === "start") {
      results = await Promise.allSettled(observers.map(runOne));
    } else {
      ordered = [...observers].reverse();
      results = [];
      for (const o of ordered) results.push(...(await Promise.allSettled([runOne(o)])));
    }
    const errors: unknown[] = [];
    results.forEach((result, index) => {
      if (result.status !== "rejected") return;
      errors.push(result.reason);
      const o = ordered[index]!;
      try {
        this.options.onError?.({
          phase,
          observerName: o.name,
          stage: o.stage,
          error: result.reason,
        });
      } catch {
        // reporting never changes the lifecycle's course
      }
    });
    return errors;
  }
}

function ascendingStages(subscribers: readonly OrderedObserver[]): number[] {
  return [...new Set(subscribers.map((o) => o.stage))].sort((a, b) => a - b);
}

function descendingStages(subscribers: readonly OrderedObserver[]): number[] {
  return [...new Set(subscribers.map((o) => o.stage))].sort((a, b) => b - a);
}
