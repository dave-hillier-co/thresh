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
}

/** A participant in a staged lifecycle. */
export interface LifecycleObserver {
  onStart(ct?: AbortSignal): Promise<void>;
  onStop(ct?: AbortSignal): Promise<void>;
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
  private stopped = false;

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

  async onStart(): Promise<void> {
    if (this.highStage !== undefined) {
      throw new Error("Lifecycle has already been started.");
    }
    const stages = ascendingStages(this.subscribers);
    for (const stage of stages) {
      this.highStage = stage;
      const observersAtStage = this.subscribers.filter(
        (o) => o.stage === stage && o.observer !== undefined,
      );
      const failures = await this.runStage(observersAtStage, "start");
      if (failures.length > 0) throw failures[0];
    }
  }

  async onStop(): Promise<void> {
    if (this.highStage === undefined || this.stopped) return;
    this.stopped = true;
    const stages = descendingStages(this.subscribers).filter((s) => s <= this.highStage!);
    for (const stage of stages) {
      this.highStage = stage;
      const observersAtStage = this.subscribers.filter(
        (o) => o.stage === stage && o.observer !== undefined,
      );
      await this.runStage(observersAtStage, "stop");
    }
  }

  /** Run every observer at one stage concurrently, returning the errors in subscription order. */
  private async runStage(
    observers: OrderedObserver[],
    phase: "start" | "stop",
  ): Promise<unknown[]> {
    const results = await Promise.allSettled(
      observers.map(async (o) =>
        phase === "start" ? o.observer!.onStart() : o.observer!.onStop(),
      ),
    );
    const errors: unknown[] = [];
    results.forEach((result, index) => {
      if (result.status !== "rejected") return;
      errors.push(result.reason);
      const o = observers[index]!;
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
