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
  readonly stage: number;
  observer: LifecycleObserver | undefined;
}

/** Staged pub/sub lifecycle subject. Single use — does not support restart. */
export class LifecycleSubject implements LifecycleObservable {
  private readonly subscribers: OrderedObserver[] = [];
  private highStage: number | undefined;

  subscribe(_observerName: string, stage: number, observer: LifecycleObserver): Disposable {
    if (this.highStage !== undefined) {
      throw new Error("Lifecycle has already been started.");
    }
    const orderedObserver: OrderedObserver = { stage, observer };
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
      const results = await Promise.allSettled(
        observersAtStage.map((o) => o.observer!.onStart()),
      );
      const failure = results.find(
        (r): r is PromiseRejectedResult => r.status === "rejected",
      );
      if (failure) {
        throw failure.reason;
      }
    }
  }

  async onStop(): Promise<void> {
    if (this.highStage === undefined) return;
    const stages = descendingStages(this.subscribers).filter((s) => s <= this.highStage!);
    for (const stage of stages) {
      this.highStage = stage;
      const observersAtStage = this.subscribers.filter(
        (o) => o.stage === stage && o.observer !== undefined,
      );
      await Promise.allSettled(observersAtStage.map((o) => o.observer!.onStop()));
    }
  }
}

function ascendingStages(subscribers: readonly OrderedObserver[]): number[] {
  return [...new Set(subscribers.map((o) => o.stage))].sort((a, b) => a - b);
}

function descendingStages(subscribers: readonly OrderedObserver[]): number[] {
  return [...new Set(subscribers.map((o) => o.stage))].sort((a, b) => b - a);
}
