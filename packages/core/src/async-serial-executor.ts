// Ported from dotnet/orleans src/Orleans.Core/Async/AsyncSerialExecutor.cs @ v10.1.0 (MIT).

/**
 * Runs queued async callbacks strictly one at a time, FIFO: each callback is
 * awaited to completion before the next one starts. Useful inside reentrant
 * grain code to make a set of async operations execute non-reentrantly.
 *
 * `addNext` returns a promise for that specific callback's outcome — it
 * resolves or rejects with whatever the callback resolved or threw. A
 * rejecting callback does not stop the queue: subsequent callbacks still run.
 */
export class AsyncSerialExecutor<TResult = void> {
  private readonly queue: Array<{
    readonly func: () => Promise<TResult>;
    readonly resolve: (value: TResult) => void;
    readonly reject: (error: unknown) => void;
  }> = [];
  private draining = false;

  /**
   * Submits the next function for execution. It runs after every previously
   * submitted function has finished, without interleaving. Returns a promise
   * that settles when this function's own execution settles.
   */
  addNext(func: () => Promise<TResult>): Promise<TResult> {
    return new Promise<TResult>((resolve, reject) => {
      this.queue.push({ func, resolve, reject });
      this.drain();
    });
  }

  private drain(): void {
    if (this.draining) return;
    this.draining = true;
    void this.run();
  }

  private async run(): Promise<void> {
    for (;;) {
      const next = this.queue.shift();
      if (next === undefined) break;
      try {
        next.resolve(await next.func());
      } catch (error) {
        next.reject(error);
      }
    }
    this.draining = false;
  }
}
