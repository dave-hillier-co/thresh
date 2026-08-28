import { InconsistentStateError } from "@thresh/core/errors";
import type { CustomStorageInterface, LogViewAdaptor } from "@thresh/core/journaled-grain";

/** How many CAS attempts one `confirmSubmittedEntries` makes before giving up. */
const DEFAULT_MAX_ATTEMPTS = 5;

export interface CustomStorageLogViewAdaptorOptions {
  /**
   * CAS attempts per confirm before an `InconsistentStateError`. Orleans is *stubborn* here --
   * `CustomStorageAdaptor.WriteAsync` re-reads and retries forever, because its retry runs on a
   * background protocol loop. Thresh awaits `confirmEvents()` inside the grain turn, so an
   * unbounded retry would hang the activation until the stuck-turn watchdog fires. A bounded
   * budget surfaces the conflict to the caller instead, and the events stay pending so a later
   * confirm can retry.
   */
  maxAttempts?: number;
}

/**
 * A `LogViewAdaptor` for grains that own their own log persistence, mirroring Orleans'
 * `CustomStorageAdaptor` over `ICustomStorageInterface<TState, TDelta>`.
 *
 * Where `LogViewAdaptorImpl` journals each event through the `StateMachineManager` substrate,
 * this one delegates entirely to the grain: the grain reads `(version, state)` back from
 * wherever it keeps them and appends a batch of deltas under a compare-and-set on the expected
 * version. The adaptor keeps only the cached view and the version it was read at -- it does not
 * retain the log, which is why `retrieveLogSegment` is unsupported here exactly as it is in
 * Orleans.
 */
export class CustomStorageLogViewAdaptorImpl<TState, TEvent> implements LogViewAdaptor<
  TState,
  TEvent
> {
  private cached: TState;
  private tentative: TState;
  private version = 0;
  private pending: TEvent[] = [];
  private readonly maxAttempts: number;
  /** The single in-flight confirm loop, if any -- see `confirmSubmittedEntries`. */
  private confirmLoop: Promise<void> | undefined;

  constructor(
    private readonly initial: () => TState,
    private readonly transition: (state: TState, event: TEvent) => TState,
    private readonly host: CustomStorageInterface<TState, TEvent>,
    options: CustomStorageLogViewAdaptorOptions = {},
  ) {
    this.cached = initial();
    this.tentative = this.cached;
    this.maxAttempts = options.maxAttempts ?? DEFAULT_MAX_ATTEMPTS;
  }

  /**
   * Loads the confirmed view from the host's storage. Called by the binder before activation,
   * mirroring `CustomStorageAdaptor.ReadAsync`. Unlike Orleans this does not retry: a grain that
   * cannot read its own state should fail activation rather than spin.
   */
  async read(): Promise<void> {
    const { version, state } = await this.host.readStateFromStorage();
    this.version = version;
    this.cached = state;
    this.recomputeTentative();
  }

  get confirmedView(): TState {
    return this.cached;
  }

  get tentativeView(): TState {
    return this.tentative;
  }

  get confirmedVersion(): number {
    return this.version;
  }

  get pendingCount(): number {
    return this.pending.length;
  }

  submit(event: TEvent): void {
    this.pending.push(event);
    this.tentative = this.transition(this.tentative, event);
  }

  submitRange(events: readonly TEvent[]): void {
    for (const event of events) this.submit(event);
  }

  /**
   * Persists every currently-pending event. Concurrent callers join the same in-flight loop --
   * two loops racing would each swap out `pending` and submit under the same expected version,
   * and one would necessarily lose the CAS. Same reasoning as `LogViewAdaptorImpl`.
   */
  async confirmSubmittedEntries(): Promise<void> {
    this.confirmLoop ??= this.runConfirmLoop().finally(() => {
      this.confirmLoop = undefined;
    });
    await this.confirmLoop;
  }

  private async runConfirmLoop(): Promise<void> {
    // Drain until nothing is pending: a concurrent raise can add events between two awaits.
    while (this.pending.length > 0) {
      const updates = this.pending;
      this.pending = [];

      let applied = false;
      let attempts = 0;
      let lastExpected = this.version;
      while (!applied && attempts < this.maxAttempts) {
        attempts++;
        lastExpected = this.version;
        try {
          applied = await this.host.applyUpdatesToStorage(updates, lastExpected);
        } catch {
          // A throw is a failed write, exactly as Orleans treats it: fall through to the
          // re-read below and try again against whatever storage now holds.
          applied = false;
        }

        if (applied) break;
        if (attempts >= this.maxAttempts) break;
        // Storage moved under us (or was briefly unavailable). Re-read, which resets the
        // expected version, then retry the same updates on top of what is now confirmed.
        await this.read();
      }

      if (!applied) {
        // Put the updates back, newest-last, so a later confirm retries them in order and
        // `pendingCount` still reports them. Orleans keeps them queued for the same reason.
        this.pending = [...updates, ...this.pending];
        this.recomputeTentative();
        // The version plays the etag's role: the CAS is on the log version, not a storage
        // etag. The stored version is left undefined on purpose -- a rejected CAS says only
        // that the version did not match, never what it actually is, and the final attempt
        // does not re-read. Claiming `this.version` here would just echo the expected value.
        throw new InconsistentStateError(
          `custom-storage log CAS failed after ${attempts} attempt(s) at expected version ${lastExpected}`,
          String(lastExpected),
          undefined,
        );
      }

      // The write is durable now, so a throwing transition must NOT be reported as a failed
      // write: re-read instead and let storage be the truth. Orleans tracks this separately as
      // `transitionssuccessful` for the same reason -- otherwise a half-applied fold would sit
      // in the view with the version already advanced past it.
      try {
        for (const event of updates) {
          this.version++;
          this.cached = this.transition(this.cached, event);
        }
        this.recomputeTentative();
      } catch {
        await this.read();
      }
    }
  }

  /**
   * Unsupported: the adaptor holds a view, not the log. Orleans' `CustomStorageAdaptor` does not
   * override `PrimaryBasedLogViewAdaptor.RetrieveLogSegment`, whose base throws
   * `NotSupportedException`. A grain that needs its own log back should read it from the same
   * storage it writes.
   */
  retrieveLogSegment(_fromVersion: number, _toVersion: number): readonly TEvent[] {
    throw new Error(
      "retrieveLogSegment is not supported under custom storage: the grain owns its log",
    );
  }

  /** Clears the host's stored state, then resyncs the view from it. */
  async clearLog(): Promise<void> {
    await this.host.clearStoredState();
    this.pending = [];
    await this.read();
  }

  private recomputeTentative(): void {
    this.tentative = this.pending.reduce(
      (state, event) => this.transition(state, event),
      this.cached,
    );
  }
}
