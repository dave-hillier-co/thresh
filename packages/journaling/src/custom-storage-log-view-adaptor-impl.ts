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
   * budget surfaces the conflict to the caller instead, and unconditional events stay pending so
   * a later confirm can retry (conditional ones are dropped and reported false -- see
   * {@link CustomStorageLogViewAdaptorImpl.tryAppend}).
   */
  maxAttempts?: number;
}

/**
 * One raised-but-not-yet-confirmed event, mirroring Orleans' `SubmissionEntry`:
 * `conditionalPosition` is the version the entry must land at (or `undefined` for an
 * unconditional `submit`), and `resolve` settles a `tryAppend` caller's promise.
 */
interface PendingEntry<TEvent> {
  readonly event: TEvent;
  readonly conditionalPosition: number | undefined;
  readonly resolve: ((appended: boolean) => void) | undefined;
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
  private pending: PendingEntry<TEvent>[] = [];
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
    this.pending.push({ event, conditionalPosition: undefined, resolve: undefined });
    this.tentative = this.transition(this.tentative, event);
  }

  submitRange(events: readonly TEvent[]): void {
    for (const event of events) this.submit(event);
  }

  /**
   * Orleans' `PrimaryBasedLogViewAdaptor.TryAppend`: the entry is submitted CONDITIONAL on the
   * position it was raised at (`confirmed version + pending count`, exactly the tentative
   * version). If the confirmed version has moved past that position by the time the write runs,
   * the entry is dropped on the first conflict -- `RemoveStaleConditionalUpdates` -- and reported
   * `false`; it is never retried on the moved base and never left pending. One deliberate
   * deviation: when the bounded attempt budget exhausts WITHOUT the position going stale (storage
   * flaky, version unmoved), Orleans would keep retrying forever on its background loop; here the
   * conditional entry is dropped and reported `false` too, because leaving it pending would let a
   * LATER confirm apply an append its caller was already told failed.
   */
  tryAppend(event: TEvent): Promise<boolean> {
    let resolveOutcome!: (appended: boolean) => void;
    const outcome = new Promise<boolean>((resolve) => (resolveOutcome = resolve));
    this.pending.push({
      event,
      conditionalPosition: this.version + this.pending.length,
      resolve: resolveOutcome,
    });
    this.tentative = this.transition(this.tentative, event);

    const confirm = async (): Promise<boolean> => {
      try {
        await this.confirmSubmittedEntries();
      } catch (error) {
        // Budget exhaustion drops conditional entries and settles them false; anything the loop
        // threw that left this entry unsettled is a genuine failure and must propagate.
        if (!(error instanceof InconsistentStateError)) throw error;
      }
      return outcome;
    };
    return confirm();
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
    // Entries stay IN `pending` until written (as Orleans keeps them queued until removal), so a
    // `tryAppend` raised while a write is in flight still computes its conditional position over
    // the whole queue. Drain until nothing is pending: a concurrent raise can add events between
    // two awaits.
    let attempts = 0;
    while (this.pending.length > 0) {
      // A conditional entry whose position an earlier write or re-read moved past is already
      // stale; drop it before it ever reaches storage, as Orleans does after every refresh.
      this.pending = this.removeStaleConditionalUpdates(this.pending);
      if (this.pending.length === 0) {
        this.recomputeTentative();
        break;
      }

      // Snapshot the batch: concurrent raises append BEHIND it while the write is in flight.
      const batch = this.pending.slice();
      attempts++;
      const lastExpected = this.version;
      let applied = false;
      try {
        applied = await this.host.applyUpdatesToStorage(
          batch.map((entry) => entry.event),
          lastExpected,
        );
      } catch {
        // A throw is a failed write, exactly as Orleans treats it: fall through to the
        // re-read below and try again against whatever storage now holds.
        applied = false;
      }

      if (applied) {
        attempts = 0;
        // The written entries are still at the queue front (only this loop removes entries);
        // drop exactly them, leaving anything raised during the write for the next round.
        this.pending.splice(0, batch.length);
        // The write is durable now, so a throwing transition must NOT be reported as a failed
        // write: re-read instead and let storage be the truth. Orleans tracks this separately as
        // `transitionssuccessful` for the same reason -- otherwise a half-applied fold would sit
        // in the view with the version already advanced past it.
        try {
          for (const entry of batch) {
            this.version++;
            this.cached = this.transition(this.cached, entry.event);
          }
          this.recomputeTentative();
        } catch {
          await this.read();
        }
        for (const entry of batch) entry.resolve?.(true);
        continue;
      }

      if (attempts >= this.maxAttempts) {
        // The UNCONDITIONAL updates stay pending, in order, so a later confirm retries them and
        // `pendingCount` still reports them -- Orleans keeps them queued for the same reason.
        // Conditional entries must NOT stay: their callers are being told the append failed, and
        // a later confirm silently applying them would land a write the caller was told failed.
        // Settle them false and drop them.
        const kept: PendingEntry<TEvent>[] = [];
        for (const entry of this.pending) {
          if (entry.conditionalPosition === undefined) kept.push(entry);
          else entry.resolve?.(false);
        }
        this.pending = kept;
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

      // Storage moved under us (or was briefly unavailable). Re-read, which resets the expected
      // version, then retry on top of what is now confirmed -- minus any conditional entry the
      // re-read revealed as stale, dropped by the check at the top of the next iteration
      // (Orleans' `RemoveStaleConditionalUpdates` after every refresh).
      await this.read();
    }
  }

  /**
   * Orleans' `RemoveStaleConditionalUpdates` over one slice of the queue: a conditional entry
   * whose position no longer equals `confirmed version + its position in the queue` has lost its
   * race -- settle its promise `false` and drop it. Once one conditional entry has failed, every
   * conditional entry behind it fails too (its base includes the dropped one).
   */
  private removeStaleConditionalUpdates(
    entries: readonly PendingEntry<TEvent>[],
  ): PendingEntry<TEvent>[] {
    const kept: PendingEntry<TEvent>[] = [];
    let foundFailedConditionalUpdates = false;
    for (const [pos, entry] of entries.entries()) {
      if (
        entry.conditionalPosition !== undefined &&
        (foundFailedConditionalUpdates || entry.conditionalPosition !== this.version + pos)
      ) {
        foundFailedConditionalUpdates = true;
        entry.resolve?.(false);
      } else {
        kept.push(entry);
      }
    }
    return kept;
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
    // A pending conditional entry can never land now; settle its caller rather than hang it.
    for (const entry of this.pending) entry.resolve?.(false);
    this.pending = [];
    await this.read();
  }

  private recomputeTentative(): void {
    this.tentative = this.pending.reduce(
      (state, entry) => this.transition(state, entry.event),
      this.cached,
    );
  }
}
