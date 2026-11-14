import type { DurableStateMachine, StateMachineManager } from "@thresh/core/durable-state-machine";
import { InconsistentStateError } from "@thresh/core/errors";
import type { LogViewAdaptor } from "@thresh/core/journaled-grain";

/**
 * A framed log record for a `JournaledGrain`'s single log: either a raised
 * event, or a confirmed-state snapshot (compaction). The snapshot frame
 * carries the monotonic confirmed version alongside the state, mirroring
 * Orleans' `GrainStateWithMetaData.GlobalVersion` travelling with the
 * snapshot -- without it, compaction (which empties `confirmedEvents`) would
 * make the version appear to reset to the post-compaction entry count.
 */
type Frame<TState, TEvent> = { t: "event"; e: TEvent } | { t: "snap"; s: TState; v?: number };

/**
 * How many times `tryAppend` re-reads and retries a failed write before reporting `false` -- the
 * custom-storage adaptor's bounded CAS budget, for the same reason: Orleans retries until storage
 * recovers, which would park the caller indefinitely.
 */
const MAX_APPEND_ATTEMPTS = 5;

/**
 * One raised-but-not-yet-confirmed event, mirroring Orleans' `SubmissionEntry`:
 * `conditionalPosition` is the version the entry must land at (`undefined` for an unconditional
 * submit) and `resolve` settles a `tryAppend` caller's promise.
 */
interface PendingEntry<TEvent> {
  readonly event: TEvent;
  readonly conditionalPosition: number | undefined;
  readonly resolve: ((appended: boolean) => void) | undefined;
}

export interface LogViewAdaptorOptions {
  /**
   * Keep the confirmed event history across compaction (default `false`). The default is
   * snapshot-plus-tail, as Orleans' state-storage provider: a compaction truncates the log and the
   * activation holds only the events since the snapshot, so `retrieveLogSegment` throws for a range
   * that starts below the snapshot's version. `true` is the opt-in to whole-log retention, as
   * Orleans' log-storage provider; until the storage seam can snapshot without truncating, every
   * compaction then rewrites the whole retained history and the activation holds it in memory.
   */
  retainEvents?: boolean;
}

/**
 * The `LogViewAdaptor` implementation, mirroring Orleans' state-storage /
 * log-storage adaptors collapsed into one: it maintains the confirmed view (by
 * folding confirmed events through `transitionState`, starting from
 * `initialState`) and the tentative view (confirmed view folded with the
 * still-pending events), and persists confirmed events one at a time through
 * the grain's `StateMachineManager` — the same append-only journal substrate
 * `@durableState` fields use.
 */
export class LogViewAdaptorImpl<TState, TEvent>
  implements LogViewAdaptor<TState, TEvent>, DurableStateMachine
{
  readonly name = "journal";

  private confirmed: TState;
  private tentative: TState;
  private confirmedEvents: TEvent[] = [];
  /**
   * Monotonic count of every event ever confirmed, surviving compaction --
   * unlike `confirmedEvents.length`, which only counts entries still held
   * in-memory since the last snapshot. Restored from the snapshot frame on
   * replay (see `apply`).
   */
  private version = 0;
  private pending: PendingEntry<TEvent>[] = [];
  /** Entries taken off `pending` for a write that has not yet been applied to `confirmed`. */
  private inFlight = 0;
  /**
   * Count of raised-but-not-yet-confirmed events. Tracked separately from
   * `pending.length` (the event being appended is taken off `pending` before
   * its append, so its own `apply` does not fold it in twice): this only
   * decreases once an event has actually been persisted (or dropped on a
   * conflict), so a raise-then-fire-and-forget-confirm still reports a
   * nonzero tentative/confirmed version gap for as long as the persist is in
   * flight.
   */
  private unconfirmedCount = 0;
  /** The single in-flight `confirmSubmittedEntries` loop, if any (see below). */
  private confirmLoop: Promise<void> | undefined;
  private readonly retainEvents: boolean;

  constructor(
    private readonly initial: () => TState,
    private readonly transition: (state: TState, event: TEvent) => TState,
    private readonly manager: StateMachineManager,
    options: LogViewAdaptorOptions = {},
  ) {
    this.retainEvents = options.retainEvents ?? false;
    this.confirmed = initial();
    this.tentative = this.confirmed;
  }

  get confirmedView(): TState {
    return this.confirmed;
  }

  get tentativeView(): TState {
    return this.tentative;
  }

  get confirmedVersion(): number {
    return this.version;
  }

  get pendingCount(): number {
    return this.unconfirmedCount;
  }

  submit(event: TEvent): void {
    this.pending.push({ event, conditionalPosition: undefined, resolve: undefined });
    this.unconfirmedCount += 1;
    this.tentative = this.transition(this.tentative, event);
  }

  submitRange(events: readonly TEvent[]): void {
    for (const event of events) this.submit(event);
  }

  /**
   * Orleans' `TryAppend` over the journal substrate: the event is submitted CONDITIONAL on the
   * position it was raised at (the confirmed version plus everything queued or in flight ahead of
   * it). When the confirm pass finds that position moved -- another writer got in first, surfacing
   * as the substrate's storage CAS (`InconsistentStateError`) -- the adaptor re-reads the log and
   * `removeStaleConditionalUpdates` drops the conditional entries, answering them `false`, while
   * unconditional entries stay pending and are written on top of the refreshed view. So after a
   * `false` the confirmed view and version equal storage and a retry computed from them can
   * succeed, and an unconditional event raised just before is neither lost nor stuck.
   *
   * Any other failure is a failed write: the log is re-read, which decides whether the write landed
   * after all (then `true`), and the append is retried up to {@link MAX_APPEND_ATTEMPTS} times
   * before the entries are dropped and `false` reported. A log that cannot even be re-read
   * rethrows. In every unsuccessful outcome the conditional entries leave the pending queue, so no
   * later confirm can apply a write its caller was told failed.
   */
  tryAppend(event: TEvent): Promise<boolean> {
    return this.tryAppendRange([event]);
  }

  /**
   * {@link tryAppend} for several events: they take consecutive conditional positions and are
   * written by one confirm pass (one storage append) with unconditional events queued ahead, so
   * they land together or not at all.
   */
  async tryAppendRange(events: readonly TEvent[]): Promise<boolean> {
    if (events.length === 0) return true;
    let settle!: (appended: boolean) => void;
    const outcome = new Promise<boolean>((resolve) => (settle = resolve));
    const entries: PendingEntry<TEvent>[] = events.map((event, i) => ({
      event,
      conditionalPosition: this.version + this.inFlight + this.pending.length + i,
      resolve: settle,
    }));
    // The confirmed version once these events are appended: everything already pending lands first.
    const landsAt = this.version + this.inFlight + this.pending.length + entries.length;
    this.pending.push(...entries);
    this.unconfirmedCount += entries.length;
    for (const event of events) this.tentative = this.transition(this.tentative, event);
    for (let attempt = 1; ; attempt++) {
      try {
        await this.confirmSubmittedEntries();
        return await outcome;
      } catch (error) {
        if (error instanceof InconsistentStateError) {
          // The loop's conflict budget ran out: it has already dropped the conditional entries
          // and answered them false.
          return await outcome;
        }
        // A failed write, as the custom-storage adaptor and Orleans treat it: re-read storage,
        // which decides whether the write landed after all, and retry on top of it.
        try {
          await this.resync();
        } catch {
          // Storage cannot even be read: whether the write landed is unknowable here.
          this.withdraw(entries);
          throw error;
        }
        if (this.version >= landsAt) {
          this.withdraw(entries);
          return true;
        }
        if (attempt >= MAX_APPEND_ATTEMPTS) {
          this.withdraw(entries);
          return false;
        }
      }
    }
  }

  /**
   * Re-reads the log through the manager, keeping whatever is still pending: `replay` resets
   * every machine (this one's pending events included) before applying storage's entries.
   */
  private async resync(): Promise<void> {
    const pending = this.pending;
    await this.manager.replay();
    // Anything raised while the log was being read landed in the reset queue; it stays behind.
    this.pending = [...pending, ...this.pending];
    this.unconfirmedCount = this.pending.length;
    this.recomputeTentative();
  }

  /** Takes `entries` back out of the pending queue and the tentative view. */
  private withdraw(entries: readonly PendingEntry<TEvent>[]): void {
    for (const entry of entries) {
      const index = this.pending.indexOf(entry);
      if (index < 0) continue;
      this.pending.splice(index, 1);
      this.unconfirmedCount -= 1;
    }
    this.recomputeTentative();
  }

  private recomputeTentative(): void {
    this.tentative = this.pending.reduce(
      (state, entry) => this.transition(state, entry.event),
      this.confirmed,
    );
  }

  /**
   * Orleans' `RemoveStaleConditionalUpdates`: when any conditional entry's position no longer
   * equals `confirmed version + its place in the queue`, every conditional entry is removed and
   * answered `false`; unconditional entries stay pending, in order, to be written on top of the
   * refreshed view.
   */
  private removeStaleConditionalUpdates(): void {
    const stale = this.pending.some(
      (entry, pos) =>
        entry.conditionalPosition !== undefined &&
        entry.conditionalPosition !== this.version + this.inFlight + pos,
    );
    if (!stale) return;
    this.dropConditionalEntries();
  }

  private dropConditionalEntries(): void {
    const kept: PendingEntry<TEvent>[] = [];
    for (const entry of this.pending) {
      if (entry.conditionalPosition === undefined) {
        kept.push(entry);
      } else {
        entry.resolve?.(false);
        this.unconfirmedCount -= 1;
      }
    }
    this.pending = kept;
    this.recomputeTentative();
  }

  /**
   * Persists every currently-pending event. Concurrent callers (reentrant
   * grain methods can genuinely overlap) join the same in-flight loop rather
   * than each starting their own pass over `pending` — two loops racing to
   * swap-and-append against the same `StateMachineManager` would otherwise
   * both submit under the same expected version and one loses the CAS.
   */
  async confirmSubmittedEntries(): Promise<void> {
    this.confirmLoop ??= this.runConfirmLoop().finally(() => {
      this.confirmLoop = undefined;
    });
    await this.confirmLoop;
  }

  private async runConfirmLoop(): Promise<void> {
    // Keep draining until nothing is left pending: a concurrent raise can add
    // more events to the end of `pending` while this loop is mid-flight
    // (between two `await`s), and those must be confirmed too before any
    // joined caller's promise settles.
    //
    // Each pass writes EVERYTHING pending as one storage append (group
    // commit: N raised events cost one write, not N). Mirrors Orleans
    // (`PrimaryBasedLogViewAdaptor.UpdatePrimary`): a batch leaves `pending` only once it is
    // actually persisted, so a failed append leaves it (and everything queued after it) in place
    // for a retry, never silently dropped. A lost storage CAS (`InconsistentStateError`) re-reads
    // the log and goes round again: conditional entries are then stale and answered false,
    // unconditional ones are written on top of the refreshed view.
    let conflicts = 0;
    for (;;) {
      this.removeStaleConditionalUpdates();
      if (this.pending.length === 0) return;
      // Removed up front, not after: `manager.appendRange` calls this
      // machine's own `apply` synchronously on success, which recomputes
      // `tentative` by folding `pending` over `confirmed` -- it must no longer
      // see these events there, or they would be applied twice (once via
      // `confirmed`, once via the recompute).
      const batch = this.pending.splice(0);
      this.inFlight = batch.length;
      try {
        await this.manager.appendRange(
          this.name,
          batch.map((entry) => ({ t: "event", e: entry.event }) satisfies Frame<TState, TEvent>),
        );
      } catch (error) {
        this.inFlight = 0;
        // Back at the front, ahead of anything concurrently raised.
        this.pending.unshift(...batch);
        this.recomputeTentative();
        if (!(error instanceof InconsistentStateError)) throw error;
        conflicts += 1;
        if (conflicts >= MAX_APPEND_ATTEMPTS) {
          // Out of budget: conditional entries must not stay (their callers are told false); the
          // unconditional ones stay pending for a later confirm, as in the custom-storage adaptor.
          this.dropConditionalEntries();
          throw error;
        }
        // Another writer moved the log: re-read, so the confirmed view and version equal storage.
        await this.resync();
        continue;
      }
      this.inFlight = 0;
      conflicts = 0;
      this.unconfirmedCount -= batch.length;
      for (const entry of batch) entry.resolve?.(true);
    }
  }

  retrieveLogSegment(fromVersion: number, toVersion: number): readonly TEvent[] {
    // `confirmedEvents` only holds events confirmed since the last snapshot;
    // `baseVersion` is the (global) version compaction last collapsed away, so
    // a requested range is translated into an offset into it.
    const baseVersion = this.version - this.confirmedEvents.length;
    if (fromVersion < baseVersion || toVersion < fromVersion || toVersion > this.version) {
      if (fromVersion < baseVersion && toVersion >= fromVersion && toVersion <= this.version) {
        throw new Error(
          `events below version ${baseVersion} were truncated by compaction; ` +
            `enable the retainEvents option (silo config retainJournaledEvents) to keep the whole history`,
        );
      }
      throw new Error(`invalid range [${fromVersion}, ${toVersion}]`);
    }
    return this.confirmedEvents.slice(fromVersion - baseVersion, toVersion - baseVersion);
  }

  async clearLog(): Promise<void> {
    await this.manager.clear();
  }

  // --- DurableStateMachine: how the manager (re)builds this grain's log ---

  reset(): void {
    this.confirmed = this.initial();
    this.confirmedEvents = [];
    this.version = 0;
    this.pending = [];
    this.inFlight = 0;
    this.unconfirmedCount = 0;
    this.tentative = this.confirmed;
  }

  apply(payload: unknown): void {
    const frame = payload as Frame<TState, TEvent>;
    if (frame.t === "event") {
      this.confirmed = this.transition(this.confirmed, frame.e);
      this.confirmedEvents.push(frame.e);
      this.version += 1;
      if (this.inFlight > 0) this.inFlight -= 1;
    } else {
      this.confirmed = frame.s;
      // Retained history (restored through `applyHistory`, ahead of this frame) stays; otherwise
      // the snapshot replaces the events below it.
      if (!this.retainEvents) this.confirmedEvents = [];
      // A snapshot written before the version travelled with it has no `v`;
      // fall back to what such a log always reported (a count from zero at the
      // snapshot) rather than poisoning the version with `undefined`.
      this.version = frame.v ?? 0;
    }
    // Recompute (don't just mirror confirmed): events may have been raised
    // concurrently with the append that triggered this `apply`.
    this.recomputeTentative();
  }

  retainedHistory(): unknown[] {
    return this.retainEvents ? [...this.confirmedEvents] : [];
  }

  applyHistory(payloads: readonly unknown[]): void {
    this.confirmedEvents.push(...(payloads as TEvent[]));
  }

  compacted(): void {
    if (!this.retainEvents) this.confirmedEvents = [];
  }

  snapshot(): unknown {
    return { t: "snap", s: this.confirmed, v: this.version } satisfies Frame<TState, TEvent>;
  }
}
