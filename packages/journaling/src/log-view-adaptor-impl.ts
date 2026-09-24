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
type Frame<TState, TEvent> = { t: "event"; e: TEvent } | { t: "snap"; s: TState; v: number };

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
  private pending: TEvent[] = [];
  /**
   * Count of raised-but-not-yet-confirmed events. Tracked separately from
   * `pending.length` (which gets swapped out at the *start* of a confirmation
   * so concurrently-raised events queue behind it): this only decreases once
   * an event has actually been persisted, so a raise-then-fire-and-forget-
   * confirm still reports a nonzero tentative/confirmed version gap for as
   * long as the persist is in flight.
   */
  private unconfirmedCount = 0;
  /** The single in-flight `confirmSubmittedEntries` loop, if any (see below). */
  private confirmLoop: Promise<void> | undefined;

  constructor(
    private readonly initial: () => TState,
    private readonly transition: (state: TState, event: TEvent) => TState,
    private readonly manager: StateMachineManager,
  ) {
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
    this.pending.push(event);
    this.unconfirmedCount += 1;
    this.tentative = this.transition(this.tentative, event);
  }

  submitRange(events: readonly TEvent[]): void {
    for (const event of events) this.submit(event);
  }

  /**
   * Orleans' `TryAppend` over the journal substrate. This adaptor's grain is the log's single
   * writer through its `StateMachineManager`, so the raise cannot lose a race to another log
   * writer the way the custom-storage adaptor's version CAS can; the one conflict left is the
   * substrate's own storage CAS (a duplicate activation), which surfaces as an
   * `InconsistentStateError` from the append. That conflict reports `false` -- and the swapped-out
   * batch is not requeued by `runConfirmLoop`, so the event is dropped, never applied by a later
   * confirm. Anything else is a genuine storage failure and propagates.
   */
  async tryAppend(event: TEvent): Promise<boolean> {
    this.submit(event);
    try {
      await this.confirmSubmittedEntries();
      return true;
    } catch (error) {
      if (error instanceof InconsistentStateError) return false;
      throw error;
    }
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
    // Mirrors Orleans (`LogViewAdaptor.cs`): an event is removed from `pending`
    // only once it is actually persisted, one at a time, so a failed append
    // leaves it (and everything queued after it) in place for a later
    // `confirmSubmittedEntries` to re-read and retry -- never silently
    // dropped. The one documented exception is `InconsistentStateError` (the
    // substrate's version CAS): that is `tryAppend`'s conditional-conflict
    // signal, which Orleans drops rather than retries on the moved base --
    // see its doc comment above.
    while (this.pending.length > 0) {
      // Removed up front, not after: `manager.append` calls this machine's own
      // `apply` synchronously on success, which recomputes `tentative` by
      // folding `pending` over `confirmed` -- it must no longer see this event
      // there, or it would be applied twice (once via `confirmed`, once via
      // the recompute).
      const event = this.pending.shift()!;
      try {
        await this.manager.append(this.name, { t: "event", e: event } satisfies Frame<
          TState,
          TEvent
        >);
      } catch (error) {
        if (!(error instanceof InconsistentStateError)) {
          // Put it back at the front, ahead of anything concurrently raised,
          // for a later `confirmSubmittedEntries` to re-read and retry.
          this.pending.unshift(event);
          throw error;
        }
        this.unconfirmedCount -= 1;
        throw error;
      }
      this.unconfirmedCount -= 1;
    }
  }

  retrieveLogSegment(fromVersion: number, toVersion: number): readonly TEvent[] {
    // `confirmedEvents` only holds events confirmed since the last snapshot;
    // `baseVersion` is the (global) version compaction last collapsed away, so
    // a requested range is translated into an offset into it.
    const baseVersion = this.version - this.confirmedEvents.length;
    if (
      fromVersion < baseVersion ||
      toVersion < fromVersion ||
      toVersion > this.version
    ) {
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
    this.unconfirmedCount = 0;
    this.tentative = this.confirmed;
  }

  apply(payload: unknown): void {
    const frame = payload as Frame<TState, TEvent>;
    if (frame.t === "event") {
      this.confirmed = this.transition(this.confirmed, frame.e);
      this.confirmedEvents.push(frame.e);
      this.version += 1;
    } else {
      this.confirmed = frame.s;
      this.confirmedEvents = [];
      this.version = frame.v;
    }
    // Recompute (don't just mirror confirmed): events may have been raised
    // concurrently with the append that triggered this `apply`.
    this.tentative = this.pending.reduce(this.transition, this.confirmed);
  }

  snapshot(): unknown {
    return { t: "snap", s: this.confirmed, v: this.version } satisfies Frame<TState, TEvent>;
  }
}
