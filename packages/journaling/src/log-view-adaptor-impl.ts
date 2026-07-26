import type { DurableStateMachine, StateMachineManager } from "@thresh/core/durable-state-machine";
import type { LogViewAdaptor } from "@thresh/core/journaled-grain";

/** A framed log record for a `JournaledGrain`'s single log: either a raised event, or a confirmed-state snapshot (compaction). */
type Frame<TState, TEvent> = { t: "event"; e: TEvent } | { t: "snap"; s: TState };

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
    return this.confirmedEvents.length;
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
    // more events while this loop is mid-flight (between two `await`s), and
    // those must be confirmed too before any joined caller's promise settles.
    while (this.pending.length > 0) {
      const toConfirm = this.pending;
      this.pending = [];
      for (const event of toConfirm) {
        await this.manager.append(this.name, { t: "event", e: event } satisfies Frame<TState, TEvent>);
        this.unconfirmedCount -= 1;
      }
    }
  }

  retrieveLogSegment(fromVersion: number, toVersion: number): readonly TEvent[] {
    if (fromVersion < 0 || toVersion < fromVersion || toVersion > this.confirmedEvents.length) {
      throw new Error(`invalid range [${fromVersion}, ${toVersion}]`);
    }
    return this.confirmedEvents.slice(fromVersion, toVersion);
  }

  async clearLog(): Promise<void> {
    await this.manager.clear();
  }

  // --- DurableStateMachine: how the manager (re)builds this grain's log ---

  reset(): void {
    this.confirmed = this.initial();
    this.confirmedEvents = [];
    this.pending = [];
    this.unconfirmedCount = 0;
    this.tentative = this.confirmed;
  }

  apply(payload: unknown): void {
    const frame = payload as Frame<TState, TEvent>;
    if (frame.t === "event") {
      this.confirmed = this.transition(this.confirmed, frame.e);
      this.confirmedEvents.push(frame.e);
    } else {
      this.confirmed = frame.s;
      this.confirmedEvents = [];
    }
    // Recompute (don't just mirror confirmed): events may have been raised
    // concurrently with the append that triggered this `apply`.
    this.tentative = this.pending.reduce(this.transition, this.confirmed);
  }

  snapshot(): unknown {
    return { t: "snap", s: this.confirmed } satisfies Frame<TState, TEvent>;
  }
}
