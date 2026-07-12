import { Grain } from "./grain";

/**
 * The log-consistency protocol surface a `JournaledGrain` delegates to,
 * mirroring Orleans' `ILogViewAdaptor<TGrainState, TEventBase>`. An
 * implementation is installed by the journaling layer (mirroring
 * `InstallAdaptor`) before activation; a grain never talks to it directly.
 */
export interface LogViewAdaptor<TState, TEvent> {
  /** The confirmed view: reflects only events that have been persisted. */
  readonly confirmedView: TState;
  /** The tentative view: confirmed events plus any still-pending ones. */
  readonly tentativeView: TState;
  /** Count of confirmed events (equivalently, the confirmed view's version). */
  readonly confirmedVersion: number;
  /** Count of raised-but-not-yet-confirmed events. */
  readonly pendingCount: number;
  /** Append one event to the pending queue and fold it into the tentative view. */
  submit(event: TEvent): void;
  /** Append several events atomically, as `submit` would for each in order. */
  submitRange(events: readonly TEvent[]): void;
  /** Persist every currently-pending event, promoting it into the confirmed view. */
  confirmSubmittedEntries(): Promise<void>;
  /** Read a slice of the confirmed event sequence. */
  retrieveLogSegment(fromVersion: number, toVersion: number): readonly TEvent[];
  /** Reset the confirmed log (and drop pending events) back to the initial state. */
  clearLog(): Promise<void>;
}

/**
 * A base class for log-consistent grains, mirroring Orleans'
 * `JournaledGrain<TGrainState, TEventBase>`. Subclasses raise events instead of
 * mutating state directly; `raiseEvent` is visible immediately through
 * `tentativeState` but only reflected in `state`/`version` once `confirmEvents`
 * has persisted it. The event log lives in the journaling package's
 * `JournalStorage` substrate — see `bindJournaledGrain`, which installs the
 * adaptor and replays the confirmed log on activation.
 */
export abstract class JournaledGrain<TState, TEvent> extends Grain {
  private adaptor: LogViewAdaptor<TState, TEvent> | undefined;

  /**
   * Wired by the journaling layer (`bindJournaledGrain`) before activation.
   * Not part of the grain's application-facing surface.
   */
  installLogViewAdaptor(adaptor: LogViewAdaptor<TState, TEvent>): void {
    this.adaptor = adaptor;
  }

  /**
   * The state before any event has been applied. Invoked by the journaling
   * layer, not part of the grain's application-facing surface.
   */
  abstract initialState(): TState;

  /**
   * Pure event application: returns the next state for `event` applied to
   * `state`. Invoked by the journaling layer, not part of the grain's
   * application-facing surface.
   */
  abstract transitionState(state: TState, event: TEvent): TState;

  private get logViewAdaptor(): LogViewAdaptor<TState, TEvent> {
    if (this.adaptor === undefined) {
      throw new Error(
        "JournaledGrain: no log-consistency adaptor installed (is journal storage configured on this silo?)",
      );
    }
    return this.adaptor;
  }

  /** Raises an event: visible immediately in `tentativeState`, not yet confirmed. */
  protected raiseEvent(event: TEvent): void {
    this.logViewAdaptor.submit(event);
  }

  /** Raises multiple events as an atomic sequence. */
  protected raiseEvents(events: readonly TEvent[]): void {
    this.logViewAdaptor.submitRange(events);
  }

  /** Persists all previously-raised, still-pending events and confirms them. */
  protected confirmEvents(): Promise<void> {
    return this.logViewAdaptor.confirmSubmittedEntries();
  }

  /** Clears the confirmed log and resets to the initial state, discarding pending events. */
  protected clearLog(): Promise<void> {
    return this.logViewAdaptor.clearLog();
  }

  /** Retrieves a segment of the confirmed event sequence. */
  protected retrieveConfirmedEvents(fromVersion: number, toVersion: number): readonly TEvent[] {
    return this.logViewAdaptor.retrieveLogSegment(fromVersion, toVersion);
  }

  /** The confirmed state: reflects only events that have been persisted. */
  protected get state(): TState {
    return this.logViewAdaptor.confirmedView;
  }

  /** The tentative state: confirmed events plus any still-pending ones. */
  protected get tentativeState(): TState {
    return this.logViewAdaptor.tentativeView;
  }

  /** The confirmed version: the number of confirmed events. */
  protected get version(): number {
    return this.logViewAdaptor.confirmedVersion;
  }

  /** The tentative version: the confirmed version plus still-pending events. */
  protected get tentativeVersion(): number {
    return this.logViewAdaptor.confirmedVersion + this.logViewAdaptor.pendingCount;
  }
}
