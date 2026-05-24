/** A pure fold of an event onto state — no I/O, no grain calls, no clock. */
export type Reducer<TState, TEvent> = (state: TState, event: TEvent) => TState;

/**
 * The grain-facing reducer facet (see [ADR 0006](../../docs/adr/0006-reducer-grains.md)).
 * State is immutable: a command handler `raise`s a past-tense event, which is
 * folded through the pure reducer to produce the next `value`. In snapshot mode
 * the folded `value` is what `write` persists; the events are transient.
 */
export interface ReducerState<TState, TEvent> {
  /** The current immutable state. Only `raise` advances it. */
  readonly value: TState;
  readonly etag: string | undefined;
  readonly exists: boolean;

  /** Fold an event onto the state, replacing `value`, and return the new state. */
  raise(event: TEvent): TState;

  /** Reload the snapshot from the store. */
  read(): Promise<void>;
  /** Persist the current folded state (snapshot mode). */
  write(): Promise<void>;
}
