import type { GrainId } from "./grain-id";

/** One serialized, already-framed log record. Opaque to the storage provider. */
export type JournalEntry = string;

/** The log as a provider hands it back: ordered entries plus the CAS version. */
export interface JournalSegment {
  /** All live entries in append order (a snapshot frame, if any, comes first). */
  entries: JournalEntry[];
  /** CAS token: the count of appends + replaces applied. `undefined` = no log yet. */
  version: number | undefined;
}

/**
 * A pluggable append-only journal store, mirroring Orleans' `ILogStorage` /
 * `IStateMachineStorage`. The provider stores ordered opaque entries plus a
 * monotonically increasing `version`; the manager owns framing. Appends and
 * the snapshot replace are conditional on the caller's last-seen `version`
 * (`undefined` = "no log yet"), carrying `GrainStorage`'s etag optimistic-
 * concurrency contract over to a log. A losing writer raises
 * `InconsistentStateError` (see `@thresh/core/errors`).
 *
 * `signal`, when given, is an ambient cancellation/deadline signal for a call
 * (see `GrainStorage`'s doc — same contract). A provider honours it on a
 * best-effort basis against its own network calls; a provider that cannot
 * cancel mid-flight may instead only abandon the *wait* for an already-sent
 * call (`@thresh/core/abort`'s `raceSignal`), or ignore it entirely.
 */
export interface JournalStorage {
  /** Read the whole log for `(logName, grainId)`. */
  read(logName: string, grainId: GrainId, signal?: AbortSignal): Promise<JournalSegment>;

  /**
   * Append `entries` to the end of the log iff the stored version equals
   * `expectedVersion` (`undefined` ⇒ the log must not exist yet). Returns the
   * new version; throws `InconsistentStateError` on mismatch.
   */
  append(
    logName: string,
    grainId: GrainId,
    entries: readonly JournalEntry[],
    expectedVersion: number | undefined,
    signal?: AbortSignal,
  ): Promise<number>;

  /**
   * Atomically replace the entire log with `entries` (the snapshot frames) iff
   * the stored version equals `expectedVersion`. Used for snapshot + truncate.
   * Returns the new version; throws `InconsistentStateError` on mismatch.
   */
  replace(
    logName: string,
    grainId: GrainId,
    entries: readonly JournalEntry[],
    expectedVersion: number | undefined,
    signal?: AbortSignal,
  ): Promise<number>;

  /** Delete the log entirely. */
  clear(logName: string, grainId: GrainId, signal?: AbortSignal): Promise<void>;
}
