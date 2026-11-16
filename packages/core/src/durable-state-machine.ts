/**
 * A registered durable structure, mirroring Orleans' `IDurableStateMachine`.
 * The manager owns the log; a machine only knows how to (re)build itself from
 * applied op payloads and how to emit a snapshot of its current state. Payloads
 * are the machine's private op union — opaque to the manager.
 */
export interface DurableStateMachine {
  /** Name under which this machine is registered on the manager (= `stateName`). */
  readonly name: string;
  /** Discard all in-memory state, returning to empty/default (before a replay). */
  reset(): void;
  /** Apply one decoded op payload to in-memory state (replay, or after a local append). */
  apply(payload: unknown): void;
  /**
   * Produce a single op payload that, applied to a freshly `reset()` machine,
   * reconstructs current state in full. Used for compaction.
   */
  snapshot(): unknown;
  /**
   * Payloads this machine wants kept across a compaction in addition to its snapshot -- a
   * `JournaledGrain`'s confirmed events, whose history is not state a snapshot can rebuild. The
   * manager writes them ahead of the snapshot frames, and on replay hands them back through
   * {@link applyHistory}, never `apply`: they restore history without folding it into state again.
   * Absent or empty for a machine whose snapshot is the whole truth.
   */
  retainedHistory?(): unknown[];
  /** Restores retained history on replay (see {@link retainedHistory}); must not alter state. */
  applyHistory?(payloads: readonly unknown[]): void;
  /** Called once a compaction has been written durably; a machine drops anything it will not retain. */
  compacted?(): void;
}

/**
 * Per-grain owner of one append-only log shared by all the grain's durable
 * structures, mirroring Orleans' `IStateMachineManager`. Created once per
 * activation; machines register before the single `replay()`.
 */
export interface StateMachineManager {
  /** Register a machine before `replay()`. Throws on a duplicate name. */
  register(machine: DurableStateMachine): void;
  /** Read the log once and dispatch every entry to its owning machine. */
  replay(): Promise<void>;
  /**
   * Append one mutation for `machineName` carrying `payload`: frame it, write it
   * through storage under the version CAS, and snapshot + truncate when the log
   * crosses the configured threshold.
   */
  append(machineName: string, payload: unknown): Promise<void>;
  /**
   * Buffer one mutation for `machineName` without touching storage, mirroring how an Orleans
   * durable structure writes its command into the manager's journal writer. The caller (the
   * machine) has already applied the payload to its own memory; the entry is framed and
   * serialised now, so later changes to a mutated object do not alter what is persisted. It
   * becomes durable on the next {@link writeState}.
   */
  buffer(machineName: string, payload: unknown): void;
  /**
   * Orleans' `IJournaledStateManager.WriteStateAsync`: append everything buffered across ALL the
   * grain's structures in ONE storage write, under one version CAS. That write is the unit of
   * durability: the buffered entries land together or not at all. A no-op when nothing is
   * buffered. Overlapping calls run one after another; a later one writes whatever was buffered
   * by the time it runs.
   *
   * A failed write surfaces its error, rolls nothing back and keeps the entries buffered for the
   * next write; a lost-writer conflict (`InconsistentStateError`) additionally makes the NEXT
   * write begin by re-reading the log, discarding in-memory state and buffered entries.
   */
  writeState(): Promise<void>;
  /**
   * Append several mutations for `machineName`, in order, as ONE storage write: they land together
   * under one version CAS or not at all.
   */
  appendRange(machineName: string, payloads: readonly unknown[]): Promise<void>;
  /** Force a snapshot + truncate now. */
  compact(): Promise<void>;
  /**
   * Delete the log entirely and reset every registered machine to its initial
   * (empty) state, mirroring Orleans' `ClearLogAsync`.
   */
  clear(): Promise<void>;
}
