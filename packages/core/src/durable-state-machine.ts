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
  /** Force a snapshot + truncate now. */
  compact(): Promise<void>;
}
