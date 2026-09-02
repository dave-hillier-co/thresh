import type { Duration } from "./duration";
import { defineGrainInterface, type GrainInterface } from "./grain-interface";
import type { GrainId } from "./grain-id";

/** Implemented by grains that receive durable reminder ticks. */
export interface Remindable {
  receiveReminder(name: string, status: TickStatus): Promise<void>;
}

/**
 * System interface used to route a reminder tick to a grain's single activation
 * through the normal dispatch path (directory → placement), so delivery never
 * creates a second activation on the silo that merely owns the reminder.
 */
export const RemindableInterface: GrainInterface<Remindable> =
  defineGrainInterface<Remindable>("system.Remindable");

export interface TickStatus {
  firstTickAt: Date;
  period: Duration;
  currentTickAt: Date;
}

/** A durable reminder record. */
export interface ReminderEntry {
  grainId: GrainId;
  name: string;
  startAt: Date;
  period: Duration;
  etag: string;
  /**
   * The instant this reminder last ticked, if ever (issue: reminder
   * double-fire on rebalance). A new owner's `reconcile()` schedules the
   * next tick from this — the next period boundary after
   * `max(lastFiredAt, startAt)` — rather than recomputing due time from the
   * original `startAt`, which would otherwise fire immediately for every
   * reminder in a range that just moved.
   */
  lastFiredAt?: Date;
}

/** Fields needed to create or update a reminder (the table assigns the etag). */
export interface ReminderRegistration {
  grainId: GrainId;
  name: string;
  startAt: Date;
  period: Duration;
}

/**
 * Pluggable durable store for reminders, mirroring Orleans `IReminderTable`.
 * `readRange` supports hash-range ownership: each silo reads the reminders whose
 * grain hashes into the range it owns and fires them locally.
 */
export interface ReminderTable {
  upsert(registration: ReminderRegistration): Promise<string>;
  remove(grainId: GrainId, name: string, etag: string): Promise<boolean>;
  read(grainId: GrainId, name: string): Promise<ReminderEntry | undefined>;
  readForGrain(grainId: GrainId): Promise<ReminderEntry[]>;
  readRange(hashBegin: number, hashEnd: number): Promise<ReminderEntry[]>;
  /**
   * Persist the instant a periodic reminder just ticked, so a future owner's
   * `reconcile()` resumes from it instead of the original `startAt` (issue:
   * reminder double-fire on rebalance). `etag` guards against a concurrent
   * `upsert`/`remove`: returns the (unchanged) etag on success, or `undefined`
   * if the etag no longer matches — the caller should not resurrect a
   * registration that was concurrently changed or removed.
   */
  recordFired(
    grainId: GrainId,
    name: string,
    etag: string,
    firedAt: Date,
  ): Promise<string | undefined>;
}

/**
 * A grain-facing view of a reminder (Orleans `IGrainReminder`, widened with the
 * fields a grain can usefully introspect). Returned by `GrainRuntime.getReminder`/
 * `getReminders` — it omits `grainId`/`etag`, which are internal to the reminder
 * table and never needed by grain code reading back its own reminders.
 */
export interface GrainReminder {
  name: string;
  startAt: Date;
  period: Duration;
}

/**
 * The runtime-facing handle a grain's `registerReminder` delegates to. The
 * reminder service implements it; the hosting layer injects it into the runtime.
 */
export interface ReminderRegistry {
  register(grainId: GrainId, name: string, due: Duration, period: Duration): Promise<void>;
  unregister(grainId: GrainId, name: string): Promise<void>;
  /** Look up a reminder registered to the grain (Orleans `IReminderRegistry.GetReminder`). */
  getReminder(grainId: GrainId, name: string): Promise<ReminderEntry | undefined>;
  /** All reminders registered to the grain (Orleans `IReminderRegistry.GetReminders`). */
  getReminders(grainId: GrainId): Promise<ReminderEntry[]>;
}
