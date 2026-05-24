import type { Duration } from "./duration";
import type { GrainId } from "./grain-id";

/** Implemented by grains that receive durable reminder ticks. */
export interface Remindable {
  receiveReminder(name: string, status: TickStatus): Promise<void>;
}

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
}
