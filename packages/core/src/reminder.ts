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
export const RemindableInterface: GrainInterface<Remindable> = defineGrainInterface<Remindable>(
  "system.Remindable",
  { methods: ["receiveReminder"] },
);

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

/**
 * The runtime-facing handle a grain's `registerReminder` delegates to. The
 * reminder service implements it; the hosting layer injects it into the runtime.
 */
export interface ReminderRegistry {
  register(grainId: GrainId, name: string, due: Duration, period: Duration): Promise<void>;
  unregister(grainId: GrainId, name: string): Promise<void>;
}
