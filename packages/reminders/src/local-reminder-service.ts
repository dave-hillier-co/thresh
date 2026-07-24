import { durationToMs, type Duration } from "@tsva/core/duration";
import type { GrainId } from "@tsva/core/grain-id";
import { isHashInRanges, type HashRange } from "@tsva/core/hash-ring";
import { noopLogger, type Logger } from "@tsva/core/logger";
import { recordReminderFired, recordReminderMissed } from "@tsva/observability/reminder-metrics";
import type {
  ReminderEntry,
  ReminderRegistry,
  ReminderTable,
  TickStatus,
} from "@tsva/core/reminder";
import type { TimeProvider, TimerHandle } from "@tsva/core/time-provider";

/** Delivers a due reminder: the silo wires this to reactivate the grain and call `receiveReminder`. */
export type ReminderFire = (grainId: GrainId, name: string, status: TickStatus) => Promise<void>;

export type { HashRange };

/**
 * Tunables for the reminder service. Mirrors a subset of Orleans
 * `ReminderOptions`; everything is optional with safe defaults.
 */
export interface ReminderServiceOptions {
  /**
   * Minimum allowed reminder period. Orleans defaults to 1 minute — high
   * frequency reminders are dangerous in production. Tests and dev hosts can
   * lower this (e.g. `{ ms: 0 }`).
   */
  minimumPeriod?: Duration;
  /** Logger for reconcile/fire/cleanup errors that would otherwise be swallowed. */
  logger?: Logger;
}

/** Orleans `ReminderOptions.MinimumReminderPeriod` default: 1 minute. */
const DEFAULT_MINIMUM_PERIOD_MS = 60_000;

interface Scheduled {
  handle: TimerHandle;
  entry: ReminderEntry;
}

/**
 * Per-silo reminder service (Orleans `LocalReminderService`). It owns hash
 * ranges of the ring and fires the reminders whose grain hashes into them,
 * reading them from the durable table. A reminder survives deactivation and the
 * owning silo's death: another silo that takes over the range re-reads the table
 * and resumes firing (`refreshOwnership`). It also re-reads its ranges
 * periodically, so a reminder registered on a silo that does not own it is
 * discovered and fired by the silo that does. Driven by the injectable clock.
 */
export class LocalReminderService implements ReminderRegistry {
  private readonly scheduled = new Map<string, Scheduled>();
  private ranges: readonly HashRange[];
  private refreshHandle: TimerHandle | undefined;
  private readonly logger: Logger;
  private readonly minimumPeriodMs: number;

  constructor(
    private readonly table: ReminderTable,
    private readonly time: TimeProvider,
    private readonly onFire: ReminderFire,
    ranges: readonly HashRange[] = [[0, 0x1_0000_0000]],
    /** How often to re-read owned ranges from the table (0 disables periodic refresh). */
    private readonly refreshIntervalMs = 0,
    options: ReminderServiceOptions = {},
  ) {
    this.ranges = ranges;
    this.logger = options.logger ?? noopLogger;
    this.minimumPeriodMs =
      options.minimumPeriod === undefined
        ? DEFAULT_MINIMUM_PERIOD_MS
        : durationToMs(options.minimumPeriod);
  }

  /** Register (or update) a reminder; schedules it locally if this silo owns it. */
  async register(grainId: GrainId, name: string, due: Duration, period: Duration): Promise<void> {
    const dueMs = durationToMs(due);
    const periodMs = durationToMs(period);
    if (dueMs < 0) throw new RangeError(`reminder ${name}: due time must not be negative`);
    if (periodMs < 0) throw new RangeError(`reminder ${name}: period must not be negative`);
    if (periodMs < this.minimumPeriodMs) {
      throw new RangeError(
        `reminder ${name}: period ${periodMs}ms is below the minimum allowed (${this.minimumPeriodMs}ms)`,
      );
    }
    const startAt = new Date(this.time.now() + dueMs);
    const etag = await this.table.upsert({ grainId, name, startAt, period });
    if (this.owns(grainId)) this.scheduleEntry({ grainId, name, startAt, period, etag });
  }

  async unregister(grainId: GrainId, name: string): Promise<void> {
    const entry = await this.table.read(grainId, name);
    if (entry !== undefined) await this.table.remove(grainId, name, entry.etag);
    this.cancel(this.key(grainId, name));
  }

  async getReminder(grainId: GrainId, name: string): Promise<ReminderEntry | undefined> {
    return this.table.read(grainId, name);
  }

  async getReminders(grainId: GrainId): Promise<ReminderEntry[]> {
    return this.table.readForGrain(grainId);
  }

  /** On a membership change (or first start): adopt the given ranges and reconcile. */
  async refreshOwnership(ranges: readonly HashRange[]): Promise<void> {
    this.ranges = ranges;
    try {
      await this.reconcile();
    } catch (error) {
      // Don't lose the periodic-refresh schedule because the first read failed.
      this.logger.error("reminder reconcile failed", { error });
    }
    this.scheduleRefresh();
  }

  stop(): void {
    if (this.refreshHandle !== undefined) this.time.clearTimer(this.refreshHandle);
    this.refreshHandle = undefined;
    this.cancelAll();
  }

  /**
   * Bring the schedule in line with the table for the ranges we own: schedule
   * reminders we don't have yet, and cancel ones we no longer own or that were
   * removed. Idempotent — already-scheduled reminders keep their timers, so a
   * periodic refresh never resets a running fixed-rate reminder.
   */
  private async reconcile(): Promise<void> {
    const owned = new Map<string, ReminderEntry>();
    for (const [begin, end] of this.ranges) {
      for (const entry of await this.table.readRange(begin, end)) {
        owned.set(this.key(entry.grainId, entry.name), entry);
      }
    }
    for (const key of [...this.scheduled.keys()]) {
      if (!owned.has(key)) this.cancel(key);
    }
    for (const [key, entry] of owned) {
      if (!this.scheduled.has(key)) this.scheduleEntry(entry);
    }
  }

  private scheduleRefresh(): void {
    if (this.refreshIntervalMs <= 0) return;
    if (this.refreshHandle !== undefined) this.time.clearTimer(this.refreshHandle);
    this.refreshHandle = this.time.setTimer(() => {
      this.scheduleRefresh(); // re-arm before reconciling, so the cadence is steady
      void this.reconcile().catch((error: unknown) => {
        this.logger.error("reminder reconcile failed", { error });
      });
    }, this.refreshIntervalMs);
  }

  private owns(grainId: GrainId): boolean {
    return isHashInRanges(grainId.getUniformHashCode(), this.ranges);
  }

  private scheduleEntry(entry: ReminderEntry): void {
    const key = this.key(entry.grainId, entry.name);
    this.cancel(key);
    const dueMs = Math.max(0, entry.startAt.getTime() - this.time.now());
    this.scheduled.set(key, { entry, handle: this.time.setTimer(() => this.fire(entry), dueMs) });
  }

  private fire(entry: ReminderEntry): void {
    const key = this.key(entry.grainId, entry.name);
    if (!this.scheduled.has(key)) return;
    const periodMs = durationToMs(entry.period);
    if (periodMs > 0) {
      // Reschedule first (fixed-rate) before delivering the tick.
      this.scheduled.set(key, {
        entry,
        handle: this.time.setTimer(() => this.fire(entry), periodMs),
      });
    } else {
      // One-shot: done. Remove it from the table so a refresh can't re-fire it.
      this.scheduled.delete(key);
      void this.table.remove(entry.grainId, entry.name, entry.etag).catch((error: unknown) => {
        this.logger.error("reminder one-shot cleanup failed", {
          error,
          grainId: entry.grainId.toString(),
          name: entry.name,
        });
      });
    }
    const status: TickStatus = {
      firstTickAt: entry.startAt,
      period: entry.period,
      currentTickAt: new Date(this.time.now()),
    };
    void this.onFire(entry.grainId, entry.name, status)
      .then(() => recordReminderFired({ "tsva.reminder.name": entry.name }))
      .catch((error: unknown) => {
        recordReminderMissed({ "tsva.reminder.name": entry.name });
        this.logger.error("reminder delivery failed", {
          error,
          grainId: entry.grainId.toString(),
          name: entry.name,
        });
      });
  }

  private cancel(key: string): void {
    const scheduled = this.scheduled.get(key);
    if (scheduled === undefined) return;
    this.time.clearTimer(scheduled.handle);
    this.scheduled.delete(key);
  }

  private cancelAll(): void {
    for (const { handle } of this.scheduled.values()) this.time.clearTimer(handle);
    this.scheduled.clear();
  }

  private key(grainId: GrainId, name: string): string {
    return `${grainId.toString()} ${name}`;
  }
}
