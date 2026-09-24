import { durationToMs, type Duration } from "@thresh/core/duration";
import type { GrainId } from "@thresh/core/grain-id";
import { isHashInRanges, type HashRange } from "@thresh/core/hash-ring";
import { noopLogger, type Logger } from "@thresh/core/logger";
import { recordReminderFired, recordReminderMissed } from "@thresh/observability/reminder-metrics";
import type {
  ReminderEntry,
  ReminderRegistry,
  ReminderTable,
  TickStatus,
} from "@thresh/core/reminder";
import type { TimeProvider, TimerHandle } from "@thresh/core/time-provider";

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
  /**
   * The local sequence number at which this schedule was made (Orleans
   * `LocalReminderData.LocalSequenceNumber`): a reconcile whose table read
   * began before it must not replace or cancel it.
   */
  sequence: number;
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
  /** Orleans `localTableSequence`: bumped at each reconcile read and each local (re)schedule. */
  private localSequence = 0;

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
    // A fresh local registration keeps its first tick at `startAt` even if the
    // table write took long enough that `startAt` is now a few ms past (e.g. a
    // zero due time): no tick was missed, so the grid skip doesn't apply.
    if (this.owns(grainId)) this.scheduleEntry({ grainId, name, startAt, period, etag }, startAt);
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
   * reminders we don't have yet, replace ones whose table etag has moved on
   * (an update from a silo that isn't the owner — issue: reconcile leaves the
   * owner on the old schedule), and cancel ones we no longer own or that were
   * removed. Idempotent for unchanged entries — already-scheduled reminders
   * with a matching etag keep their timers, so a periodic refresh never
   * resets a running fixed-rate reminder. Mirrors Orleans'
   * `ReadTableAndStartTimers`, which replaces the local reminder whenever the
   * table's etag differs from the one it has.
   */
  private async reconcile(): Promise<void> {
    // Orleans' cachedSequence: anything scheduled locally after this point is
    // newer than what the read below can return, so the read must not
    // replace or cancel it.
    const readSequence = ++this.localSequence;
    const owned = new Map<string, ReminderEntry>();
    for (const [begin, end] of this.ranges) {
      for (const entry of await this.table.readRange(begin, end)) {
        owned.set(this.key(entry.grainId, entry.name), entry);
      }
    }
    for (const [key, current] of [...this.scheduled]) {
      if (!owned.has(key) && current.sequence < readSequence) this.cancel(key);
    }
    for (const [key, entry] of owned) {
      const current = this.scheduled.get(key);
      if (current === undefined) {
        this.scheduleEntry(entry);
      } else if (current.entry.etag !== entry.etag && current.sequence < readSequence) {
        this.scheduleEntry(entry);
      }
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

  /**
   * The next due instant for `entry`. Mirrors Orleans' `CalculateInitialDueTime`:
   * if `startAt` (or the last recorded tick) hasn't passed yet, that's the due
   * time; otherwise ticks have been missed and we skip straight to the next
   * `startAt + n*period` grid boundary rather than firing a catch-up tick.
   * Used both for the very first schedule (a reminder registered, or
   * reconciled onto a new owner, with a long-past `startAt` doesn't fire
   * immediately — issue: reminder fires a catch-up tick after downtime) and
   * after every fire (so ticks stay on the grid instead of drifting by
   * fire-time + period — issue: reminder double-fire on rebalance).
   */
  private nextDueAt(entry: ReminderEntry): Date {
    const periodMs = durationToMs(entry.period);
    if (periodMs <= 0) return entry.startAt; // one-shot: never reconciled/rescheduled after firing
    const startMs = entry.startAt.getTime();
    // Like CalculateInitialDueTime, measure from *now*: the first grid
    // boundary at or after now (a boundary exactly at now is due now). A
    // recorded tick only ever pushes that later — never to a boundary that is
    // already past — so a new owner neither re-fires the tick the old owner
    // just delivered nor fires a catch-up tick for a long-past lastFiredAt.
    let periods = Math.max(0, Math.ceil((this.time.now() - startMs) / periodMs));
    if (entry.lastFiredAt !== undefined) {
      const firedPeriods = Math.floor((entry.lastFiredAt.getTime() - startMs) / periodMs) + 1;
      periods = Math.max(periods, firedPeriods);
    }
    return new Date(startMs + periods * periodMs);
  }

  private scheduleEntry(entry: ReminderEntry, firstDueAt: Date = this.nextDueAt(entry)): void {
    const key = this.key(entry.grainId, entry.name);
    this.cancel(key);
    const dueMs = Math.max(0, firstDueAt.getTime() - this.time.now());
    this.scheduled.set(key, {
      entry,
      handle: this.time.setTimer(() => this.fire(entry), dueMs),
      sequence: ++this.localSequence,
    });
  }

  private fire(entry: ReminderEntry): void {
    const key = this.key(entry.grainId, entry.name);
    const current = this.scheduled.get(key);
    if (current === undefined) return;
    const periodMs = durationToMs(entry.period);
    const firedAt = new Date(this.time.now());
    if (periodMs > 0) {
      // Reschedule first (fixed-rate) before delivering the tick. Persist the
      // tick instant so a future owner (rebalance/restart) resumes from it
      // rather than refiring from the original startAt. Schedule the next tick
      // from the startAt + n*period grid (nextDueAt), not fire-time + period,
      // so a late tick doesn't drag every later tick's schedule with it.
      const tickedEntry: ReminderEntry = { ...entry, lastFiredAt: firedAt };
      const nextDueMs = Math.max(0, this.nextDueAt(tickedEntry).getTime() - this.time.now());
      this.scheduled.set(key, {
        entry: tickedEntry,
        handle: this.time.setTimer(() => this.fire(tickedEntry), nextDueMs),
        sequence: current.sequence, // same registration, just its next tick
      });
      void this.table
        .recordFired(entry.grainId, entry.name, entry.etag, firedAt)
        .catch((error: unknown) => {
          this.logger.error("reminder recordFired failed", {
            error,
            grainId: entry.grainId.toString(),
            name: entry.name,
          });
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
      .then(() => recordReminderFired({ "thresh.reminder.name": entry.name }))
      .catch((error: unknown) => {
        recordReminderMissed({ "thresh.reminder.name": entry.name });
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
