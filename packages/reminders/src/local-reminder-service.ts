import { durationToMs, type Duration } from "@tsva/core/duration";
import type { GrainId } from "@tsva/core/grain-id";
import type {
  ReminderEntry,
  ReminderRegistry,
  ReminderTable,
  TickStatus,
} from "@tsva/core/reminder";
import type { TimeProvider, TimerHandle } from "@tsva/core/time-provider";

/** Delivers a due reminder: the silo wires this to reactivate the grain and call `receiveReminder`. */
export type ReminderFire = (grainId: GrainId, name: string, status: TickStatus) => Promise<void>;

/** A half-open hash range `[begin, end)` this silo owns on the ring. */
export type HashRange = readonly [begin: number, end: number];

interface Scheduled {
  handle: TimerHandle;
  entry: ReminderEntry;
}

function inRanges(hash: number, ranges: readonly HashRange[]): boolean {
  return ranges.some(([begin, end]) =>
    begin <= end ? hash >= begin && hash < end : hash >= begin || hash < end,
  );
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

  constructor(
    private readonly table: ReminderTable,
    private readonly time: TimeProvider,
    private readonly onFire: ReminderFire,
    ranges: readonly HashRange[] = [[0, 0x1_0000_0000]],
    /** How often to re-read owned ranges from the table (0 disables periodic refresh). */
    private readonly refreshIntervalMs = 0,
  ) {
    this.ranges = ranges;
  }

  /** Register (or update) a reminder; schedules it locally if this silo owns it. */
  async register(grainId: GrainId, name: string, due: Duration, period: Duration): Promise<void> {
    const startAt = new Date(this.time.now() + durationToMs(due));
    const etag = await this.table.upsert({ grainId, name, startAt, period });
    if (this.owns(grainId)) this.scheduleEntry({ grainId, name, startAt, period, etag });
  }

  async unregister(grainId: GrainId, name: string): Promise<void> {
    const entry = await this.table.read(grainId, name);
    if (entry !== undefined) await this.table.remove(grainId, name, entry.etag);
    this.cancel(this.key(grainId, name));
  }

  /** On a membership change (or first start): adopt the given ranges and reconcile. */
  async refreshOwnership(ranges: readonly HashRange[]): Promise<void> {
    this.ranges = ranges;
    await this.reconcile();
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
      void this.reconcile().catch(() => undefined);
    }, this.refreshIntervalMs);
  }

  private owns(grainId: GrainId): boolean {
    return inRanges(grainId.getUniformHashCode(), this.ranges);
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
      void this.table.remove(entry.grainId, entry.name, entry.etag).catch(() => undefined);
    }
    const status: TickStatus = {
      firstTickAt: entry.startAt,
      period: entry.period,
      currentTickAt: new Date(this.time.now()),
    };
    void this.onFire(entry.grainId, entry.name, status).catch(() => undefined);
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
