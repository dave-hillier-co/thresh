// Ported from dotnet/orleans src/Orleans.Core/Utils/ObserverManager.cs @ v10.1.0 (MIT).
import type { Duration } from "./duration";
import { durationToMs } from "./duration";
import type { TimeProvider } from "./time-provider";

interface ObserverEntry<TObserver> {
  observer: TObserver;
  lastSeenMs: number;
}

/**
 * A read-only view over the observer entries at a point in time, held open
 * while the manager iterates it. Mirrors Orleans' `ReadSnapshot`/`_numReaders`
 * mechanism: while any snapshot is outstanding, a write must copy the backing
 * map rather than mutate it in place, so in-flight iteration never observes a
 * torn or concurrently-mutated collection. `release()` must be called exactly
 * once, in a `finally`, to balance the reader count.
 */
class ReadSnapshot<TId, TObserver> {
  constructor(
    private readonly manager: ObserverManager<TId, TObserver>,
    readonly entries: ReadonlyMap<TId, ObserverEntry<TObserver>>,
  ) {}

  release(): void {
    this.manager.releaseReader();
  }
}

/**
 * Maintains a snapshot-based, time-expiring collection of subscribers.
 * Reusable by anything that needs pub/sub-style fan-out with lazy,
 * TTL-based expiry (e.g. grain observers, client callbacks).
 *
 * Notifications and enumeration operate over a snapshot taken at the start of
 * the call, so subscribing/unsubscribing during notification or enumeration
 * is safe and visible immediately via `observers`/`count`, but does not affect
 * the in-flight notification/enumeration.
 */
export class ObserverManager<TId, TObserver> implements Iterable<TObserver> {
  private entries = new Map<TId, ObserverEntry<TObserver>>();
  private numReaders = 0;
  private readSnapshotEntries: Map<TId, ObserverEntry<TObserver>> | undefined;

  /** Mutable: a new value takes effect for subsequent notify/clearExpired calls. */
  expirationDuration: Duration;

  constructor(
    expirationDuration: Duration,
    private readonly clock: TimeProvider,
  ) {
    this.expirationDuration = expirationDuration;
  }

  get count(): number {
    return this.entries.size;
  }

  /** A copy of the current id -> observer mapping. */
  get observers(): ReadonlyMap<TId, TObserver> {
    const result = new Map<TId, TObserver>();
    for (const [id, entry] of this.entries) {
      result.set(id, entry.observer);
    }
    return result;
  }

  /** Adds `observer`, or renews its subscription and replaces its value if `id` is already subscribed. */
  subscribe(id: TId, observer: TObserver): void {
    const entries = this.getWritableEntries();
    const now = this.clock.now();
    const existing = entries.get(id);
    if (existing !== undefined) {
      existing.lastSeenMs = now;
      existing.observer = observer;
    } else {
      entries.set(id, { observer, lastSeenMs: now });
    }
  }

  unsubscribe(id: TId): void {
    const entries = this.getWritableEntries();
    entries.delete(id);
  }

  /** Removes all observers. */
  clear(): void {
    const entries = this.getWritableEntries();
    entries.clear();
  }

  /** Removes all observers whose subscription has expired. */
  clearExpired(): void {
    const now = this.clock.now();
    const defunct: TId[] = [];
    const snapshot = this.createReadSnapshot();
    try {
      for (const [id, entry] of snapshot.entries) {
        if (this.isExpired(entry, now)) {
          defunct.push(id);
        }
      }
    } finally {
      snapshot.release();
    }
    this.removeDefunct(defunct);
  }

  /**
   * Notifies observers matching `predicate` (all, if omitted) with an
   * async `notification`. Observers already expired at call time are skipped
   * and marked defunct; observers whose notification throws (rejects) are
   * likewise marked defunct. All defunct observers are removed once the
   * snapshot has been fully walked.
   */
  async notify(
    notification: (observer: TObserver) => Promise<void>,
    predicate?: (observer: TObserver) => boolean,
  ): Promise<void> {
    const now = this.clock.now();
    const defunct: TId[] = [];
    const snapshot = this.createReadSnapshot();
    try {
      for (const [id, entry] of snapshot.entries) {
        if (this.isExpired(entry, now)) {
          defunct.push(id);
          continue;
        }
        if (predicate !== undefined && !predicate(entry.observer)) {
          continue;
        }
        try {
          await notification(entry.observer);
        } catch {
          defunct.push(id);
        }
      }
    } finally {
      snapshot.release();
    }
    this.removeDefunct(defunct);
  }

  /** Synchronous form of {@link notify}, for callbacks that don't return a promise. */
  notifySync(
    notification: (observer: TObserver) => void,
    predicate?: (observer: TObserver) => boolean,
  ): void {
    const now = this.clock.now();
    const defunct: TId[] = [];
    const snapshot = this.createReadSnapshot();
    try {
      for (const [id, entry] of snapshot.entries) {
        if (this.isExpired(entry, now)) {
          defunct.push(id);
          continue;
        }
        if (predicate !== undefined && !predicate(entry.observer)) {
          continue;
        }
        try {
          notification(entry.observer);
        } catch {
          defunct.push(id);
        }
      }
    } finally {
      snapshot.release();
    }
    this.removeDefunct(defunct);
  }

  [Symbol.iterator](): Iterator<TObserver> {
    const snapshot = this.createReadSnapshot();
    const inner = snapshot.entries.values();
    let released = false;
    const release = () => {
      if (!released) {
        released = true;
        snapshot.release();
      }
    };
    return {
      next: (): IteratorResult<TObserver> => {
        const step = inner.next();
        if (step.done) {
          release();
          return { done: true, value: undefined };
        }
        return { done: false, value: step.value.observer };
      },
      return: (value?: unknown): IteratorResult<TObserver> => {
        release();
        return { done: true, value: value as TObserver };
      },
    };
  }

  /** @internal balances a reader started by {@link createReadSnapshot}. */
  releaseReader(): void {
    this.numReaders--;
    if (this.numReaders === 0) {
      this.readSnapshotEntries = undefined;
    }
  }

  private isExpired(entry: ObserverEntry<TObserver>, now: number): boolean {
    return entry.lastSeenMs + durationToMs(this.expirationDuration) < now;
  }

  private removeDefunct(defunct: readonly TId[]): void {
    if (defunct.length === 0) {
      return;
    }
    const entries = this.getWritableEntries();
    for (const id of defunct) {
      entries.delete(id);
    }
  }

  private createReadSnapshot(): ReadSnapshot<TId, TObserver> {
    this.numReaders++;
    this.readSnapshotEntries = this.entries;
    return new ReadSnapshot(this, this.entries);
  }

  /**
   * Returns the backing map for mutation, copying it first if a read
   * snapshot is currently outstanding over it (copy-on-write).
   */
  private getWritableEntries(): Map<TId, ObserverEntry<TObserver>> {
    if (this.numReaders > 0 && this.entries === this.readSnapshotEntries) {
      this.entries = new Map(this.entries);
    }
    return this.entries;
  }
}
