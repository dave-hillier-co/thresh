import { InconsistentStateError } from "@thresh/core/errors";
import type { GrainId } from "@thresh/core/grain-id";
import { backingObjectId } from "@thresh/core/journal-storage";
import type { JournalEntry, JournalSegment, JournalStorage } from "@thresh/core/journal-storage";

interface Log {
  entries: JournalEntry[];
  version: number;
}

/**
 * In-memory journal store for development and tests. Not durable and not shared
 * across silos; mirrors `MemoryGrainStorage`. The `version` is a per-log op
 * counter, giving the same optimistic-concurrency behaviour as the Redis
 * provider (a stale writer loses the CAS and raises `InconsistentStateError`).
 */
export class MemoryJournalStorage implements JournalStorage {
  readonly backingKey: string;
  private readonly logs: Map<string, Log>;

  /**
   * `sharing` makes this a second provider over the SAME backing store as another
   * `MemoryJournalStorage`: a distinct provider object reading and writing the same logs, as two
   * named Postgres providers over one pool and tables do.
   */
  constructor(options: { sharing?: MemoryJournalStorage } = {}) {
    this.logs = options.sharing?.logs ?? new Map<string, Log>();
    this.backingKey = `memory:${backingObjectId(this.logs)}`;
  }

  async read(logName: string, grainId: GrainId): Promise<JournalSegment> {
    const log = this.logs.get(this.key(logName, grainId));
    if (log === undefined) return { entries: [], version: undefined };
    return { entries: [...log.entries], version: log.version };
  }

  async append(
    logName: string,
    grainId: GrainId,
    entries: readonly JournalEntry[],
    expectedVersion: number | undefined,
  ): Promise<number> {
    const key = this.key(logName, grainId);
    const log = this.logs.get(key);
    this.checkVersion(logName, grainId, log?.version, expectedVersion);
    const next = log ?? { entries: [], version: 0 };
    next.entries.push(...entries);
    next.version += 1;
    this.logs.set(key, next);
    return next.version;
  }

  async replace(
    logName: string,
    grainId: GrainId,
    entries: readonly JournalEntry[],
    expectedVersion: number | undefined,
  ): Promise<number> {
    const key = this.key(logName, grainId);
    const log = this.logs.get(key);
    this.checkVersion(logName, grainId, log?.version, expectedVersion);
    const version = (log?.version ?? 0) + 1;
    this.logs.set(key, { entries: [...entries], version });
    return version;
  }

  async clear(logName: string, grainId: GrainId): Promise<void> {
    this.logs.delete(this.key(logName, grainId));
  }

  private checkVersion(
    logName: string,
    grainId: GrainId,
    stored: number | undefined,
    expected: number | undefined,
  ): void {
    if (stored !== expected) {
      throw new InconsistentStateError(
        `journal version conflict on ${logName} for ${grainId.toString()}`,
        expected === undefined ? undefined : String(expected),
        stored === undefined ? undefined : String(stored),
      );
    }
  }

  private key(logName: string, grainId: GrainId): string {
    return `${grainId.toString()}/${logName}`;
  }
}
