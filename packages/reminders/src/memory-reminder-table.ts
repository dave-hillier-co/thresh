import type { GrainId } from "@thresh/core/grain-id";
import { isHashInRange } from "@thresh/core/hash-ring";
import type { ReminderEntry, ReminderRegistration, ReminderTable } from "@thresh/core/reminder";

/** In-memory reminder table for development and tests; not durable. */
export class MemoryReminderTable implements ReminderTable {
  private readonly entries = new Map<string, ReminderEntry>();
  private etagCounter = 0;

  async upsert(registration: ReminderRegistration): Promise<string> {
    const etag = String(++this.etagCounter);
    this.entries.set(this.key(registration.grainId, registration.name), { ...registration, etag });
    return etag;
  }

  async remove(grainId: GrainId, name: string, etag: string): Promise<boolean> {
    const key = this.key(grainId, name);
    const existing = this.entries.get(key);
    if (existing === undefined || existing.etag !== etag) return false;
    this.entries.delete(key);
    return true;
  }

  async read(grainId: GrainId, name: string): Promise<ReminderEntry | undefined> {
    return this.entries.get(this.key(grainId, name));
  }

  async readForGrain(grainId: GrainId): Promise<ReminderEntry[]> {
    return [...this.entries.values()].filter((e) => e.grainId.equals(grainId));
  }

  async readRange(hashBegin: number, hashEnd: number): Promise<ReminderEntry[]> {
    return [...this.entries.values()].filter((e) =>
      isHashInRange(e.grainId.getUniformHashCode(), hashBegin, hashEnd),
    );
  }

  async recordFired(
    grainId: GrainId,
    name: string,
    etag: string,
    firedAt: Date,
  ): Promise<string | undefined> {
    const key = this.key(grainId, name);
    const existing = this.entries.get(key);
    if (existing === undefined || existing.etag !== etag) return undefined;
    this.entries.set(key, { ...existing, lastFiredAt: firedAt });
    return etag;
  }

  private key(grainId: GrainId, name: string): string {
    return `${grainId.toString()}${name}`;
  }
}
