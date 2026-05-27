import type { GrainId } from "@tsva/core/grain-id";
import type { JournalStorage } from "@tsva/core/journal-storage";
import type { DurableStateMachine, StateMachineManager } from "@tsva/core/durable-state-machine";
import { deserializeValue, serializeValue } from "@tsva/core/value-codec";

/** A framed log record: which machine, whether it is an op or a snapshot, and the payload. */
interface LogEnvelope {
  /** Owning machine name (= `stateName`). */
  m: string;
  /** Frame kind: `"op"` = incremental mutation, `"snap"` = full-state snapshot. */
  k: "op" | "snap";
  /** Machine-specific payload (op or snapshot), opaque to the manager. */
  p: unknown;
}

export interface StateMachineManagerOptions {
  /** Snapshot + truncate once the live log reaches this many entries (default 100). */
  snapshotThreshold?: number;
}

/**
 * Per-grain owner of one append-only log shared by all the grain's durable
 * structures (see [ADR 0019](../../docs/adr/0019-durable-journaling.md)). It
 * frames mutations, persists them through a `JournalStorage` under version CAS,
 * replays the log on activation, and snapshots + truncates past a threshold.
 * Within an activation everything runs on serialized turns, so no in-process
 * locking is needed; the only concurrency is a cross-silo incarnation, which the
 * storage version CAS fences out.
 */
export class StateMachineManagerImpl implements StateMachineManager {
  private readonly machines = new Map<string, DurableStateMachine>();
  private readonly snapshotThreshold: number;
  private version: number | undefined;
  private liveEntryCount = 0;

  constructor(
    private readonly logName: string,
    private readonly grainId: GrainId,
    private readonly storage: JournalStorage,
    options: StateMachineManagerOptions = {},
  ) {
    this.snapshotThreshold = options.snapshotThreshold ?? 100;
  }

  register(machine: DurableStateMachine): void {
    if (this.machines.has(machine.name)) {
      throw new Error(
        `duplicate durable state machine "${machine.name}" on ${this.grainId.toString()}`,
      );
    }
    this.machines.set(machine.name, machine);
  }

  async replay(): Promise<void> {
    const segment = await this.storage.read(this.logName, this.grainId);
    this.version = segment.version;
    this.liveEntryCount = segment.entries.length;
    for (const machine of this.machines.values()) machine.reset();
    for (const entry of segment.entries) {
      const env = deserializeValue<LogEnvelope>(entry);
      const machine = this.machines.get(env.m);
      // A structure removed from the grain across deploys: drop its orphan
      // entries (they vanish at the next compaction) rather than crash activation.
      if (machine === undefined) continue;
      machine.apply(env.p);
    }
  }

  async append(machineName: string, payload: unknown): Promise<void> {
    const machine = this.machines.get(machineName);
    if (machine === undefined) throw new Error(`unknown durable state machine "${machineName}"`);
    const entry = serializeValue({ m: machineName, k: "op", p: payload } satisfies LogEnvelope);
    // Persist first; only on success apply to memory (a conflicting write leaves
    // the structure untouched and fails the call). The manager owns the apply so
    // that an in-turn compaction below snapshots up-to-date in-memory state.
    this.version = await this.storage.append(this.logName, this.grainId, [entry], this.version);
    machine.apply(payload);
    this.liveEntryCount += 1;
    if (this.liveEntryCount >= this.threshold()) await this.compact();
  }

  async compact(): Promise<void> {
    const frames = [...this.machines.values()].map((machine) =>
      serializeValue({ m: machine.name, k: "snap", p: machine.snapshot() } satisfies LogEnvelope),
    );
    this.version = await this.storage.replace(this.logName, this.grainId, frames, this.version);
    this.liveEntryCount = frames.length;
  }

  /** Keep the threshold above the machine count so we don't recompact on every append. */
  private threshold(): number {
    return Math.max(this.snapshotThreshold, this.machines.size + 1);
  }
}
