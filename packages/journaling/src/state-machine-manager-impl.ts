import type { GrainId } from "@tsva/core/grain-id";
import type { JournalStorage } from "@tsva/core/journal-storage";
import type { DurableStateMachine, StateMachineManager } from "@tsva/core/durable-state-machine";
import { deserializeValue, serializeValue } from "@tsva/core/value-codec";

/** A framed log record: which machine, whether it is an op or a snapshot, and the payload. */
interface LogEnvelope {
  /** Owning machine name (= `stateName`). */
  m: string;
  /**
   * Frame kind: `"op"` = incremental mutation, `"snap"` = full-state snapshot,
   * `"vessel"` = buffered data kept for a currently-unregistered structure (see
   * `RetirementRecord`).
   */
  k: "op" | "snap" | "vessel";
  /** Machine-specific payload (op/snapshot) or a `VesselPayload`; opaque to callers. */
  p: unknown;
}

/** A `"vessel"` frame's payload: written and read only by the manager itself. */
interface VesselPayload {
  /** How many compactions this name has survived while unregistered. */
  count: number;
  /** Raw op/snapshot payloads applied to this name before/while unregistered. */
  ops: unknown[];
}

/**
 * In-memory bookkeeping for a structure whose name appears in the log but is
 * not registered on this activation's manager — removed from the grain across
 * a deploy. Mirrors Orleans' `RetiredStateMachineVessel` plus its retirement
 * tracker: the data is kept alive across a bounded number of compactions
 * rather than dropped at the very next one, and is resurrected in full if the
 * name is registered again before its grace period runs out.
 */
interface RetirementRecord {
  ops: unknown[];
  compactionsSinceOrphaned: number;
}

export interface StateMachineManagerOptions {
  /** Snapshot + truncate once the live log reaches this many entries (default 100). */
  snapshotThreshold?: number;
  /**
   * Number of compactions an unregistered structure's data survives before
   * being purged for good (default 2): kept through the first compaction that
   * still finds it unregistered, purged on the next. Re-registering the
   * structure before that resurrects it immediately — its buffered ops are
   * replayed onto the real machine — and, if it is ever removed again, its
   * grace period starts over.
   */
  retirementGraceCompactions?: number;
}

/**
 * Per-grain owner of one append-only log shared by all the grain's durable
 * structures. It
 * frames mutations, persists them through a `JournalStorage` under version CAS,
 * replays the log on activation, and snapshots + truncates past a threshold.
 * Within an activation everything runs on serialized turns, so no in-process
 * locking is needed; the only concurrency is a cross-silo incarnation, which the
 * storage version CAS fences out.
 */
export class StateMachineManagerImpl implements StateMachineManager {
  private readonly machines = new Map<string, DurableStateMachine>();
  private readonly snapshotThreshold: number;
  private readonly retirementGraceCompactions: number;
  private readonly retiring = new Map<string, RetirementRecord>();
  private version: number | undefined;
  private liveEntryCount = 0;

  constructor(
    private readonly logName: string,
    private readonly grainId: GrainId,
    private readonly storage: JournalStorage,
    options: StateMachineManagerOptions = {},
  ) {
    this.snapshotThreshold = options.snapshotThreshold ?? 100;
    this.retirementGraceCompactions = options.retirementGraceCompactions ?? 2;
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
    this.retiring.clear();
    for (const machine of this.machines.values()) machine.reset();
    for (const raw of segment.entries) {
      const env = deserializeValue<LogEnvelope>(raw);
      const machine = this.machines.get(env.m);
      if (machine !== undefined) {
        // Registered this activation: a vessel frame unwraps into the buffered
        // ops it holds (resurrection), applied in original order.
        if (env.k === "vessel") {
          for (const op of (env.p as VesselPayload).ops) machine.apply(op);
        } else {
          machine.apply(env.p);
        }
        continue;
      }
      // Unregistered this activation: buffer rather than drop, so a bounded
      // grace period can still save it if it comes back.
      if (env.k === "vessel") {
        const payload = env.p as VesselPayload;
        this.retiring.set(env.m, {
          ops: [...payload.ops],
          compactionsSinceOrphaned: payload.count,
        });
      } else {
        const record = this.retiring.get(env.m) ?? { ops: [], compactionsSinceOrphaned: 0 };
        record.ops.push(env.p);
        this.retiring.set(env.m, record);
      }
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
    // Keep the threshold above the machine count so we don't recompact on every append.
    if (this.liveEntryCount >= Math.max(this.snapshotThreshold, this.machines.size + 1))
      await this.compact();
  }

  async compact(): Promise<void> {
    const frames = [...this.machines.values()].map((machine) =>
      serializeValue({ m: machine.name, k: "snap", p: machine.snapshot() } satisfies LogEnvelope),
    );
    for (const [name, record] of this.retiring) {
      const count = record.compactionsSinceOrphaned + 1;
      if (count >= this.retirementGraceCompactions) {
        // Grace period elapsed while still unregistered: purge for good by
        // simply not writing a frame for it.
        this.retiring.delete(name);
        continue;
      }
      record.compactionsSinceOrphaned = count;
      frames.push(
        serializeValue({
          m: name,
          k: "vessel",
          p: { count, ops: record.ops } satisfies VesselPayload,
        } satisfies LogEnvelope),
      );
    }
    this.version = await this.storage.replace(this.logName, this.grainId, frames, this.version);
    this.liveEntryCount = frames.length;
  }
}
