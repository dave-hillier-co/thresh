import { TransactionAbortedError, TransactionOrphanCallError } from "@tsva/core/errors";
import { Guid } from "@tsva/core/guid";
import type {
  EnlistedParticipant,
  ParticipantId,
  TransactionInfo,
} from "@tsva/core/transaction-info";
import { TransactionResourceInterface } from "@tsva/core/transaction-resource";
import { CausalClock } from "@tsva/runtime/causal-clock";
import type { Dispatcher } from "@tsva/runtime/dispatcher";
import type { TimeProvider } from "@tsva/runtime/time-provider";

/**
 * The per-silo transaction agent (Orleans `TransactionAgent`). It begins a
 * transaction on behalf of the originating call — assigning a unique id and a
 * causal-clock timestamp — and resolves it at the boundary with an optimistic,
 * serializable commit.
 *
 * Resolve runs the faithful two-phase protocol: elect the transaction manager
 * from the writers, **prepare** every participant (each validates its lock and
 * stages its tentative state), and then **commit** them all — or, if any prepare
 * vetoes, **abort** them all (cascading abort) and raise
 * {@link TransactionAbortedError} so the caller may retry. A participant on this
 * silo is driven directly; one merged back from another silo is reached over the
 * dispatcher via the `TransactionResource` system extension.
 */
export class TransactionAgent {
  private readonly clock: CausalClock;
  private dispatcher: Dispatcher | undefined;

  constructor(time: TimeProvider) {
    this.clock = new CausalClock(time);
  }

  setDispatcher(dispatcher: Dispatcher): void {
    this.dispatcher = dispatcher;
  }

  startTransaction(readOnly = false): TransactionInfo {
    return {
      id: Guid.newGuid().toString(),
      timeStamp: this.clock.utcNow(),
      readOnly,
      participants: new Map(),
      pendingCalls: 0,
    };
  }

  /** Commit the transaction across its participants, or abort all and throw. */
  async resolve(info: TransactionInfo): Promise<void> {
    // Orleans `TransactionInfo.MustAbort`: a call forked off this transaction
    // (see `forkTransaction`, `@tsva/core/transaction-info`) that never
    // completed leaves the transaction's true read/write set unknowable, so it
    // may not commit — abort instead, even if it enlisted no participants at
    // all (an orphan fork with no other work still must not silently "succeed").
    if (info.pendingCalls !== 0) {
      await this.abort(info);
      throw new TransactionOrphanCallError(info.id, info.pendingCalls);
    }
    const enlisted = [...info.participants.values()];
    if (enlisted.length === 0) return;

    // Elect the transaction manager from the writers (Orleans: the first write
    // participant). In-process the agent coordinates the rounds directly.
    const manager = this.electManager(enlisted);
    const writeParticipants = enlisted.filter((e) => e.access.writes > 0).map((e) => e.id);

    const prepared = await Promise.all(
      enlisted.map(async (e) => {
        try {
          return await this.prepare(e, info, manager?.id ?? e.id);
        } catch {
          return false;
        }
      }),
    );

    if (prepared.every((ok) => ok)) {
      // The TM durably records the commit before any participant commits — the
      // atomic commit point. A crash after this leaves participants in-doubt,
      // and recovery resolves them to commit by querying the TM.
      if (manager !== undefined) await this.recordCommit(manager, info, writeParticipants);
      await Promise.all(enlisted.map((e) => this.commit(e, info)));
      return;
    }
    await this.abort(info);
    throw new TransactionAbortedError(info.id, "a participant failed to prepare");
  }

  /** Abort: discard every enlisted participant's tentative writes and release locks. */
  async abort(info: TransactionInfo): Promise<void> {
    await Promise.all([...info.participants.values()].map((e) => this.abortOne(e, info)));
  }

  private prepare(
    e: EnlistedParticipant,
    info: TransactionInfo,
    manager: ParticipantId,
  ): Promise<boolean> {
    if (e.participant !== undefined) {
      return Promise.resolve(e.participant.prepare(info.id, info.timeStamp, manager));
    }
    return this.route(e.id, "prepare", [
      e.id.stateName,
      info.id,
      info.timeStamp,
      manager,
    ]) as Promise<boolean>;
  }

  /** Have the elected TM durably record the commit (locally or over the dispatcher). */
  private async recordCommit(
    manager: EnlistedParticipant,
    info: TransactionInfo,
    writeParticipants: ParticipantId[],
  ): Promise<void> {
    if (manager.participant !== undefined) {
      await manager.participant.recordCommit(info.id, info.timeStamp, writeParticipants);
      return;
    }
    await this.route(manager.id, "recordCommit", [
      manager.id.stateName,
      info.id,
      info.timeStamp,
      writeParticipants,
    ]);
  }

  private async commit(e: EnlistedParticipant, info: TransactionInfo): Promise<void> {
    if (e.participant !== undefined) {
      await e.participant.commit(info.id);
      return;
    }
    await this.route(e.id, "commit", [e.id.stateName, info.id]);
  }

  private async abortOne(e: EnlistedParticipant, info: TransactionInfo): Promise<void> {
    if (e.participant !== undefined) {
      await e.participant.abort(info.id);
      return;
    }
    await this.route(e.id, "abort", [e.id.stateName, info.id]);
  }

  private route(id: ParticipantId, method: string, args: unknown[]): Promise<unknown> {
    if (this.dispatcher === undefined) throw new Error("transaction agent has no dispatcher");
    return this.dispatcher.invoke({
      target: id.grainId,
      interfaceId: TransactionResourceInterface.id,
      method,
      args,
      options: {},
      reentrancyId: Guid.newGuid().toString(),
    });
  }

  /** The elected manager (first writer), or undefined for a read-only transaction. */
  private electManager(enlisted: readonly EnlistedParticipant[]): EnlistedParticipant | undefined {
    return enlisted.find((e) => e.access.writes > 0);
  }
}
