import {
  TransactionAbortedError,
  TransactionInDoubtError,
  TransactionOrphanCallError,
} from "@thresh/core/errors";
import { Guid } from "@thresh/core/guid";
import type {
  EnlistedParticipant,
  ParticipantId,
  TransactionInfo,
  TransactionManager,
} from "@thresh/core/transaction-info";
import { isTransactionManager } from "@thresh/core/transaction-info";
import { TransactionResourceInterface } from "@thresh/core/transaction-resource";
import { CausalClock } from "@thresh/runtime/causal-clock";
import type { Dispatcher } from "@thresh/runtime/dispatcher";
import type { TimeProvider } from "@thresh/runtime/time-provider";

/**
 * The participant elected to manage the transaction. `local` carries its live
 * object when it is enlisted on this silo (the agent drives the manager methods
 * directly); absent when the manager was merged back from another silo and is
 * reached over the dispatcher instead.
 */
interface ElectedManager {
  readonly id: ParticipantId;
  readonly local?: TransactionManager | undefined;
}

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
      status: "active",
      pendingCalls: 0,
    };
  }

  /** Commit the transaction across its participants, or abort all and throw. */
  async resolve(info: TransactionInfo): Promise<void> {
    // Close the participant set *before* the snapshot below, not after the
    // round: preparing and committing await durable writes, so there is a
    // window in which a detached caller — a `oneWay` `supported` call whose
    // originator resolved the moment its call returned — is still running and
    // would otherwise enlist into the set this round is already working from.
    // Such a resource would never be prepared, committed or aborted, and the
    // lock it took would be held until deactivation; `requireTransaction`
    // refuses it instead (`TransactionAlreadyResolvedError`).
    info.status = "resolving";
    // Orleans `TransactionInfo.MustAbort`: a call forked off this transaction
    // (see `forkTransaction`, `@thresh/core/transaction-info`) that never
    // completed leaves the transaction's true read/write set unknowable, so it
    // may not commit — abort instead, even if it enlisted no participants at
    // all (an orphan fork with no other work still must not silently "succeed").
    if (info.pendingCalls !== 0) {
      await this.abort(info);
      throw new TransactionOrphanCallError(info.id, info.pendingCalls);
    }
    const enlisted = [...info.participants.values()];
    if (enlisted.length === 0) {
      info.status = "committed";
      return;
    }

    // Elect the transaction manager from the writers (Orleans: the first write
    // participant that supports the Manager role). In-process the agent
    // coordinates the rounds directly.
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
      // and recovery resolves them to commit by querying the TM. A transaction
      // whose writers are all resource-only has no such point to record: its
      // participants still commit (the port's `TransactionCommitter` is
      // memory-only, with no durable prepare record to recover either), but
      // nothing is left claiming a commit record exists.
      if (manager !== undefined) {
        try {
          await this.recordCommit(manager, info, writeParticipants);
        } catch (error) {
          // We cannot tell whether the record actually persisted before this
          // failed (e.g. the write landed but the ack was lost) — so this is
          // exactly the "unknown" case `TransactionInDoubtError` exists for,
          // not an abort: every participant here is already prepared, and
          // aborting them now could contradict a commit that DID land.
          // Recovery resolves the truth later by querying the TM directly.
          throw new TransactionInDoubtError(info.id, { cause: error });
        }
      }
      // The commit is now decided (the TM has recorded it, or there was no
      // writer to record it) — participants only have to apply it.
      info.status = "committed";
      // Past this point the commit is durably decided: every participant's
      // write *will* eventually apply. A participant whose own commit step
      // throws here (e.g. an external, non-grain resource enlisted via
      // `TransactionParticipant.commit`, Orleans `ITransactionalResource`)
      // does not undo anyone else's already-applied commit — it only leaves
      // the caller unable to confirm this call itself finished applying
      // everywhere, hence the distinct, non-abort `TransactionInDoubtError`
      // (Orleans `OrleansTransactionInDoubtException`) rather than the
      // generic `TransactionAbortedError`.
      const results = await Promise.allSettled(enlisted.map((e) => this.commit(e, info)));
      const failed = results.find((r): r is PromiseRejectedResult => r.status === "rejected");
      if (failed !== undefined) {
        throw new TransactionInDoubtError(info.id, { cause: failed.reason });
      }
      return;
    }
    await this.abort(info);
    throw new TransactionAbortedError(info.id, "a participant failed to prepare");
  }

  /** Abort: discard every enlisted participant's tentative writes and release locks. */
  async abort(info: TransactionInfo): Promise<void> {
    // Closed before the round runs, for the same reason as `resolve`: an abort
    // releases the locks held for the participants it knows about, so a
    // resource that slipped in behind the snapshot would keep its own.
    info.status = "aborted";
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
    manager: ElectedManager,
    info: TransactionInfo,
    writeParticipants: ParticipantId[],
  ): Promise<void> {
    if (manager.local !== undefined) {
      await manager.local.recordCommit(info.id, info.timeStamp, writeParticipants);
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

  /**
   * The elected manager (Orleans' first write participant that supports the
   * Manager role), or undefined when no writer can manage the transaction.
   *
   * A locally-enlisted participant must implement the manager half of the
   * contract to be elected: electing one that cannot record a commit — a
   * `TransactionCommitter`, say — would leave the transaction with no durable
   * commit point at all, and `recordCommit` would route the recovery `status`
   * query to a resource that has no answer for it. A participant merged back
   * from another silo carries no role of its own (the reply's
   * `SerializedParticipant` is just `{id, access}`), so it is taken at its
   * word: it is reached through `TransactionResource`, whose contract covers
   * the manager methods too.
   */
  private electManager(enlisted: readonly EnlistedParticipant[]): ElectedManager | undefined {
    for (const e of enlisted) {
      if (e.access.writes === 0) continue;
      const local = e.participant;
      if (local === undefined) return { id: e.id };
      if (isTransactionManager(local)) return { id: e.id, local };
    }
    return undefined;
  }
}
