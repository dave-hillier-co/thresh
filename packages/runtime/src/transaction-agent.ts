import {
  GrainCallTimeoutError,
  TransactionAbortedError,
  TransactionBrokenLockError,
  TransactionCascadingAbortError,
  TransactionInDoubtError,
  TransactionOrphanCallError,
} from "@thresh/core/errors";
import { Guid } from "@thresh/core/guid";
import type {
  AgentCompletedParticipant,
  EnlistedParticipant,
  ParticipantId,
  TransactionalStatus,
  TransactionInfo,
  TransactionManager,
} from "@thresh/core/transaction-info";
import {
  definitelyAborted,
  isAgentCompleted,
  isTransactionManager,
  sameParticipant,
} from "@thresh/core/transaction-info";
import {
  TransactionResourceInterface,
  type TransactionResourceMethod,
} from "@thresh/core/transaction-resource";
import { CausalClock } from "@thresh/runtime/causal-clock";
import type { Dispatcher } from "@thresh/runtime/dispatcher";
import type { TimeProvider } from "@thresh/runtime/time-provider";

/**
 * The participant elected to manage the transaction. `local` carries its live
 * object when it is enlisted on this silo (the agent calls it directly); absent
 * when the manager was merged back from another silo and is reached over the
 * dispatcher instead.
 */
interface ElectedManager {
  readonly enlisted: EnlistedParticipant;
  readonly local?: TransactionManager | undefined;
}

/**
 * The status the agent reports for a step that failed, and the exception it
 * failed with, if any (Orleans `Resolve`'s `(TransactionalStatus, Exception)`).
 */
interface Outcome {
  readonly status: TransactionalStatus;
  readonly exception?: unknown;
}

/**
 * The exception a caller sees for a transaction that did not commit (Orleans
 * `TransactionalStatusExtensions.ConvertToUserException`). Only a definite
 * abort is reported as one; any other failure leaves the outcome unknown to the
 * agent, so it is in doubt.
 */
export function transactionStatusError(
  status: TransactionalStatus,
  transactionId: string,
  cause?: unknown,
): Error {
  const options = cause === undefined ? undefined : { cause };
  switch (status) {
    case "prepareTimeout":
      return new TransactionAbortedError(transactionId, "the prepare timed out", options);
    case "cascadingAbort":
      return new TransactionCascadingAbortError(transactionId, cause);
    case "brokenLock":
      return new TransactionBrokenLockError(transactionId, "before prepare");
    case "lockValidationFailed":
      return new TransactionBrokenLockError(
        transactionId,
        "when validating accesses during prepare",
      );
    case "participantResponseTimeout":
      return new TransactionAbortedError(
        transactionId,
        "timed out waiting for the read-only participants",
        options,
      );
    case "commitFailure":
      return new TransactionAbortedError(transactionId, "unable to commit", options);
    default:
      return new TransactionInDoubtError(transactionId, options);
  }
}

/**
 * The per-silo transaction agent (Orleans `TransactionAgent`). It begins a
 * transaction on behalf of the originating call — assigning a unique id and a
 * causal-clock timestamp — and resolves it at the boundary.
 *
 * Resolving starts the commit and leaves the rest to the transaction manager,
 * as Orleans does:
 *
 * - A read-only transaction commits in one phase: `commitReadOnly` to every
 *   participant, and an abort of all of them if any answers other than `ok`.
 * - A read-write transaction elects the TM (the first write participant that
 *   can manage), sends a one-way `prepare` to every other participant and
 *   awaits `prepareAndCommit` on the TM. The TM collects the votes, makes the
 *   commit durable, answers, and confirms or cancels the participants itself.
 *   The agent only cancels the other write participants when the TM's answer
 *   is a definite abort.
 *
 * A participant on this silo is called directly; one merged back from another
 * silo is reached over the dispatcher via the `TransactionResource` system
 * extension.
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

  /** Commit the transaction, or throw the exception its failure maps to. */
  async resolve(info: TransactionInfo): Promise<void> {
    // Close the participant set before reading it: a detached caller — a
    // `oneWay` `supported` call whose originator resolved the moment its call
    // returned — may still be running, and would otherwise enlist into a set
    // this round no longer reads. Such a resource would never be prepared,
    // committed or aborted; `requireTransaction` refuses it instead
    // (`TransactionAlreadyResolvedError`).
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

    const completedByAgent: AgentCompletedParticipant[] = [];
    const resources: EnlistedParticipant[] = [];
    for (const e of enlisted) {
      if (e.participant !== undefined && isAgentCompleted(e.participant)) {
        completedByAgent.push(e.participant);
      } else {
        resources.push(e);
      }
    }
    const writers = resources.filter((e) => e.access.writes > 0);

    let outcome: Outcome = { status: "ok" };
    if (writers.length === 0) {
      if (resources.length > 0) outcome = await this.commitReadOnlyTransaction(info, resources);
    } else {
      const manager = this.electManager(writers);
      if (manager === undefined) {
        await this.abort(info);
        throw new TransactionAbortedError(info.id, "no write participant can manage the commit");
      }
      outcome = await this.commitReadWriteTransaction(
        info,
        writers.map((e) => e.id),
        resources,
        manager,
      );
    }
    if (outcome.status !== "ok") {
      throw transactionStatusError(outcome.status, info.id, outcome.exception);
    }

    // The commit is durable. A resource the agent completes itself applies it
    // now; one whose step throws does not undo anyone's commit, it only leaves
    // the caller unable to confirm this call finished applying everywhere.
    info.status = "committed";
    const results = await Promise.allSettled(completedByAgent.map((p) => p.commit(info.id)));
    const failed = results.find((r): r is PromiseRejectedResult => r.status === "rejected");
    if (failed !== undefined) {
      throw new TransactionInDoubtError(info.id, { cause: failed.reason });
    }
  }

  /** Orleans `TransactionAgent.CommitReadOnlyTransaction`. */
  private async commitReadOnlyTransaction(
    info: TransactionInfo,
    resources: readonly EnlistedParticipant[],
  ): Promise<Outcome> {
    let outcome: Outcome = { status: "ok" };
    try {
      const results = await Promise.all(resources.map((e) => this.commitReadOnly(e, info)));
      const failed = results.find((s) => s !== "ok");
      if (failed !== undefined) outcome = { status: failed };
    } catch (exception) {
      outcome = {
        status:
          exception instanceof GrainCallTimeoutError
            ? "participantResponseTimeout"
            : "presumedAbort",
        exception,
      };
    }
    if (outcome.status !== "ok") {
      await Promise.all(resources.map((e) => this.abortOne(e, info).catch(() => undefined)));
    }
    return outcome;
  }

  /** Orleans `TransactionAgent.CommitReadWriteTransaction`. */
  private async commitReadWriteTransaction(
    info: TransactionInfo,
    writeParticipants: ParticipantId[],
    resources: readonly EnlistedParticipant[],
    manager: ElectedManager,
  ): Promise<Outcome> {
    let outcome: Outcome;
    try {
      for (const e of resources) {
        if (sameParticipant(e.id, manager.enlisted.id)) continue;
        this.prepare(e, info, manager.enlisted.id);
      }
      // Wait for the TM to commit the transaction.
      outcome = {
        status: await this.prepareAndCommit(manager, info, writeParticipants, resources.length),
      };
    } catch (exception) {
      outcome = {
        status: exception instanceof GrainCallTimeoutError ? "tmResponseTimeout" : "presumedAbort",
        exception,
      };
    }
    if (outcome.status !== "ok" && definitelyAborted(outcome.status)) {
      await Promise.all(
        writeParticipants
          .filter((p) => !sameParticipant(p, manager.enlisted.id))
          .map((p) => this.cancel(p, info, outcome.status, resources).catch(() => undefined)),
      );
    }
    return outcome;
  }

  /** Abort: release every enlisted participant's lock and roll back its tentative writes. */
  async abort(info: TransactionInfo): Promise<void> {
    // Closed before the round runs, for the same reason as `resolve`: an abort
    // releases the locks held for the participants it knows about, so a
    // resource that slipped in behind the snapshot would keep its own.
    info.status = "aborted";
    await Promise.all(
      [...info.participants.values()].map((e) => this.abortOne(e, info).catch(() => undefined)),
    );
  }

  /** One-way `prepare`. */
  private prepare(e: EnlistedParticipant, info: TransactionInfo, manager: ParticipantId): void {
    const args = [info.id, e.access, info.timeStamp, manager] as const;
    const sent =
      e.participant !== undefined && !isAgentCompleted(e.participant)
        ? e.participant.prepare(...args)
        : this.route(e.id, "prepare", args);
    sent.catch(() => undefined);
  }

  private commitReadOnly(e: EnlistedParticipant, info: TransactionInfo) {
    if (e.participant !== undefined && !isAgentCompleted(e.participant)) {
      return e.participant.commitReadOnly(info.id, e.access, info.timeStamp);
    }
    return this.route(e.id, "commitReadOnly", [
      info.id,
      e.access,
      info.timeStamp,
    ]) as Promise<TransactionalStatus>;
  }

  private prepareAndCommit(
    manager: ElectedManager,
    info: TransactionInfo,
    writeParticipants: ParticipantId[],
    totalParticipants: number,
  ): Promise<TransactionalStatus> {
    const args = [
      info.id,
      manager.enlisted.access,
      info.timeStamp,
      writeParticipants,
      totalParticipants,
    ] as const;
    if (manager.local !== undefined) return manager.local.prepareAndCommit(...args);
    return this.route(
      manager.enlisted.id,
      "prepareAndCommit",
      args,
    ) as Promise<TransactionalStatus>;
  }

  private cancel(
    id: ParticipantId,
    info: TransactionInfo,
    status: TransactionalStatus,
    resources: readonly EnlistedParticipant[],
  ): Promise<unknown> {
    const local = resources.find((e) => sameParticipant(e.id, id))?.participant;
    if (local !== undefined && !isAgentCompleted(local)) {
      return local.cancel(info.id, info.timeStamp, status);
    }
    return this.route(id, "cancel", [info.id, info.timeStamp, status]);
  }

  private async abortOne(e: EnlistedParticipant, info: TransactionInfo): Promise<void> {
    if (e.participant !== undefined) {
      await e.participant.abort(info.id);
      return;
    }
    await this.route(e.id, "abort", [info.id]);
  }

  private route(
    id: ParticipantId,
    method: TransactionResourceMethod,
    args: readonly unknown[],
  ): Promise<unknown> {
    if (this.dispatcher === undefined) {
      return Promise.reject(new Error("transaction agent has no dispatcher"));
    }
    return this.dispatcher.invoke({
      target: id.grainId,
      interfaceId: TransactionResourceInterface.id,
      method,
      args: [id.stateName, ...args],
      // Always-interleave (see `TransactionResourceInterface`): a local
      // dispatch takes the request's options as given.
      options: TransactionResourceInterface.options[method] ?? {},
    });
  }

  /**
   * The elected manager (Orleans' first write participant that supports the
   * Manager role), or undefined when no writer can manage the transaction.
   *
   * A locally-enlisted participant must implement the manager role to be
   * elected. A participant merged back from another silo carries no role of
   * its own (the reply's `SerializedParticipant` is just `{id, access}`), so it
   * is taken at its word: it is reached through `TransactionResource`, whose
   * contract covers the manager messages too.
   */
  private electManager(writers: readonly EnlistedParticipant[]): ElectedManager | undefined {
    for (const e of writers) {
      const local = e.participant;
      if (local === undefined) return { enlisted: e };
      if (isTransactionManager(local)) return { enlisted: e, local };
    }
    return undefined;
  }
}
