import { defineGrainInterface } from "./grain-interface";
import type { ParticipantId } from "./transaction-info";

/**
 * The system extension the transaction agent calls to drive a transactional
 * resource that lives on another silo (Orleans `ITransactionalResourceExtension`).
 * The first argument names the state on the target grain; the activation routes
 * the call to that resource. Mirrors the `StreamConsumer` system-extension
 * precedent. Local participants are driven directly (no dispatch); this is the
 * path for participants merged back from a remote silo.
 *
 * `status` and `recordCommit` serve recovery: the elected TM records a commit
 * record before participants commit (the atomic commit point), and an in-doubt
 * participant asks the TM via `status` whether the transaction committed.
 */
export interface TransactionResource {
  prepare(
    stateName: string,
    transactionId: string,
    timeStamp: number,
    manager: ParticipantId,
  ): Promise<boolean>;
  commit(stateName: string, transactionId: string): Promise<void>;
  abort(stateName: string, transactionId: string): Promise<void>;
  /** TM only: durably record that the transaction committed (its commit point). */
  recordCommit(
    stateName: string,
    transactionId: string,
    timeStamp: number,
    writeParticipants: ParticipantId[],
  ): Promise<void>;
  /** TM only: whether the transaction committed (recovery query). */
  status(stateName: string, transactionId: string): Promise<boolean>;
}

export const TransactionResourceInterface = defineGrainInterface<TransactionResource>(
  "system.TransactionResource",
);
