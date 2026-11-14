import { defineGrainInterface } from "./grain-interface";
import type { AccessCounter, ParticipantId, TransactionOutcome } from "./transaction-info";

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
 * participant asks the TM via `status` what it knows of the transaction's fate.
 */
export interface TransactionResource {
  /**
   * Validate that this resource still holds the transaction's lock with
   * exactly `accessCount` (the reads and writes the coordinator recorded for
   * it), then stage its tentative state; `false` vetoes the commit.
   */
  prepare(
    stateName: string,
    transactionId: string,
    accessCount: AccessCounter,
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
  /** TM only: what the TM knows of the transaction's fate (a prepared participant's query). */
  status(stateName: string, transactionId: string): Promise<TransactionOutcome>;
}

/**
 * Every protocol call is always-interleave, as Orleans marks
 * `ITransactionalResourceExtension` and `ITransactionManagerExtension`: they
 * must run while the grain's own turn is suspended — above all while that turn
 * waits for the very lock an abort or commit here releases.
 */
export const TransactionResourceInterface = defineGrainInterface<TransactionResource>(
  "system.TransactionResource",
  {
    options: {
      prepare: { alwaysInterleave: true },
      commit: { alwaysInterleave: true },
      abort: { alwaysInterleave: true },
      recordCommit: { alwaysInterleave: true },
      status: { alwaysInterleave: true },
    },
  },
);
