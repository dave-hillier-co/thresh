/**
 * Cross-grain transaction context and participant contracts (Phase 7,
 * [ADR 0008](../../docs/adr/0008-cross-grain-transactions.md)). Mirrors Orleans'
 * `TransactionInfo` / `ParticipantId` / `AccessCounter` from
 * `Orleans.Transactions`. These are pure contracts; the agent, the wait-die lock
 * and the commit protocol live in the runtime and `@tsva/transactions`.
 */

import type { GrainId } from "./grain-id";

/**
 * How a method relates to the ambient transaction, mirroring Orleans'
 * `TransactionOption`. `create`/`createOrJoin` start one if needed; `join`
 * requires one; `suppress` runs outside any; `notAllowed` rejects if one is
 * present; `supported` joins if present but never starts one.
 */
export type TransactionOption =
  | "create"
  | "createOrJoin"
  | "join"
  | "supported"
  | "notAllowed"
  | "suppress";

/** Reads and writes a transaction performed on one participant (Orleans `AccessCounter`). */
export interface AccessCounter {
  reads: number;
  writes: number;
}

/**
 * Serializable identity of a transactional resource: the grain that hosts it and
 * the named state on that grain (Orleans `ParticipantId`). Lets the agent route
 * prepare/commit/abort to a participant on any silo via the dispatcher.
 */
export interface ParticipantId {
  grainId: GrainId;
  stateName: string;
}

/** Stable map key for a participant within a transaction. */
export function participantKey(id: ParticipantId): string {
  return `${id.grainId.toString()}/${id.stateName}`;
}

/**
 * A transactional resource enlisted in a transaction, driven by the agent at the
 * boundary through the two-phase protocol (Orleans `ITransactionalResource`):
 *
 * - `prepare` validates the resource still holds its lock with the expected
 *   access and durably stages its tentative state; it returns `false` (or
 *   throws) to veto the commit.
 * - `commit` makes the prepared tentative state the committed version.
 * - `abort` discards tentative state and releases locks.
 *
 * A read-only participant may skip staging in `prepare` (validate-only). Commit
 * application must be idempotent: recovery may re-apply a prepared record.
 */
export interface TransactionParticipant {
  prepare(
    transactionId: string,
    timeStamp: number,
    manager: ParticipantId,
  ): boolean | Promise<boolean>;
  commit(transactionId: string): void | Promise<void>;
  abort(transactionId: string): void | Promise<void>;
  /** TM only: durably record the transaction's commit before participants commit. */
  recordCommit(
    transactionId: string,
    timeStamp: number,
    writeParticipants: ParticipantId[],
  ): void | Promise<void>;
}

/**
 * A participant enlisted in a transaction, with the access it has accrued. A
 * participant enlisted on the local silo carries its live `participant` object
 * (the agent drives it directly); one merged back from another silo via a reply
 * carries only `id`, and the agent routes to it over the dispatcher.
 */
export interface EnlistedParticipant {
  readonly id: ParticipantId;
  readonly participant?: TransactionParticipant | undefined;
  readonly access: AccessCounter;
}

/**
 * The ambient context for one transaction, propagated along the call chain
 * through the request context. Within a single process the same object flows by
 * reference, so resources enlist themselves into `participants` and the agent
 * reads that set at the boundary. Cross-silo merging of participant sets via
 * replies is later-slice work.
 */
export interface TransactionInfo {
  /** Globally unique transaction id. */
  readonly id: string;
  /** Logical commit timestamp from the agent's causal clock; also the priority. */
  timeStamp: number;
  readonly readOnly: boolean;
  /** Live participant set, keyed by a stable per-resource key. */
  readonly participants: Map<string, EnlistedParticipant>;
}
