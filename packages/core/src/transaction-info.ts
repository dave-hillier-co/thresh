/**
 * Cross-grain transaction context and participant contracts. Mirrors Orleans'
 * `TransactionInfo` / `ParticipantId` / `AccessCounter` from
 * `Orleans.Transactions`. These are pure contracts; the agent, the wait-die lock
 * and the commit protocol live in the runtime and `@thresh/transactions`.
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

/** A transaction's status once resolution has begun: its participant set is closed. */
export type ResolvedStatus = "resolving" | "committed" | "aborted";

/**
 * Where a transaction stands (Orleans `TransactionStatus`). `active` while it
 * may still take on participants; `resolving` from the moment its boundary
 * begins the commit round — which is when the participant set is snapshotted —
 * through until the outcome is decided; then `committed` or `aborted` for good.
 * Anything past `active` ({@link ResolvedStatus}) means a resource enlisting
 * now would never be prepared, committed or aborted, so it must not be allowed
 * to take state (see {@link isResolved} and `requireTransaction`,
 * `@thresh/runtime/invocation-context`).
 */
export type TransactionStatus = "active" | ResolvedStatus;

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
 *
 * This alone makes a resource participant and nothing more: it can never be
 * elected transaction manager — see {@link TransactionManager}.
 */
export interface TransactionParticipant {
  prepare(
    transactionId: string,
    timeStamp: number,
    manager: ParticipantId,
  ): boolean | Promise<boolean>;
  commit(transactionId: string): void | Promise<void>;
  abort(transactionId: string): void | Promise<void>;
}

/**
 * A participant that can also act as the transaction's manager (Orleans
 * `ITransactionManager` — a separate interface upstream, which does not extend
 * `ITransactionalResource` either). Beyond driving its own state, a manager
 * durably records the transaction's commit before any participant commits —
 * the protocol's atomic commit point — and answers a recovering participant's
 * `status` query about it. Both halves are required of the same participant:
 * recording without answering leaves a sibling's in-doubt record unresolvable,
 * and answering without recording reports a commit that was never durable.
 *
 * Not every resource can serve: `TransactionalStateImpl` does (its commit
 * records are WAL-backed), while `TransactionCommitter` deliberately does not.
 * `TransactionAgent` elects the manager from the write participants that
 * implement this — so a transaction whose writers are all resource-only commits
 * without a durable commit point, rather than routing the commit point at a
 * manager that cannot record one. See `electManager`.
 */
export interface TransactionManager extends TransactionParticipant {
  /** Durable commit record: the transaction has committed, whatever happens next. */
  recordCommit(
    transactionId: string,
    timeStamp: number,
    writeParticipants: ParticipantId[],
  ): void | Promise<void>;
  /** Whether `transactionId` committed — the query a recovering participant makes. */
  status(transactionId: string): boolean | Promise<boolean>;
}

/**
 * Whether `participant` implements the manager half of the contract as well as
 * the resource half. This is the port's stand-in for Orleans'
 * `ParticipantId.IsManager()` role check, which a participant here has no field
 * to declare (`ParticipantId` is just a grain id and a state name): the methods
 * themselves are the declaration.
 */
export function isTransactionManager(
  participant: TransactionParticipant,
): participant is TransactionManager {
  const candidate = participant as Partial<TransactionManager>;
  return typeof candidate.recordCommit === "function" && typeof candidate.status === "function";
}

/**
 * A participant enlisted in a transaction, with the access it has accrued. A
 * participant enlisted on the local silo carries its live `participant` object
 * (the agent drives it directly, and can tell from that object whether it may
 * serve as the transaction manager — see {@link isTransactionManager}); one
 * merged back from another silo via a reply carries only `id`, and the agent
 * routes to it over the dispatcher.
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
  /**
   * Where this transaction stands, set by the agent's boundary as it resolves
   * it. Absent means `active`: a context rebuilt from a remote hop's wire
   * header (`ClusterNode`) carries no status of its own — that silo cannot know
   * the originator has since resolved the transaction.
   */
  status?: TransactionStatus;
  /**
   * Count of calls forked off this transaction (via {@link forkTransaction})
   * that have not yet been matched by a completion, mirroring Orleans
   * `TransactionInfo.PendingCalls`. The root boundary must see this at zero
   * before it may resolve (commit) — see `TransactionAgent.resolve` and
   * `TransactionOrphanCallError` (`@thresh/core/errors`).
   */
  pendingCalls: number;
}

/**
 * Whether the transaction's boundary has resolved it (or begun to). Once it
 * has, the participant set is closed: the agent resolves from a snapshot, so a
 * resource enlisting now would never be prepared, committed or aborted, and
 * the state lock it took would be held until deactivation — so
 * `requireTransaction` (`@thresh/runtime/invocation-context`) refuses the
 * enlistment outright instead.
 */
export function isResolved(
  info: TransactionInfo,
): info is TransactionInfo & { status: ResolvedStatus } {
  return info.status !== undefined && info.status !== "active";
}

/**
 * Detach a call from the transaction's own completion without awaiting it
 * (Orleans `TransactionInfo.Fork`): increments {@link TransactionInfo.pendingCalls}
 * so the root boundary can detect, at resolve time, that an orphaned call was
 * left outstanding and abort rather than commit unsafely.
 */
export function forkTransaction(info: TransactionInfo): void {
  info.pendingCalls += 1;
}
