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

/**
 * The outcome a step of the commit protocol reports (Orleans
 * `TransactionalStatus`): `ok`, or why the transaction did not commit.
 *
 * - `prepareTimeout`: the TM did not hear every participant's vote in time.
 * - `cascadingAbort`: a transaction this one depends on aborted.
 * - `brokenLock`: a lock was lost (timeout, wait-die, a replaced activation).
 * - `lockValidationFailed`: the recorded accesses did not match the lock's.
 * - `participantResponseTimeout`: a read-only participant did not answer.
 * - `tmResponseTimeout`: the agent gave up waiting for the TM.
 * - `storageConflict`: storage was changed by a duplicate activation.
 * - `presumedAbort`: the TM never heard of the transaction.
 * - `unknownException`, `assertionFailed`, `commitFailure`: as named.
 */
export type TransactionalStatus =
  | "ok"
  | "prepareTimeout"
  | "cascadingAbort"
  | "brokenLock"
  | "lockValidationFailed"
  | "participantResponseTimeout"
  | "tmResponseTimeout"
  | "storageConflict"
  | "presumedAbort"
  | "unknownException"
  | "assertionFailed"
  | "commitFailure";

/**
 * Whether `status` means the transaction certainly did not commit anywhere
 * (Orleans `TransactionalStatusExtensions.DefinitelyAborted`). Any other
 * non-`ok` status leaves the outcome unknown to whoever received it.
 */
export function definitelyAborted(status: TransactionalStatus): boolean {
  switch (status) {
    case "prepareTimeout":
    case "cascadingAbort":
    case "brokenLock":
    case "lockValidationFailed":
    case "participantResponseTimeout":
    case "commitFailure":
      return true;
    default:
      return false;
  }
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
 * the named state on that grain (Orleans `ParticipantId`). Lets the agent and
 * the transaction manager route protocol messages to a participant on any silo
 * via the dispatcher.
 */
export interface ParticipantId {
  grainId: GrainId;
  stateName: string;
}

/** Stable map key for a participant within a transaction. */
export function participantKey(id: ParticipantId): string {
  return `${id.grainId.toString()}/${id.stateName}`;
}

/** Whether two participant ids name the same resource. */
export function sameParticipant(a: ParticipantId, b: ParticipantId): boolean {
  return a.stateName === b.stateName && a.grainId.equals(b.grainId);
}

/**
 * A transactional resource: the participant role of the commit protocol
 * (Orleans `ITransactionalResource`). The agent and the transaction manager
 * send it these messages; every one of them names the transaction by id.
 *
 * - `commitReadOnly`: the agent's one-phase commit of a read-only transaction.
 *   The resource validates its lock (Orleans `ReaderWriterLock.ValidateLock`)
 *   and answers once the read is ordered behind every earlier pending write.
 * - `prepare`: one-way, from the agent to every participant but the TM. The
 *   resource validates its lock, durably records its tentative state with the
 *   TM's identity, and reports its vote to the TM through `prepared`.
 * - `abort`: one-way, from the agent: release the lock and roll back.
 * - `cancel`: from the TM: the transaction aborted; drop its prepared record
 *   and every later record built on it.
 * - `confirm`: from the TM: the transaction committed; make the prepared
 *   record the committed version, and answer once that is durable.
 *
 * This alone makes a resource a participant and nothing more: it can never be
 * elected transaction manager; see {@link TransactionManager}.
 */
export interface TransactionParticipant {
  commitReadOnly(
    transactionId: string,
    accessCount: AccessCounter,
    timeStamp: number,
  ): Promise<TransactionalStatus>;
  prepare(
    transactionId: string,
    accessCount: AccessCounter,
    timeStamp: number,
    transactionManager: ParticipantId,
  ): Promise<void>;
  abort(transactionId: string): Promise<void>;
  cancel(transactionId: string, timeStamp: number, status: TransactionalStatus): Promise<void>;
  confirm(transactionId: string, timeStamp: number): Promise<void>;
}

/**
 * The transaction manager role (Orleans `ITransactionManager`, a separate
 * interface upstream that does not extend `ITransactionalResource`). The agent
 * elects the TM from the write participants that implement it.
 *
 * - `prepareAndCommit`: from the agent. The TM validates its own lock, waits
 *   for a `prepared` vote from every other participant, makes the commit
 *   durable together with its own write, and answers. It then confirms the
 *   other write participants and, once all have confirmed, removes its commit
 *   record; on an abort it cancels them instead.
 * - `prepared`: one-way, from a participant: its vote.
 * - `ping`: one-way, from a prepared participant still waiting for the
 *   outcome. The TM answers a transaction it has never heard of with a
 *   `cancel` (presumed abort), and otherwise lets its own confirmation reach
 *   the participant.
 *
 * Not every resource can serve: `TransactionalStateImpl` does, while
 * `TransactionCommitter` deliberately does not. See `electManager` in
 * `TransactionAgent`.
 */
export interface TransactionManager {
  prepareAndCommit(
    transactionId: string,
    accessCount: AccessCounter,
    timeStamp: number,
    writeParticipants: ParticipantId[],
    totalParticipants: number,
  ): Promise<TransactionalStatus>;
  prepared(
    transactionId: string,
    timeStamp: number,
    participant: ParticipantId,
    status: TransactionalStatus,
  ): Promise<void>;
  ping(transactionId: string, timeStamp: number, participant: ParticipantId): Promise<void>;
}

/**
 * Whether `participant` implements the manager role as well as the resource
 * role. This is the port's stand-in for Orleans' `ParticipantId.IsManager()`
 * role check, which a participant here has no field to declare
 * (`ParticipantId` is just a grain id and a state name): the methods
 * themselves are the declaration.
 */
export function isTransactionManager(
  participant: TransactionParticipant | AgentCompletedParticipant,
): participant is TransactionParticipant & TransactionManager {
  const candidate = participant as Partial<TransactionManager>;
  return (
    typeof candidate.prepareAndCommit === "function" &&
    typeof candidate.prepared === "function" &&
    typeof candidate.ping === "function"
  );
}

/**
 * A resource outside the commit protocol that the agent completes itself once
 * the TM has decided: `commit` after a commit, `abort` otherwise. The port's
 * memory-only `TransactionCommitter` (`@thresh/transactions`) is the one such
 * resource; upstream's committer is a full protocol participant with its own
 * storage, elected TM ahead of every other writer. See `docs/deviations.md`.
 */
export interface AgentCompletedParticipant {
  readonly completedByAgent: true;
  commit(transactionId: string): Promise<void>;
  abort(transactionId: string): Promise<void>;
}

/** Whether `participant` is completed by the agent rather than through the protocol. */
export function isAgentCompleted(
  participant: TransactionParticipant | AgentCompletedParticipant,
): participant is AgentCompletedParticipant {
  return (participant as Partial<AgentCompletedParticipant>).completedByAgent === true;
}

/**
 * A transactional resource that can delay its activation's deactivation (Orleans
 * `IActivationLifetime.BlockDeactivation`): while `blocksDeactivation` is true
 * — it holds a transaction lock, a transaction in its commit queue, or a commit
 * it is still confirming — a deactivation (idle collection, migration,
 * rebalancing, an explicit `deactivateOnIdle`) waits for it to clear, for at
 * most five seconds (Orleans `ActivationLifetime.OnStopAsync`), then proceeds.
 */
export interface DeactivationBlocker {
  readonly blocksDeactivation: boolean;
}

/** Whether `resource` is a {@link DeactivationBlocker} that is blocking right now. */
export function blocksDeactivation(resource: unknown): boolean {
  return (
    typeof resource === "object" &&
    resource !== null &&
    (resource as Partial<DeactivationBlocker>).blocksDeactivation === true
  );
}

/**
 * A participant enlisted in a transaction, with the access it has accrued. A
 * participant enlisted on the local silo carries its live `participant` object
 * (the agent sends it messages directly, and can tell from that object whether
 * it may serve as the transaction manager — see {@link isTransactionManager});
 * one merged back from another silo via a reply carries only `id`, and the
 * agent routes to it over the dispatcher.
 */
export interface EnlistedParticipant {
  readonly id: ParticipantId;
  readonly participant?: TransactionParticipant | AgentCompletedParticipant | undefined;
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
