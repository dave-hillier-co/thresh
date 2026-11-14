import type { GrainId } from "@thresh/core/grain-id";
import { TransactionReadOnlyViolatedError } from "@thresh/core/errors";
import type { AgentCompletedParticipant, ParticipantId } from "@thresh/core/transaction-info";
import { requireTransaction } from "@thresh/runtime/invocation-context";

/**
 * A commit-time action against a non-grain "service" resource, enlisted
 * directly in a transaction's commit (Orleans
 * `ITransactionCommitOperation<TService>`). Applied once, from
 * {@link TransactionCommitter.commit}, after the transaction manager has made
 * the commit durable. Returning `false` or throwing means the commit step
 * itself failed or could not be confirmed — since the commit is already
 * decided by then, that surfaces to the caller as `TransactionInDoubtError`
 * (`@thresh/core/errors`), not an abort.
 */
export interface TransactionCommitOperation<TService> {
  commit(transactionId: string, service: TService): boolean | Promise<boolean>;
}

/**
 * A transactional resource that wraps a plain (non-grain) service object,
 * mirroring Orleans' `TransactionCommitter<TService>`
 * (`src/Orleans.Transactions/TOC/TransactionCommitter.cs`). It stages a
 * {@link TransactionCommitOperation} and, at commit time, runs it against
 * `service` — letting an external system (a message queue, a payment gateway,
 * a legacy RPC endpoint) take part in a transaction without being a grain.
 *
 * This is deliberately a thin, memory-only port for the parity test suite, and
 * it departs from upstream in how it takes part. Upstream's committer has its
 * own storage-backed commit queue and is elected TM ahead of every other
 * writer (a priority manager), running the operation as a precondition of its
 * commit write. This one has no storage and cannot be reached by participant
 * id, so it stays outside the protocol: it is an
 * {@link AgentCompletedParticipant}, which the agent commits after the elected
 * TM has made the commit durable, and aborts otherwise. A re-activation loses
 * a pending operation. See `docs/deviations.md`.
 */
export class TransactionCommitter<TService> implements AgentCompletedParticipant {
  readonly completedByAgent = true as const;
  private readonly pending = new Map<string, TransactionCommitOperation<TService>>();

  constructor(
    private readonly grainId: GrainId,
    private readonly stateName: string,
    private readonly service: TService,
  ) {}

  private get participantId(): ParticipantId {
    return { grainId: this.grainId, stateName: this.stateName };
  }

  private get key(): string {
    return `${this.grainId.toString()}/${this.stateName}`;
  }

  /** Stage `operation`, enlisting this committer as a write participant of the ambient transaction. */
  onCommit(operation: TransactionCommitOperation<TService>): void {
    const tx = requireTransaction();
    if (tx.readOnly) throw new TransactionReadOnlyViolatedError(tx.id);
    this.pending.set(tx.id, operation);
    const existing = tx.participants.get(this.key);
    if (existing === undefined) {
      tx.participants.set(this.key, {
        id: this.participantId,
        participant: this,
        access: { reads: 0, writes: 1 },
      });
    } else {
      existing.access.writes += 1;
    }
  }

  /** Run the staged operation against the service; a `false`/throw fails only this commit step. */
  async commit(transactionId: string): Promise<void> {
    const operation = this.pending.get(transactionId);
    this.pending.delete(transactionId);
    if (operation === undefined) return;
    const ok = await operation.commit(transactionId, this.service);
    if (!ok) {
      throw new Error(`transaction ${transactionId} commit operation reported failure`);
    }
  }

  async abort(transactionId: string): Promise<void> {
    this.pending.delete(transactionId);
  }
}
