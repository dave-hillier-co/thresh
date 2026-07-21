import type { GrainId } from "@tsva/core/grain-id";
import { TransactionReadOnlyViolatedError } from "@tsva/core/errors";
import type { ParticipantId, TransactionParticipant } from "@tsva/core/transaction-info";
import { requireTransaction } from "@tsva/runtime/invocation-context";

/**
 * A commit-time action against a non-grain "service" resource, enlisted
 * directly in a transaction's 2PC commit phase (Orleans
 * `ITransactionCommitOperation<TService>`). Applied once, from
 * {@link TransactionCommitter.commit}, after every participant (grain-hosted
 * or not) has prepared successfully. Returning `false` or throwing means the
 * commit step itself failed or could not be confirmed — since the transaction
 * manager has already durably recorded the commit by then, that surfaces to
 * the caller as `TransactionInDoubtError` (`@tsva/core/errors`), not an abort.
 */
export interface TransactionCommitOperation<TService> {
  commit(transactionId: string, service: TService): boolean | Promise<boolean>;
}

/**
 * A transactional resource that wraps a plain (non-grain) service object,
 * mirroring Orleans' `TransactionCommitter<TService>`
 * (`src/Orleans.Transactions/TOC/TransactionCommitter.cs`). Where
 * `TransactionalStateImpl` stages and commits a grain's own state,
 * `TransactionCommitter` stages a {@link TransactionCommitOperation} and, at
 * commit time, runs it against `service` — letting an external system (a
 * message queue, a payment gateway, a legacy RPC endpoint) be enlisted as a
 * genuine 2PC participant without being a grain itself.
 *
 * This is deliberately a thin, memory-only port for the parity test suite: it
 * has no durable prepare record of its own (a re-activation after a crash
 * between prepare and commit simply loses the pending operation, unlike
 * `TransactionalStateImpl`'s WAL-backed staging) — acceptable because the
 * upstream test this exists for
 * (`TocFaultTransactionMemoryTests.MultiGrainWriteTransactionWithCommitException`)
 * only exercises the in-process commit-time failure path, not crash recovery.
 */
export class TransactionCommitter<TService> implements TransactionParticipant {
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

  /** Nothing durable to stage beyond the in-memory pending operation recorded by `onCommit`. */
  prepare(): boolean {
    return true;
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

  abort(transactionId: string): void {
    this.pending.delete(transactionId);
  }

  /** This committer is never the elected manager in the ported test scenarios. */
  recordCommit(): void {
    // no-op: see class doc — no durable commit record of its own.
  }
}
