import { Guid } from "@tsva/core/guid";
import type { TransactionInfo, TransactionParticipant } from "@tsva/core/transaction-info";
import { currentTransaction, requireTransaction } from "@tsva/runtime/invocation-context";

/**
 * A minimal transactional value — the Slice 1 seed for the `TransactionalState`
 * facet ([ADR 0008](../../docs/adr/0008-cross-grain-transactions.md)). It keeps a
 * committed value plus per-transaction tentative writes and enlists itself as a
 * participant on first access, so the agent commits or aborts it at the
 * boundary. Reads outside a transaction see the committed value.
 *
 * Slice 2 grows this into the full facet (versioned state, `performRead` /
 * `performUpdate`, and the wait-die reader-writer lock).
 */
export class TransactionalValue<T> implements TransactionParticipant {
  private readonly key = Guid.newGuid().toString();
  private committed: T;
  private readonly tentative = new Map<string, T>();

  constructor(initial: T) {
    this.committed = initial;
  }

  /** The tentative value if this transaction has written, else the committed value. */
  get(): T {
    const tx = currentTransaction();
    if (tx === undefined) return this.committed;
    this.enlist(tx, 1, 0);
    return this.tentative.has(tx.id) ? (this.tentative.get(tx.id) as T) : this.committed;
  }

  /** Stage a tentative write for the current transaction; requires one in scope. */
  set(value: T): void {
    const tx = requireTransaction();
    this.enlist(tx, 0, 1);
    this.tentative.set(tx.id, value);
  }

  prepare(_transactionId: string): boolean {
    // No durable staging in the seed; a transaction's tentative write is always
    // ready to commit once it reaches prepare.
    return true;
  }

  commit(transactionId: string): void {
    if (this.tentative.has(transactionId)) {
      this.committed = this.tentative.get(transactionId) as T;
      this.tentative.delete(transactionId);
    }
  }

  abort(transactionId: string): void {
    this.tentative.delete(transactionId);
  }

  private enlist(tx: TransactionInfo, reads: number, writes: number): void {
    const existing = tx.participants.get(this.key);
    if (existing === undefined) {
      tx.participants.set(this.key, { participant: this, access: { reads, writes } });
    } else {
      existing.access.reads += reads;
      existing.access.writes += writes;
    }
  }
}
