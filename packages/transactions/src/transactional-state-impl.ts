import type { GrainId } from "@tsva/core/grain-id";
import type { TransactionalState } from "@tsva/core/transactional-state";
import type { TransactionInfo, TransactionParticipant } from "@tsva/core/transaction-info";
import type {
  PendingTransactionState,
  TransactionalStateMetadata,
  TransactionalStateStorage,
  TransactionalStorageLoadResponse,
} from "@tsva/core/transactional-storage";
import { requireTransaction } from "@tsva/runtime/invocation-context";
import { ReaderWriterLock } from "@tsva/transactions/reader-writer-lock";

const clone = <T>(value: T): T =>
  typeof structuredClone === "function"
    ? structuredClone(value)
    : (JSON.parse(JSON.stringify(value)) as T);

interface Tentative<T> {
  transactionId: string;
  value: T;
  sequenceId: number;
  timeStamp: number;
  prepared: boolean;
}

/**
 * A transactional-state resource (Orleans `TransactionalState`) for one named
 * state on a grain. It keeps a committed version with a dense sequence id and a
 * single per-transaction tentative copy (single because the wait-die lock admits
 * one writer at a time), enlists itself as a participant on first access, and
 * runs the two-phase protocol against a durable {@link TransactionalStateStorage}:
 * `prepare` stages a pending record, `commit` promotes it to committed, `abort`
 * drops it. The committed version is loaded on activation, so it survives
 * deactivation and silo restart.
 */
export class TransactionalStateImpl<T> implements TransactionalState<T>, TransactionParticipant {
  private readonly key: string;
  private readonly lock = new ReaderWriterLock();
  private committed!: T;
  private committedSequenceId = 0;
  private etag: string | undefined;
  private metadata: TransactionalStateMetadata = { timeStamp: 0, commitRecords: {} };
  private tentative: Tentative<T> | undefined;

  constructor(
    private readonly stateName: string,
    private readonly grainId: GrainId,
    private readonly initial: () => T,
    private readonly storage: TransactionalStateStorage,
  ) {
    this.key = `${grainId.toString()}/${stateName}`;
  }

  /** Load the committed version before `onActivate`, so reads serve durable state. */
  async load(): Promise<void> {
    const response = (await this.storage.load(
      this.stateName,
      this.grainId,
    )) as TransactionalStorageLoadResponse<T>;
    this.etag = response.etag;
    this.committedSequenceId = response.committedSequenceId;
    this.metadata = response.metadata;
    this.committed = response.etag === undefined ? this.initial() : response.committedState;
    // In-doubt pending states (response.pendingStates) are resolved by recovery
    // in a later slice; a clean restart after commit has none.
  }

  async performRead<R>(read: (state: T) => R): Promise<R> {
    const tx = requireTransaction();
    await this.lock.enter(tx.id, tx.timeStamp, "read");
    this.enlist(tx, 1, 0);
    const state = this.tentative?.transactionId === tx.id ? this.tentative.value : this.committed;
    return read(state);
  }

  async performUpdate<R>(update: (state: T) => R): Promise<R> {
    const tx = requireTransaction();
    await this.lock.enter(tx.id, tx.timeStamp, "write");
    this.enlist(tx, 0, 1);
    if (this.tentative?.transactionId !== tx.id) {
      this.tentative = {
        transactionId: tx.id,
        value: clone(this.committed),
        sequenceId: this.committedSequenceId + 1,
        timeStamp: tx.timeStamp,
        prepared: false,
      };
    }
    return update(this.tentative.value);
  }

  /** Durably stage this transaction's tentative state as a pending record. */
  async prepare(transactionId: string, timeStamp: number): Promise<boolean> {
    if (this.tentative?.transactionId !== transactionId) return true; // read-only participant
    const pending: PendingTransactionState<T> = {
      sequenceId: this.tentative.sequenceId,
      transactionId,
      timeStamp,
      state: this.tentative.value,
    };
    this.etag = await this.storage.store(
      this.stateName,
      this.grainId,
      this.etag,
      this.metadata,
      [pending],
      undefined,
      undefined,
    );
    this.tentative.prepared = true;
    return true;
  }

  async commit(transactionId: string): Promise<void> {
    const tentative = this.tentative;
    if (tentative?.transactionId === transactionId) {
      this.etag = await this.storage.store(
        this.stateName,
        this.grainId,
        this.etag,
        this.metadata,
        [],
        tentative.sequenceId,
        undefined,
      );
      this.committed = tentative.value;
      this.committedSequenceId = tentative.sequenceId;
      this.tentative = undefined;
    }
    this.lock.release(transactionId);
  }

  async abort(transactionId: string): Promise<void> {
    const tentative = this.tentative;
    if (tentative?.transactionId === transactionId) {
      if (tentative.prepared) {
        // Drop the durable pending record staged during prepare.
        this.etag = await this.storage.store(
          this.stateName,
          this.grainId,
          this.etag,
          this.metadata,
          [],
          undefined,
          this.committedSequenceId,
        );
      }
      this.tentative = undefined;
    }
    this.lock.release(transactionId);
  }

  private enlist(tx: TransactionInfo, reads: number, writes: number): void {
    const existing = tx.participants.get(this.key);
    if (existing === undefined) {
      tx.participants.set(this.key, {
        id: { grainId: this.grainId, stateName: this.stateName },
        participant: this,
        access: { reads, writes },
      });
    } else {
      existing.access.reads += reads;
      existing.access.writes += writes;
    }
  }
}
