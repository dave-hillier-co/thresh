import type { GrainId } from "@tsva/core/grain-id";
import type { TransactionalState } from "@tsva/core/transactional-state";
import type {
  ParticipantId,
  TransactionInfo,
  TransactionParticipant,
} from "@tsva/core/transaction-info";
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

/** Asks the elected TM whether an in-doubt transaction committed (recovery). */
export type ResolveStatus = (manager: ParticipantId, transactionId: string) => Promise<boolean>;

/**
 * A transactional-state resource (Orleans `TransactionalState`) for one named
 * state on a grain. It keeps a committed version with a dense sequence id and a
 * single per-transaction tentative copy (single because the wait-die lock admits
 * one writer at a time), enlists itself as a participant on first access, and
 * runs the two-phase protocol against a durable {@link TransactionalStateStorage}:
 * `prepare` stages a pending record (with the elected TM), `commit` promotes it,
 * `abort` drops it. The committed version is loaded on activation; an in-doubt
 * pending record left by a mid-commit crash is resolved against its TM.
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
    private readonly resolveStatus?: ResolveStatus,
  ) {
    this.key = `${grainId.toString()}/${stateName}`;
  }

  private get participantId(): ParticipantId {
    return { grainId: this.grainId, stateName: this.stateName };
  }

  /** Load the committed version before `onActivate`, then resolve any in-doubt pending. */
  async load(): Promise<void> {
    const response = (await this.storage.load(
      this.stateName,
      this.grainId,
    )) as TransactionalStorageLoadResponse<T>;
    this.etag = response.etag;
    this.committedSequenceId = response.committedSequenceId;
    this.metadata = response.metadata;
    // No committed transaction yet (a fresh record, or one with only pending
    // states) means the initial value; otherwise the loaded committed snapshot.
    this.committed = response.committedSequenceId === 0 ? this.initial() : response.committedState;
    if (response.pendingStates.length > 0) await this.recover(response.pendingStates);
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

  /** Durably stage this transaction's tentative state, recording its TM. */
  async prepare(
    transactionId: string,
    timeStamp: number,
    manager: ParticipantId,
  ): Promise<boolean> {
    if (this.tentative?.transactionId !== transactionId) return true; // read-only participant
    const pending: PendingTransactionState<T> = {
      sequenceId: this.tentative.sequenceId,
      transactionId,
      timeStamp,
      transactionManager: manager,
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

  /** TM only: durably record the commit (the atomic point), before participants commit. */
  async recordCommit(
    transactionId: string,
    timeStamp: number,
    writeParticipants: ParticipantId[],
  ): Promise<void> {
    this.metadata = {
      timeStamp: Math.max(this.metadata.timeStamp, timeStamp),
      commitRecords: {
        ...this.metadata.commitRecords,
        [transactionId]: {
          timeStamp,
          writeParticipants: writeParticipants.map((p) => `${p.grainId.toString()}/${p.stateName}`),
        },
      },
    };
    this.etag = await this.storage.store(
      this.stateName,
      this.grainId,
      this.etag,
      this.metadata,
      [],
      undefined,
      undefined,
    );
  }

  /** TM only: whether the transaction committed (recovery query). */
  status(transactionId: string): boolean {
    return this.metadata.commitRecords[transactionId] !== undefined;
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

  /**
   * Resolve in-doubt pending records left by a mid-commit crash: ask each one's
   * TM whether it committed, then promote it to committed or drop it. Without a
   * resolver (e.g. a direct unit test) the records are left untouched.
   */
  private async recover(pendingStates: readonly PendingTransactionState<T>[]): Promise<void> {
    if (this.resolveStatus === undefined) return;
    for (const pending of pendingStates) {
      const manager = pending.transactionManager;
      // If this resource is the TM, read its own commit record directly — routing
      // a status query to ourselves while activating would deadlock.
      const isSelf =
        manager !== undefined &&
        manager.grainId.equals(this.grainId) &&
        manager.stateName === this.stateName;
      const committed =
        manager !== undefined &&
        (isSelf
          ? this.status(pending.transactionId)
          : await this.resolveStatus(manager, pending.transactionId));
      if (committed) {
        this.etag = await this.storage.store(
          this.stateName,
          this.grainId,
          this.etag,
          this.metadata,
          [],
          pending.sequenceId,
          undefined,
        );
        this.committed = pending.state;
        this.committedSequenceId = pending.sequenceId;
      } else {
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
    }
  }

  private enlist(tx: TransactionInfo, reads: number, writes: number): void {
    const existing = tx.participants.get(this.key);
    if (existing === undefined) {
      tx.participants.set(this.key, {
        id: this.participantId,
        participant: this,
        access: { reads, writes },
      });
    } else {
      existing.access.reads += reads;
      existing.access.writes += writes;
    }
  }
}
