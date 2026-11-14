import type { GrainId } from "@thresh/core/grain-id";
import type { ParticipantId } from "@thresh/core/transaction-info";
import type {
  PendingTransactionState,
  TransactionalStateMetadata,
  TransactionalStateStorage,
  TransactionalStorageLoadResponse,
} from "@thresh/core/transactional-storage";

/**
 * Accumulates the storage events of a resource's commit queue so they are
 * written in one `store` call (Orleans `StorageBatch<TState>`): prepare
 * records, the read time stamps, cancellations, confirmations, and the TM's
 * commit records being added and collected. Actions to run once the batch is
 * durable are queued alongside.
 */
export class StorageBatch<T> {
  readonly metadata: TransactionalStateMetadata;
  etag: string | undefined;

  // Watermarks for confirm and cancel.
  private confirmUpTo: number;
  private cancelAbove: number;
  private readonly cancelAboveStart: number;

  private readonly prepares = new Map<number, PendingTransactionState<T>>();
  private readonly followUpActions: Array<() => void> = [];
  private readonly failureActions: Array<(error: unknown) => void> = [];
  private readonly storeConditions: Array<() => boolean | Promise<boolean>> = [];

  private total = 0;
  private confirms = 0;

  constructor(
    metadata: TransactionalStateMetadata,
    etag: string | undefined,
    confirmUpTo: number,
    cancelAbove: number,
  ) {
    this.metadata = metadata;
    this.etag = etag;
    this.confirmUpTo = confirmUpTo;
    this.cancelAbove = cancelAbove;
    this.cancelAboveStart = cancelAbove;
  }

  /** The first batch after a load. */
  static fromLoad<T>(response: TransactionalStorageLoadResponse<T>): StorageBatch<T> {
    const last = response.pendingStates.at(-1)?.sequenceId;
    return new StorageBatch<T>(
      {
        timeStamp: response.metadata.timeStamp,
        commitRecords: { ...response.metadata.commitRecords },
      },
      response.etag,
      response.committedSequenceId,
      last ?? response.committedSequenceId,
    );
  }

  /** The batch that follows `previous` while `previous` is being written. */
  static after<T>(previous: StorageBatch<T>): StorageBatch<T> {
    return new StorageBatch<T>(
      previous.metadata,
      previous.etag,
      previous.confirmUpTo,
      previous.cancelAbove,
    );
  }

  get batchSize(): number {
    return this.total;
  }

  async store(
    storage: TransactionalStateStorage,
    stateName: string,
    grainId: GrainId,
  ): Promise<string> {
    const prepares = [...this.prepares.values()].sort((a, b) => a.sequenceId - b.sequenceId);
    return storage.store(
      stateName,
      grainId,
      this.etag,
      this.metadata,
      prepares,
      this.confirms > 0 ? this.confirmUpTo : undefined,
      this.cancelAbove < this.cancelAboveStart ? this.cancelAbove : undefined,
    );
  }

  runFollowUpActions(): void {
    for (const action of this.followUpActions) action();
  }

  /**
   * The batch will never be written (its store failed, or the queue restored
   * from storage without it): tell whoever waits on a follow-up. Orleans drops
   * such a batch silently; a commit-record collection waiting on it would then
   * wait for ever, so the port fails it instead and the waiter retries.
   */
  fail(error: unknown): void {
    for (const action of this.failureActions) action(error);
  }

  read(timestamp: number): void {
    this.total += 1;
    if (this.metadata.timeStamp < timestamp) this.metadata.timeStamp = timestamp;
  }

  prepare(
    sequenceNumber: number,
    transactionId: string,
    timestamp: number,
    transactionManager: ParticipantId | undefined,
    state: T,
  ): void {
    this.total += 1;
    if (this.metadata.timeStamp < timestamp) this.metadata.timeStamp = timestamp;
    this.prepares.set(sequenceNumber, {
      sequenceId: sequenceNumber,
      transactionId,
      timeStamp: timestamp,
      transactionManager,
      state,
    });
    if (this.cancelAbove < sequenceNumber) this.cancelAbove = sequenceNumber;
  }

  cancel(sequenceNumber: number): void {
    this.total += 1;
    this.prepares.delete(sequenceNumber);
    if (this.cancelAbove > sequenceNumber - 1) this.cancelAbove = sequenceNumber - 1;
  }

  confirm(sequenceNumber: number): void {
    this.total += 1;
    this.confirms += 1;
    this.confirmUpTo = sequenceNumber;
    // Drop the prepare records a later confirmed state supersedes.
    for (const id of [...this.prepares.keys()]) {
      if (id < this.confirmUpTo) this.prepares.delete(id);
    }
  }

  commit(transactionId: string, timestamp: number, writeParticipants: ParticipantId[]): void {
    this.total += 1;
    this.metadata.commitRecords[transactionId] = {
      timeStamp: timestamp,
      writeParticipants,
    };
  }

  collect(transactionId: string): void {
    this.total += 1;
    delete this.metadata.commitRecords[transactionId];
  }

  followUpAction(action: () => void): void {
    this.followUpActions.push(action);
  }

  onFailure(action: (error: unknown) => void): void {
    this.failureActions.push(action);
  }

  addStorePreCondition(condition: () => boolean | Promise<boolean>): void {
    this.storeConditions.push(condition);
  }

  async checkStorePreConditions(): Promise<boolean> {
    if (this.storeConditions.length === 0) return true;
    const results = await Promise.all(this.storeConditions.map((c) => c()));
    return results.every((ok) => ok);
  }
}
