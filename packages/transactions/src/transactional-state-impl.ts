import type { GrainId } from "@thresh/core/grain-id";
import type { PerformReadOptions, TransactionalState } from "@thresh/core/transactional-state";
import type {
  AccessCounter,
  DeactivationBlocker,
  ParticipantId,
  TransactionalStatus,
  TransactionInfo,
  TransactionManager,
  TransactionParticipant,
} from "@thresh/core/transaction-info";
import type { TransactionProtocol } from "@thresh/core/transaction-resource";
import type { TransactionalStateStorage } from "@thresh/core/transactional-storage";
import { requireTransaction } from "@thresh/runtime/invocation-context";
import { systemTimeProvider, type TimeProvider } from "@thresh/core/time-provider";
import { TransactionReadOnlyViolatedError } from "@thresh/core/errors";
import { TransactionQueue } from "@thresh/transactions/transaction-queue";
import {
  defaultTransactionsOptions,
  type TransactionsOptions,
} from "@thresh/transactions/transactions-options";

const clone = <T>(value: T): T =>
  typeof structuredClone === "function"
    ? structuredClone(value)
    : (JSON.parse(JSON.stringify(value)) as T);

/**
 * A transactional-state resource (Orleans `TransactionalState<TState>`) for one
 * named state on a grain, with both protocol roles Orleans registers beside it:
 * the participant (`TransactionalResource`) and the transaction manager
 * (`TransactionManager`). All three are thin fronts on one
 * {@link TransactionQueue}, which holds the lock, the commit queue and the
 * durable storage batches.
 *
 * `protocol` is how this resource sends messages to the others (its votes and
 * pings to a TM; as TM, its confirmations and cancellations). The host wires it
 * to the dispatcher.
 */
export class TransactionalStateImpl<T>
  implements TransactionalState<T>, TransactionParticipant, TransactionManager, DeactivationBlocker
{
  private readonly key: string;
  private readonly queue: TransactionQueue<T>;

  constructor(
    private readonly stateName: string,
    private readonly grainId: GrainId,
    initial: () => T,
    storage: TransactionalStateStorage,
    protocol?: TransactionProtocol,
    options?: Partial<TransactionsOptions>,
    time: TimeProvider = systemTimeProvider,
  ) {
    this.key = `${grainId.toString()}/${stateName}`;
    this.queue = new TransactionQueue<T>({
      resource: this.participantId,
      storage,
      initial,
      copy: clone,
      options: { ...defaultTransactionsOptions, ...options },
      time,
      protocol,
    });
  }

  private get participantId(): ParticipantId {
    return { grainId: this.grainId, stateName: this.stateName };
  }

  /** Load the committed version and resume what storage left in flight, before `onActivate`. */
  async load(): Promise<void> {
    await this.queue.load();
  }

  /** Stop every worker and timer. Call on deactivation. */
  unload(): void {
    this.queue.unload();
  }

  /**
   * Orleans `ActivationLifetime.BlockDeactivation`: while this resource holds
   * a transaction lock, has a transaction in its commit queue, or is still
   * confirming a commit, its activation's deactivation is delayed, for at most
   * the activation's five-second bound.
   */
  get blocksDeactivation(): boolean {
    return this.queue.blocksDeactivation;
  }

  async performRead<R>(read: (state: T) => R, options?: PerformReadOptions): Promise<R> {
    const tx = requireTransaction();
    // `UseExclusiveLock` (Orleans): take a write lock up front instead of a
    // shared read lock, so a later read-then-write on this same grain in this
    // transaction serializes with contenders rather than racing a read-to-write
    // upgrade against them.
    return this.queue.enterLock(
      tx,
      tx.participants.get(this.key)?.access,
      true,
      options?.exclusive === true,
      (state) => {
        this.enlist(tx, 1, 0);
        return read(state);
      },
    );
  }

  async performUpdate<R>(update: (state: T) => R): Promise<R> {
    const tx = requireTransaction();
    // A read-only transaction may never write, regardless of contention —
    // reject before touching the lock (Orleans: OrleansReadOnlyViolatedException).
    if (tx.readOnly) throw new TransactionReadOnlyViolatedError(tx.id);
    return this.queue.enterLock(
      tx,
      tx.participants.get(this.key)?.access,
      false,
      false,
      (state) => {
        this.enlist(tx, 0, 1);
        return update(state);
      },
    );
  }

  // ------------------------------------------- participant (Orleans TransactionalResource)

  commitReadOnly(
    transactionId: string,
    accessCount: AccessCounter,
    timeStamp: number,
  ): Promise<TransactionalStatus> {
    return this.queue.commitReadOnly(transactionId, accessCount, timeStamp);
  }

  prepare(
    transactionId: string,
    accessCount: AccessCounter,
    timeStamp: number,
    transactionManager: ParticipantId,
  ): Promise<void> {
    return this.queue.notifyOfPrepare(transactionId, accessCount, timeStamp, transactionManager);
  }

  async abort(transactionId: string): Promise<void> {
    await this.queue.ready();
    this.queue.rollback(transactionId);
  }

  async cancel(
    transactionId: string,
    timeStamp: number,
    status: TransactionalStatus,
  ): Promise<void> {
    await this.queue.notifyOfCancel(transactionId, timeStamp, status);
  }

  async confirm(transactionId: string, timeStamp: number): Promise<void> {
    await this.queue.notifyOfConfirm(transactionId, timeStamp);
  }

  // ---------------------------------------------- manager (Orleans TransactionManager)

  prepareAndCommit(
    transactionId: string,
    accessCount: AccessCounter,
    timeStamp: number,
    writeParticipants: ParticipantId[],
    totalParticipants: number,
  ): Promise<TransactionalStatus> {
    return this.queue.prepareAndCommit(
      transactionId,
      accessCount,
      timeStamp,
      writeParticipants,
      totalParticipants,
    );
  }

  prepared(
    transactionId: string,
    timeStamp: number,
    _participant: ParticipantId,
    status: TransactionalStatus,
  ): Promise<void> {
    return this.queue.notifyOfPrepared(transactionId, timeStamp, status);
  }

  async ping(transactionId: string, timeStamp: number, participant: ParticipantId): Promise<void> {
    await this.queue.notifyOfPing(transactionId, timeStamp, participant);
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
