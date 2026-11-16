import { InconsistentStateError } from "@thresh/core/errors";
import type { TimeProvider } from "@thresh/core/time-provider";
import {
  sameParticipant,
  type AccessCounter,
  type ParticipantId,
  type TransactionalStatus,
  type TransactionInfo,
} from "@thresh/core/transaction-info";
import type {
  TransactionProtocol,
  TransactionResourceMethod,
} from "@thresh/core/transaction-resource";
import type {
  TransactionalStateStorage,
  TransactionalStorageLoadResponse,
} from "@thresh/core/transactional-storage";
import { BatchWorker } from "@thresh/transactions/batch-worker";
import { ConfirmationWorker } from "@thresh/transactions/confirmation-worker";
import { ReaderWriterLock } from "@thresh/transactions/reader-writer-lock";
import { StorageBatch } from "@thresh/transactions/storage-batch";
import { deferred, TransactionRecord } from "@thresh/transactions/transaction-record";
import type { TransactionsOptions } from "@thresh/transactions/transactions-options";

/** `prepared` votes that arrived before the TM's own `prepareAndCommit`. */
interface PreparedMessages {
  count: number;
  status: TransactionalStatus;
}

/** What a {@link TransactionQueue} needs from the resource that owns it. */
export interface TransactionQueueOptions<T> {
  readonly resource: ParticipantId;
  readonly storage: TransactionalStateStorage;
  readonly initial: () => T;
  readonly copy: (state: T) => T;
  readonly options: TransactionsOptions;
  readonly time: TimeProvider;
  readonly protocol: TransactionProtocol | undefined;
}

/**
 * The commit protocol of one transactional resource (Orleans
 * `TransactionQueue<TState>`, with its `ReadWriteLock`, `StorageBatch` and
 * `ConfirmationWorker`). A transaction first holds the resource's lock; once
 * its role is known it leaves the lock for the commit queue, where records
 * commit strictly in order:
 *
 * - Every transaction's state is built on the newest pending one
 *   ({@link getMostRecentState}), so the pending records form a chain.
 * - The storage worker commits the ready records at the bottom of the queue in
 *   one storage batch; as TM it writes the commit record in the same batch.
 * - A participant's vote goes to its TM only when its record reaches the
 *   bottom, and it then pings the TM until the outcome arrives.
 * - An abort removes the record and, by cascade, every later record built on
 *   it ({@link abortCommits}).
 * - On load, prepared records left in storage are queued again ahead of new
 *   work, and the commit records the TM still holds are confirmed again.
 */
export class TransactionQueue<T> {
  readonly lock: ReaderWriterLock;
  private readonly resource: ParticipantId;
  private readonly storage: TransactionalStateStorage;
  private readonly initial: () => T;
  private readonly copy: (state: T) => T;
  private readonly options: TransactionsOptions;
  private readonly time: TimeProvider;
  private readonly protocol: TransactionProtocol | undefined;
  private readonly storageWorker: BatchWorker;
  private readonly confirmationWorker: ConfirmationWorker<T>;

  /** Transactions holding the lock whose role is not known yet (Orleans' lock group). */
  private readonly active = new Map<string, TransactionRecord<T>>();
  private commitQueue: TransactionRecord<T>[] = [];
  private readonly unprocessedPreparedMessages = new Map<string, PreparedMessages>();
  private storageBatch!: StorageBatch<T>;
  private stableState!: T;
  private stableSequenceNumber = 0;
  private currentReadyTask: Promise<void> = Promise.resolve();
  /** Whether `currentReadyTask` has completed successfully. */
  private isReady = false;
  private unloaded = false;

  constructor(config: TransactionQueueOptions<T>) {
    this.resource = config.resource;
    this.storage = config.storage;
    this.initial = config.initial;
    this.copy = config.copy;
    this.options = config.options;
    this.time = config.time;
    this.protocol = config.protocol;
    this.lock = new ReaderWriterLock(config.time, {
      lockTimeoutMs: config.options.lockTimeoutMs,
      // Orleans `BreakLock` → `NotifyOfAbort` on an undetermined record: nobody
      // to tell; the agent finds out when the lock fails validation.
      onBreak: (transactionId) => this.active.delete(transactionId),
    });
    this.storageWorker = new BatchWorker(() => this.storageWork(), config.time);
    this.confirmationWorker = new ConfirmationWorker<T>(
      config.resource,
      config.options.confirmationRetryDelayMs,
      config.protocol,
      () => this.storageBatch,
      () => this.storageWorker.notify(),
      config.time,
    );
  }

  /**
   * Orleans `ActivationLifetime.BlockDeactivation`: a transaction holds the
   * lock, waits in the commit queue, or is still being confirmed.
   */
  get blocksDeactivation(): boolean {
    return (
      this.lock.isHeld ||
      this.active.size > 0 ||
      this.commitQueue.length > 0 ||
      this.confirmationWorker.size > 0
    );
  }

  /** Commit records this TM still holds in memory (they are collected once confirmed). */
  get commitRecordCount(): number {
    return Object.keys(this.storageBatch.metadata.commitRecords).length;
  }

  // ---------------------------------------------------------------- lifecycle

  /** Load from storage (Orleans `NotifyOfRestore`). */
  async load(): Promise<void> {
    try {
      await this.ready();
    } finally {
      this.readyTask = this.restore();
    }
    await this.readyTask;
  }

  /** The activation is going away: no further work, and no timer left behind. */
  unload(): void {
    this.unloaded = true;
    this.storageWorker.dispose();
    this.confirmationWorker.stop();
    this.lock.dispose();
  }

  /**
   * Wait until the queue can take requests (Orleans `TransactionQueue.Ready`).
   * Like awaiting a completed .NET task, it does not yield when the queue is
   * already ready, so a caller's next step runs in the same tick.
   */
  ready(): Promise<void> | undefined {
    if (this.isReady) return undefined;
    return (async () => {
      try {
        await this.readyTask;
      } catch (error) {
        await this.abortAndRestore("unknownException", error, false);
      }
    })();
  }

  private set readyTask(task: Promise<void>) {
    this.isReady = false;
    this.currentReadyTask = task;
    task.then(
      () => {
        if (this.currentReadyTask === task) this.isReady = true;
      },
      () => undefined,
    );
  }

  private get readyTask(): Promise<void> {
    return this.currentReadyTask;
  }

  private async restore(): Promise<void> {
    const response = (await this.storage.load(
      this.resource.stateName,
      this.resource.grainId,
    )) as TransactionalStorageLoadResponse<T>;
    this.storageBatch?.fail(new Error("transactional state restored from storage"));
    this.storageBatch = StorageBatch.fromLoad(response);
    this.stableState =
      response.committedSequenceId === 0 ? this.initial() : response.committedState;
    this.stableSequenceNumber = response.committedSequenceId;

    // Resume prepared transactions (as a participant), ahead of any new work.
    const pending = [...response.pendingStates].sort((a, b) => a.sequenceId - b.sequenceId);
    for (const prepared of pending) {
      if (
        prepared.sequenceId <= response.committedSequenceId ||
        prepared.transactionManager === undefined
      ) {
        continue;
      }
      const record = new TransactionRecord<T>();
      record.role = "remoteCommit";
      record.transactionId = prepared.transactionId;
      record.timestamp = prepared.timeStamp;
      record.priority = prepared.timeStamp;
      record.state = prepared.state;
      record.sequenceNumber = prepared.sequenceId;
      record.transactionManager = prepared.transactionManager;
      record.prepareIsPersisted = true;
      // Orleans `default(DateTime)`: the vote is in the past, so ping at once.
      record.lastSent = Number.NEGATIVE_INFINITY;
      record.numberWrites = 1;
      this.commitQueue.push(record);
      this.stableSequenceNumber = prepared.sequenceId;
    }

    // Resume committed transactions (as TM).
    for (const [transactionId, record] of Object.entries(
      this.storageBatch.metadata.commitRecords,
    )) {
      this.confirmationWorker.add(transactionId, record.timeStamp, record.writeParticipants);
    }

    this.storageWorker.notify();
  }

  // ------------------------------------------------------------ reads, writes

  /**
   * The state a transaction entering the lock builds on: the newest pending
   * record's, or the committed state when nothing is pending (Orleans
   * `GetMostRecentState`).
   */
  getMostRecentState(): { state: T; sequenceNumber: number } {
    const last = this.commitQueue.at(-1);
    if (last === undefined) {
      return { state: this.stableState, sequenceNumber: this.stableSequenceNumber };
    }
    return { state: last.state as T, sequenceNumber: last.sequenceNumber };
  }

  /**
   * Run `operation` under the lock (Orleans `ReadWriteLock.EnterLock` with
   * `TransactionalState.PerformRead`/`PerformUpdate`'s task): link the
   * transaction to the most recent state and, on its first write, give it a
   * copy with the next sequence number.
   */
  async enterLock<R>(
    tx: TransactionInfo,
    counter: AccessCounter | undefined,
    isRead: boolean,
    exclusive: boolean,
    operation: (state: T) => R,
  ): Promise<R> {
    const restoring = this.ready();
    if (restoring !== undefined) await restoring;
    await this.lock.enter(
      tx.id,
      tx.timeStamp,
      isRead && !exclusive ? "read" : "write",
      { timeoutMs: this.options.lockAcquireTimeoutMs },
      { isRead, counter },
    );
    let record = this.active.get(tx.id);
    if (record === undefined) {
      record = new TransactionRecord<T>();
      record.transactionId = tx.id;
      record.priority = tx.timeStamp;
      this.active.set(tx.id, record);
    }
    record.timestamp = tx.timeStamp;
    if (record.state === undefined) {
      const recent = this.getMostRecentState();
      record.state = recent.state;
      record.sequenceNumber = recent.sequenceNumber;
    }
    if (!isRead && !record.hasCopiedState) {
      record.state = this.copy(record.state);
      record.sequenceNumber += 1;
      record.hasCopiedState = true;
    }
    if (isRead) record.numberReads += 1;
    else record.numberWrites += 1;
    return operation(record.state);
  }

  /**
   * Orleans `ReadWriteLock.ValidateLock`: the record of a transaction that
   * still holds the lock with exactly `accessCount`; `brokenLock` (with a fresh
   * record) when it holds none, `lockValidationFailed` (rolled back) when the
   * counts differ.
   */
  private validateLock(
    transactionId: string,
    accessCount: AccessCounter,
  ): [TransactionalStatus, TransactionRecord<T>] {
    const validation = this.lock.validate(transactionId, accessCount);
    const record = this.active.get(transactionId);
    if (validation === "brokenLock" || record === undefined) {
      const fresh = new TransactionRecord<T>();
      fresh.transactionId = transactionId;
      return ["brokenLock", fresh];
    }
    if (validation === "lockValidationFailed") {
      this.rollback(transactionId);
      return ["lockValidationFailed", record];
    }
    return ["ok", record];
  }

  /** Release the lock and roll back (Orleans `ReadWriteLock.Rollback`). */
  rollback(transactionId: string): void {
    this.active.delete(transactionId);
    this.lock.release(transactionId);
  }

  // -------------------------------------------------------- protocol messages

  /** The TM's `prepareAndCommit` (Orleans `TransactionManager.PrepareAndCommit`). */
  async prepareAndCommit(
    transactionId: string,
    accessCount: AccessCounter,
    timeStamp: number,
    writeParticipants: ParticipantId[],
    totalParticipants: number,
  ): Promise<TransactionalStatus> {
    const [status, record] = this.validateLock(transactionId, accessCount);
    record.timestamp = timeStamp;
    record.role = "localCommit";
    record.transactionManager = this.resource;
    record.waitCount = totalParticipants - 1;
    record.waitingSince = this.time.now();
    record.writeParticipants = writeParticipants;
    const answer = deferred<TransactionalStatus>();
    record.promiseForTA = answer;
    if (status !== "ok") await this.notifyOfAbort(record, status);
    else await this.exitLock(record);
    return answer.promise;
  }

  /** A participant's `commitReadOnly` (Orleans `TransactionalResource.CommitReadOnly`). */
  async commitReadOnly(
    transactionId: string,
    accessCount: AccessCounter,
    timeStamp: number,
  ): Promise<TransactionalStatus> {
    const [status, record] = this.validateLock(transactionId, accessCount);
    record.timestamp = timeStamp;
    record.role = "readOnly";
    const answer = deferred<TransactionalStatus>();
    record.promiseForTA = answer;
    if (status !== "ok") await this.notifyOfAbort(record, status);
    else await this.exitLock(record);
    return answer.promise;
  }

  /** A participant's `prepare` (Orleans `TransactionQueue.NotifyOfPrepare`). */
  async notifyOfPrepare(
    transactionId: string,
    accessCount: AccessCounter,
    timeStamp: number,
    transactionManager: ParticipantId,
  ): Promise<void> {
    const [status, record] = this.validateLock(transactionId, accessCount);
    record.timestamp = timeStamp;
    record.role = "remoteCommit";
    record.transactionManager = transactionManager;
    record.lastSent = undefined;
    record.prepareIsPersisted = false;
    if (status !== "ok") await this.notifyOfAbort(record, status);
    else await this.exitLock(record);
  }

  /**
   * The record's role is known: it leaves the lock for the commit queue
   * (Orleans `ReadWriteLock.LockExits` → `EnqueueCommit`). Queued before the
   * lock is released, so the next transaction the release admits builds on it.
   */
  private async exitLock(record: TransactionRecord<T>): Promise<void> {
    this.active.delete(record.transactionId);
    const enqueued = this.enqueueCommit(record);
    this.lock.release(record.transactionId);
    await enqueued;
    this.storageWorker.notify();
  }

  /** Orleans `TransactionQueue.EnqueueCommit`. */
  private async enqueueCommit(record: TransactionRecord<T>): Promise<void> {
    try {
      this.commitQueue.push(record);
      switch (record.role) {
        case "readOnly":
          break;
        case "localCommit": {
          // Votes that arrived ahead of the agent's request.
          const early = this.unprocessedPreparedMessages.get(record.transactionId);
          if (early !== undefined) {
            this.unprocessedPreparedMessages.delete(record.transactionId);
            if (early.status === "ok") record.waitCount -= early.count;
            else await this.abortCommits(early.status, this.commitQueue.length - 1);
          }
          break;
        }
        case "remoteCommit": {
          if (record.numberWrites > 0) {
            this.storageBatch.prepare(
              record.sequenceNumber,
              record.transactionId,
              record.timestamp,
              record.transactionManager,
              record.state as T,
            );
          } else {
            this.storageBatch.read(record.timestamp);
          }
          this.storageBatch.followUpAction(() => {
            record.prepareIsPersisted = true;
          });
          break;
        }
        default:
          throw new Error(`${record.role} is not a supported commit role`);
      }
    } catch (error) {
      await this.notifyOfAbort(record, "unknownException", error);
    }
  }

  /** The TM hears a participant's vote (Orleans `TransactionQueue.NotifyOfPrepared`). */
  async notifyOfPrepared(
    transactionId: string,
    _timeStamp: number,
    status: TransactionalStatus,
  ): Promise<void> {
    const pos = this.find(transactionId);
    if (pos !== -1) {
      const entry = this.commitQueue[pos]!;
      if (entry.role !== "localCommit") {
        throw new Error(`wrong commit type for a prepared vote: ${entry.role}`);
      }
      if (status === "ok") {
        entry.waitCount -= 1;
        this.storageWorker.notify();
      } else {
        await this.abortCommits(status, pos);
      }
      return;
    }
    // Ahead of the agent's `prepareAndCommit`: remember it.
    let early = this.unprocessedPreparedMessages.get(transactionId);
    if (early === undefined) {
      early = { count: 0, status: "ok" };
      this.unprocessedPreparedMessages.set(transactionId, early);
    }
    if (status === "ok") early.count += 1;
    else early.status = status;
  }

  /** A prepared participant asks after the outcome (Orleans `TransactionQueue.NotifyOfPing`). */
  async notifyOfPing(
    transactionId: string,
    timeStamp: number,
    participant: ParticipantId,
  ): Promise<void> {
    await this.ready();
    if (this.find(transactionId) !== -1) {
      // Still in the commit queue, so undetermined: the confirm or cancel will follow.
      this.storageWorker.notify();
      return;
    }
    // The port's addition: the agent's `prepareAndCommit` has not reached this
    // TM yet while the transaction still holds its lock here. Presuming an
    // abort now could contradict a commit that request would still make; the
    // lock timeout ends this, and the next ping is then answered.
    if (this.active.has(transactionId)) return;
    if (!this.confirmationWorker.isConfirmed(transactionId)) {
      // Never heard of it, so it must have aborted.
      await this.send(participant, "cancel", [transactionId, timeStamp, "presumedAbort"]);
    }
  }

  /** The TM's `confirm` reaches a participant (Orleans `TransactionQueue.NotifyOfConfirm`). */
  async notifyOfConfirm(transactionId: string, _timeStamp: number): Promise<void> {
    await this.ready();
    const pos = this.find(transactionId);
    if (pos === -1) return; // already confirmed
    const entry = this.commitQueue[pos]!;
    if (entry.role !== "remoteCommit") {
      throw new Error(`wrong commit type for a confirm: ${entry.role}`);
    }
    entry.confirmationResponsePromise ??= deferred<void>();
    this.storageWorker.notify();
    await entry.confirmationResponsePromise.promise;
  }

  /** The TM's `cancel` reaches a participant (Orleans `TransactionQueue.NotifyOfCancel`). */
  async notifyOfCancel(
    transactionId: string,
    _timeStamp: number,
    status: TransactionalStatus,
  ): Promise<void> {
    await this.ready();
    const pos = this.find(transactionId);
    if (pos === -1) return;
    this.storageBatch.cancel(this.commitQueue[pos]!.sequenceNumber);
    await this.abortCommits(status, pos);
    this.storageWorker.notify();
  }

  /** Orleans `TransactionQueue.NotifyOfAbort`: tell whoever must hear of an abort. */
  private async notifyOfAbort(
    entry: TransactionRecord<T>,
    status: TransactionalStatus,
    exception?: unknown,
  ): Promise<void> {
    switch (entry.role) {
      case "notYetDetermined":
        // Nobody to tell: the agent finds the broken lock when it validates.
        return;
      case "remoteCommit": {
        entry.confirmationResponsePromise?.reject(new Error(`confirm failed: status ${status}`));
        // Too late to abort once the vote went out: the TM decides.
        if (entry.lastSent !== undefined) return;
        this.sendOneWay(entry.transactionManager!, "prepared", [
          entry.transactionId,
          entry.timestamp,
          this.resource,
          status,
        ]);
        return;
      }
      case "localCommit": {
        try {
          await Promise.all(
            entry.writeParticipants
              .filter((p) => !sameParticipant(p, this.resource))
              .map((p) => this.send(p, "cancel", [entry.transactionId, entry.timestamp, status])),
          );
        } catch {
          // A participant that misses the cancel pings, and is presumed aborted.
        }
        this.answerAgent(entry, status, exception);
        return;
      }
      case "readOnly":
        this.answerAgent(entry, status, exception);
        return;
      default:
        throw new Error(`${String(entry.role)} is not a supported commit role`);
    }
  }

  private answerAgent(
    entry: TransactionRecord<T>,
    status: TransactionalStatus,
    exception: unknown,
  ): void {
    if (exception !== undefined) entry.promiseForTA?.reject(exception);
    else entry.promiseForTA?.resolve(status);
  }

  /**
   * Abort the records from `from` up (Orleans `TransactionQueue.AbortCommits`):
   * the first with `status`, every later one, built on it, as a cascading
   * abort; and break the transactions holding the lock, which may have read
   * one of them.
   */
  private async abortCommits(status: TransactionalStatus, from = 0): Promise<void> {
    const aborted = this.commitQueue.splice(from);
    const pending = aborted.map((entry, i) =>
      this.notifyOfAbort(entry, i === 0 ? status : "cascadingAbort"),
    );
    this.abortExecutingTransactions();
    await Promise.all(pending);
  }

  private abortExecutingTransactions(): void {
    this.lock.breakHolders();
  }

  private find(transactionId: string): number {
    return this.commitQueue.findIndex((r) => r.transactionId === transactionId);
  }

  // ----------------------------------------------------------- storage worker

  /** Orleans `TransactionQueue.StorageWork`. */
  private async storageWork(): Promise<void> {
    if (this.unloaded) return;
    let writeAttempted = false;
    try {
      // Count the committable records at the bottom of the queue.
      let committable = 0;
      while (
        committable < this.commitQueue.length &&
        this.commitQueue[committable]!.readyToCommit
      ) {
        committable += 1;
      }
      if (committable > 0) this.collectEventsForBatch(committable);
      else await this.checkProgressOfCommitQueue();

      let sent: StorageBatch<T> | undefined;
      if (this.storageBatch.batchSize > 0) {
        sent = this.storageBatch;
        this.storageBatch = StorageBatch.after(sent);
        try {
          if (await sent.checkStorePreConditions()) {
            // Once the store is attempted its outcome is unknown until it answers.
            writeAttempted = true;
            this.storageBatch.etag = await sent.store(
              this.storage,
              this.resource.stateName,
              this.resource.grainId,
            );
          } else {
            sent.fail(new Error("store preconditions not met"));
            await this.abortAndRestore("commitFailure", undefined, false);
            return;
          }
        } catch (error) {
          sent.fail(error);
          const status: TransactionalStatus =
            error instanceof InconsistentStateError ? "storageConflict" : "unknownException";
          await this.abortAndRestore(status, error, writeAttempted);
          return;
        }
      }

      if (committable > 0) {
        const last = this.commitQueue[committable - 1]!;
        this.stableState = last.state as T;
        this.stableSequenceNumber = last.sequenceNumber;
        this.commitQueue.splice(0, committable);
        this.storageWorker.notify();
      }
      if (sent !== undefined) {
        sent.runFollowUpActions();
        this.storageWorker.notify();
      }
    } catch (error) {
      await this.abortAndRestore("unknownException", error, writeAttempted);
    }
  }

  /** Orleans `TransactionQueue.CollectEventsForBatch`. */
  private collectEventsForBatch(batchSize: number): void {
    for (let i = 0; i < batchSize; i++) {
      const entry = this.commitQueue[i]!;
      switch (entry.role) {
        case "localCommit":
          this.onLocalCommit(entry);
          break;
        case "remoteCommit": {
          const confirmation = entry.confirmationResponsePromise;
          // Without a confirm this is a read-only participant that has voted:
          // nothing to store.
          if (confirmation !== undefined) {
            // Confirm in storage, then answer the TM so it can collect.
            this.storageBatch.confirm(entry.sequenceNumber);
            this.storageBatch.followUpAction(() => confirmation.resolve());
          }
          break;
        }
        case "readOnly": {
          // Store the read time stamp, then answer.
          this.storageBatch.read(entry.timestamp);
          this.storageBatch.followUpAction(() => entry.promiseForTA?.resolve("ok"));
          break;
        }
        default:
          throw new Error(`${entry.role} is not a supported commit role`);
      }
    }
  }

  /** Orleans `TransactionQueue.OnLocalCommit`: the TM's own commit. */
  private onLocalCommit(entry: TransactionRecord<T>): void {
    this.storageBatch.prepare(
      entry.sequenceNumber,
      entry.transactionId,
      entry.timestamp,
      entry.transactionManager,
      entry.state as T,
    );
    this.storageBatch.commit(entry.transactionId, entry.timestamp, entry.writeParticipants);
    this.storageBatch.confirm(entry.sequenceNumber);
    // Once durable, answer the agent.
    this.storageBatch.followUpAction(() => entry.promiseForTA?.resolve("ok"));
    if (entry.writeParticipants.length > 1) {
      // Then confirm the other write participants, and collect.
      this.storageBatch.followUpAction(() =>
        this.confirmationWorker.add(entry.transactionId, entry.timestamp, entry.writeParticipants),
      );
    } else {
      // No other write participant to confirm: collect in the same batch.
      this.storageBatch.collect(entry.transactionId);
    }
  }

  /** Orleans `TransactionQueue.CheckProgressOfCommitQueue`: send votes and pings, time out. */
  private async checkProgressOfCommitQueue(): Promise<void> {
    const bottom = this.commitQueue[0];
    if (bottom === undefined) return;
    const now = this.time.now();
    switch (bottom.role) {
      case "localCommit": {
        const deadline = bottom.waitingSince + this.options.prepareTimeoutMs;
        if (deadline <= now) await this.abortCommits("prepareTimeout");
        else this.storageWorker.notify(deadline);
        break;
      }
      case "remoteCommit": {
        const frequency = this.options.remoteTransactionPingFrequencyMs;
        if (bottom.prepareIsPersisted && bottom.lastSent === undefined) {
          this.sendOneWay(bottom.transactionManager!, "prepared", [
            bottom.transactionId,
            bottom.timestamp,
            this.resource,
            "ok",
          ]);
          bottom.lastSent = now;
          if (bottom.isReadOnly) this.storageWorker.notify();
          else this.storageWorker.notify(now + frequency);
        } else if (!bottom.isReadOnly && bottom.lastSent !== undefined) {
          // Ping, to reactivate a TM that crashed.
          if (bottom.lastSent + frequency <= now) {
            this.sendOneWay(bottom.transactionManager!, "ping", [
              bottom.transactionId,
              bottom.timestamp,
              this.resource,
            ]);
            bottom.lastSent = now;
          }
          this.storageWorker.notify(bottom.lastSent + frequency);
        }
        break;
      }
      default:
        throw new Error(`${bottom.role} is not a supported commit role`);
    }
  }

  /** Orleans `TransactionQueue.AbortAndRestore`. */
  private async abortAndRestore(
    status: TransactionalStatus,
    exception: unknown,
    storageOutcomeInDoubt: boolean,
  ): Promise<void> {
    const task = (this.readyTask = (async () => {
      this.abortExecutingTransactions();
      this.lock.abortWaiters();
      const pending: Promise<void>[] = [];
      for (const entry of this.commitQueue) {
        if (storageOutcomeInDoubt) {
          // The write may have landed: recovery decides, not stale memory.
          this.completeInDoubtEntryLocally(entry, status, exception);
        } else {
          pending.push(this.notifyOfAbort(entry, status, exception));
        }
      }
      this.commitQueue = [];
      await Promise.all(pending);
      await this.restore();
    })());
    await task;
  }

  private completeInDoubtEntryLocally(
    entry: TransactionRecord<T>,
    status: TransactionalStatus,
    exception: unknown,
  ): void {
    switch (entry.role) {
      case "notYetDetermined":
        break;
      case "remoteCommit":
        entry.confirmationResponsePromise?.reject(
          exception ?? new Error(`confirm failed: status ${status}`),
        );
        break;
      case "localCommit":
      case "readOnly":
        this.answerAgent(entry, status, exception);
        break;
    }
  }

  // --------------------------------------------------------------- transport

  private send(
    target: ParticipantId,
    method: TransactionResourceMethod,
    args: readonly unknown[],
  ): Promise<unknown> {
    if (this.protocol === undefined) {
      return Promise.reject(new Error("no transaction protocol to reach another resource"));
    }
    return this.protocol.invoke(target, method, args);
  }

  private sendOneWay(
    target: ParticipantId,
    method: TransactionResourceMethod,
    args: readonly unknown[],
  ): void {
    this.send(target, method, args).catch(() => undefined);
  }
}
