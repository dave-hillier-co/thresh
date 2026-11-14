import type { GrainId } from "@thresh/core/grain-id";
import type { PerformReadOptions, TransactionalState } from "@thresh/core/transactional-state";
import type {
  AccessCounter,
  ParticipantId,
  TransactionInfo,
  TransactionOutcome,
  TransactionParticipant,
} from "@thresh/core/transaction-info";
import type {
  PendingTransactionState,
  TransactionalStateMetadata,
  TransactionalStateStorage,
  TransactionalStorageLoadResponse,
} from "@thresh/core/transactional-storage";
import { requireTransaction } from "@thresh/runtime/invocation-context";
import {
  systemTimeProvider,
  type TimeProvider,
  type TimerHandle,
} from "@thresh/core/time-provider";
import { TransactionAbortedError, TransactionReadOnlyViolatedError } from "@thresh/core/errors";
import { ReaderWriterLock } from "@thresh/transactions/reader-writer-lock";
import { EMPTY_METADATA } from "@thresh/transactions/transactional-storage-apply";
import {
  defaultTransactionsOptions,
  type TransactionsOptions,
} from "@thresh/transactions/transactions-options";

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
  /** Set while this resource, as TM, is durably recording the commit. */
  recordingCommit?: boolean;
}

/** Asks the elected TM what it knows of an in-doubt transaction's fate (recovery). */
export type ResolveStatus = (
  manager: ParticipantId,
  transactionId: string,
) => Promise<TransactionOutcome>;

/**
 * A transactional-state resource (Orleans `TransactionalState`) for one named
 * state on a grain. It keeps a committed version with a sequence id and a
 * single per-transaction tentative copy (single because the wait-die lock admits
 * one writer at a time), enlists itself as a participant on first access, and
 * runs the two-phase protocol against a durable {@link TransactionalStateStorage}:
 * `prepare` stages a pending record (with the elected TM), `commit` promotes it,
 * `abort` drops it. The committed version is loaded on activation; an in-doubt
 * pending record left by a mid-commit crash is resolved against its TM. If the
 * TM cannot be reached at that point (e.g. it crashed too), a periodic
 * confirmation worker keeps retrying the recovery query with backoff — rather
 * than leaving the record in-doubt until the next activation — until it
 * resolves or {@link unload} stops it.
 *
 * Because the resource keeps serving while a record is in doubt (which is what
 * that worker is for), a sequence id identifies a record for as long as it is
 * staged: new ones are minted above every pending record this activation knows
 * of, never reusing an id, and each of `commit`/`abort`/resolution only ever
 * promotes or drops the record it was raised for. See
 * {@link nextSequenceId} and {@link resolveOne}.
 */
export class TransactionalStateImpl<T> implements TransactionalState<T>, TransactionParticipant {
  private readonly key: string;
  private readonly lock: ReaderWriterLock;
  private readonly options: TransactionsOptions;
  private readonly time: TimeProvider;
  private committed!: T;
  private committedSequenceId = 0;
  /**
   * The highest sequence id this activation knows to exist in the durable
   * record for a prepared (pending) transaction — the ones `load()` brought in
   * (including any left in doubt and still unresolved) plus our own staged
   * writes. New tentative versions are numbered strictly above it, never reusing
   * a pending record's id: `store` replaces a pending state *by sequence id*,
   * so handing a live transaction an id an unresolved record already owns would
   * overwrite that record, and recovery would then promote whatever record
   * happened to sit at the id it was resolving. Ids may be skipped (a record
   * resolved and dropped frees nothing), which costs nothing: they only have to
   * order promotions.
   */
  private pendingSequenceIdHighWater = 0;
  private etag: string | undefined;
  private metadata: TransactionalStateMetadata = EMPTY_METADATA;
  private tentative: Tentative<T> | undefined;
  /** Pending records whose TM could not be reached during recovery, awaiting the confirmation worker. */
  private readonly inDoubt = new Map<string, PendingTransactionState<T>>();
  private confirmationTimer: TimerHandle | undefined;
  private confirmationDelayMs = 0;
  /** Bounds how long the prepared tentative waits for the coordinator; see {@link armPreparedTimer}. */
  private preparedTimer: TimerHandle | undefined;

  /** Set by {@link unload}: no timer may be armed once the resource is gone. */
  private unloaded = false;

  constructor(
    private readonly stateName: string,
    private readonly grainId: GrainId,
    private readonly initial: () => T,
    private readonly storage: TransactionalStateStorage,
    private readonly resolveStatus?: ResolveStatus,
    options?: Partial<TransactionsOptions>,
    time: TimeProvider = systemTimeProvider,
  ) {
    this.key = `${grainId.toString()}/${stateName}`;
    this.options = { ...defaultTransactionsOptions, ...options };
    this.time = time;
    this.lock = new ReaderWriterLock(time, {
      lockTimeoutMs: this.options.lockTimeoutMs,
      onBreak: (transactionId) => this.onLockBroken(transactionId),
    });
  }

  private get participantId(): ParticipantId {
    return { grainId: this.grainId, stateName: this.stateName };
  }

  /**
   * Persist `metadata` (the current one unless a caller has a newer one to
   * write) with the given pending/commit/abort deltas, updating the etag.
   */
  private async storeState(
    statesToPrepare: PendingTransactionState<T>[],
    commitUpTo?: number,
    abortAfter?: number,
    metadata: TransactionalStateMetadata = this.metadata,
  ): Promise<void> {
    this.etag = await this.storage.store(
      this.stateName,
      this.grainId,
      this.etag,
      metadata,
      statesToPrepare,
      commitUpTo,
      abortAfter,
    );
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
    for (const pending of response.pendingStates) {
      this.pendingSequenceIdHighWater = Math.max(
        this.pendingSequenceIdHighWater,
        pending.sequenceId,
      );
    }
    // No committed transaction yet (a fresh record, or one with only pending
    // states) means the initial value; otherwise the loaded committed snapshot.
    this.committed = response.committedSequenceId === 0 ? this.initial() : response.committedState;
    if (response.pendingStates.length > 0) await this.recover(response.pendingStates);
  }

  /**
   * The next sequence id for a new tentative version: above the last committed
   * version *and* above every pending record this activation knows of, so a live
   * transaction can never take the id of a record that still sits unresolved in
   * the durable record.
   */
  private nextSequenceId(): number {
    return Math.max(this.committedSequenceId, this.pendingSequenceIdHighWater) + 1;
  }

  /**
   * Stop the confirmation worker, if running. Call on deactivation: an idle
   * activation shouldn't keep retrying against a TM forever — the next
   * activation's `load()` re-attempts recovery from scratch.
   */
  unload(): void {
    this.unloaded = true;
    if (this.confirmationTimer !== undefined) {
      this.time.clearTimer(this.confirmationTimer);
      this.confirmationTimer = undefined;
    }
    this.confirmationDelayMs = 0;
    this.clearPreparedTimer();
    this.lock.dispose();
  }

  /**
   * Orleans `ActivationLifetime.BlockDeactivation`: while this resource holds a
   * transaction lock (a tentative or prepared transaction, or a reader) or an
   * inherited prepared record whose outcome it does not know yet, its
   * activation must not be idle-collected, migrated or rebalanced away —
   * deactivating would break the lock, or strand the record until some later
   * activation happens to load it. Both are bounded: an unprepared holder by
   * the hold bound, a prepared one by {@link armPreparedTimer}, an in-doubt
   * record by its TM's eventual answer.
   */
  get blocksDeactivation(): boolean {
    return this.lock.isHeld || this.inDoubt.size > 0;
  }

  async performRead<R>(read: (state: T) => R, options?: PerformReadOptions): Promise<R> {
    const tx = requireTransaction();
    // `UseExclusiveLock` (Orleans): take a write lock up front instead of a
    // shared read lock, so a later read-then-write on this same grain in this
    // transaction serializes with contenders rather than racing a read-to-write
    // upgrade against them.
    const mode = options?.exclusive === true ? "write" : "read";
    await this.lock.enter(
      tx.id,
      tx.timeStamp,
      mode,
      { timeoutMs: this.options.lockAcquireTimeoutMs },
      { isRead: true, counter: tx.participants.get(this.key)?.access },
    );
    this.enlist(tx, 1, 0);
    const state = this.tentative?.transactionId === tx.id ? this.tentative.value : this.committed;
    return read(state);
  }

  async performUpdate<R>(update: (state: T) => R): Promise<R> {
    const tx = requireTransaction();
    // A read-only transaction may never write, regardless of contention —
    // reject before touching the lock (Orleans: OrleansReadOnlyViolatedException).
    if (tx.readOnly) throw new TransactionReadOnlyViolatedError(tx.id);
    await this.lock.enter(
      tx.id,
      tx.timeStamp,
      "write",
      { timeoutMs: this.options.lockAcquireTimeoutMs },
      { isRead: false, counter: tx.participants.get(this.key)?.access },
    );
    this.enlist(tx, 0, 1);
    if (this.tentative?.transactionId !== tx.id) {
      const sequenceId = this.nextSequenceId();
      // Claimed the moment it is minted, whether or not this transaction ends
      // up preparing: an id is never handed out twice.
      this.pendingSequenceIdHighWater = sequenceId;
      this.tentative = {
        transactionId: tx.id,
        value: clone(this.committed),
        sequenceId,
        timeStamp: tx.timeStamp,
        prepared: false,
      };
    }
    return update(this.tentative.value);
  }

  /**
   * Vote on the transaction: validate that this resource still holds its lock
   * with exactly `accessCount` — the reads and writes the coordinator recorded
   * for it — then durably stage its tentative state, recording its TM. Mirrors
   * Orleans `TransactionQueue.NotifyOfPrepare` / `TransactionalResource.CommitReadOnly`,
   * both of which start from `ReaderWriterLock.ValidateLock`: a transaction this
   * activation holds no lock for (`brokenLock` — the lock was broken, or this is
   * a fresh activation that never saw the transaction) or holds with different
   * counts (`lockValidationFailed`, which also rolls it back here) votes no, so
   * the transaction aborts rather than committing without a write it made. A
   * read-only participant is validated the same way; it just has nothing to stage.
   */
  async prepare(
    transactionId: string,
    accessCount: AccessCounter,
    timeStamp: number,
    manager: ParticipantId,
  ): Promise<boolean> {
    const validation = this.lock.validate(transactionId, accessCount);
    if (validation !== "ok") {
      if (validation === "lockValidationFailed") this.rollback(transactionId);
      return false;
    }
    // Prepared: out of the lock group, so the hold bound no longer applies.
    this.lock.exit(transactionId);
    if (this.tentative?.transactionId !== transactionId) return true; // read-only participant
    const pending: PendingTransactionState<T> = {
      sequenceId: this.tentative.sequenceId,
      transactionId,
      timeStamp,
      transactionManager: manager,
      state: this.tentative.value,
    };
    await this.storeState([pending]);
    this.tentative.prepared = true;
    this.armPreparedTimer(transactionId, manager);
    return true;
  }

  private isSelf(manager: ParticipantId): boolean {
    return manager.grainId.equals(this.grainId) && manager.stateName === this.stateName;
  }

  /**
   * Bound how long a prepared tentative waits for its coordinator, so a
   * coordinator that dies after prepare cannot hold this lock (and, through
   * {@link blocksDeactivation}, this activation) for ever:
   *
   * - As TM, after `prepareTimeoutMs` (Orleans `PrepareTimeout`) without a
   *   recorded commit, decide abort: drop its own record and release the lock.
   *   {@link recordCommit} then refuses, and {@link status} answers
   *   `presumedAbort`, so every participant converges on the abort.
   * - As an ordinary participant, every `remoteTransactionPingFrequencyMs`
   *   (Orleans `RemoteTransactionPingFrequency`) ask the TM, and apply a
   *   decided outcome; `undetermined` or an unreachable TM asks again later.
   */
  private armPreparedTimer(transactionId: string, manager: ParticipantId): void {
    this.clearPreparedTimer();
    if (this.unloaded) return;
    const isSelf = this.isSelf(manager);
    if (!isSelf && this.resolveStatus === undefined) return;
    const delay = isSelf
      ? this.options.prepareTimeoutMs
      : this.options.remoteTransactionPingFrequencyMs;
    this.preparedTimer = this.time.setTimer(() => {
      this.preparedTimer = undefined;
      void this.checkPrepared(transactionId, manager);
    }, delay);
  }

  private clearPreparedTimer(): void {
    if (this.preparedTimer === undefined) return;
    this.time.clearTimer(this.preparedTimer);
    this.preparedTimer = undefined;
  }

  private stillPrepared(transactionId: string): boolean {
    return this.tentative?.transactionId === transactionId && this.tentative.prepared;
  }

  private async checkPrepared(transactionId: string, manager: ParticipantId): Promise<void> {
    if (!this.stillPrepared(transactionId)) return;
    let outcome: TransactionOutcome;
    if (this.isSelf(manager)) {
      outcome =
        this.metadata.commitRecords[transactionId] !== undefined
          ? "committed"
          : this.tentative?.recordingCommit === true
            ? "undetermined"
            : "presumedAbort";
    } else {
      try {
        outcome = await this.resolveStatus!(manager, transactionId);
      } catch {
        outcome = "undetermined";
      }
    }
    if (!this.stillPrepared(transactionId)) return;
    try {
      if (outcome === "committed") await this.commit(transactionId);
      else if (outcome === "presumedAbort") await this.abort(transactionId);
    } catch {
      // The durable write failed; try again on the next tick.
    }
    if (this.stillPrepared(transactionId)) this.armPreparedTimer(transactionId, manager);
  }

  /**
   * TM only: durably record the commit (the atomic point), before participants
   * commit. Refused unless this manager still holds the transaction prepared
   * (Orleans `TransactionManager.PrepareAndCommit` validates the TM's own lock
   * first): once it no longer holds it — its lock broke, it aborted, or this is
   * a fresh activation that dropped its own record — it may already have
   * answered {@link status} with `presumedAbort` to a participant that then
   * dropped its write, so recording a commit now would split the outcome.
   */
  async recordCommit(
    transactionId: string,
    timeStamp: number,
    writeParticipants: ParticipantId[],
  ): Promise<void> {
    const tentative = this.tentative;
    if (tentative?.transactionId !== transactionId || !tentative.prepared) {
      throw new TransactionAbortedError(
        transactionId,
        "the transaction manager no longer holds the transaction",
      );
    }
    const metadata: TransactionalStateMetadata = {
      timeStamp: Math.max(this.metadata.timeStamp, timeStamp),
      commitRecords: {
        ...this.metadata.commitRecords,
        [transactionId]: {
          timeStamp,
          writeParticipants: writeParticipants.map((p) => `${p.grainId.toString()}/${p.stateName}`),
        },
      },
    };
    // Durable first, in-memory after. {@link status} reads `this.metadata`, and
    // that is what a participant's recovery query is answered from — so
    // adopting the record before the write lands would have the live manager
    // answer "committed" for a commit that is not durable, while the same
    // record resolves to abort once the TM reactivates over that storage. The
    // agent converts this rejection to `TransactionInDoubtError` and leaves
    // participants prepared precisely so that recovery can resolve the truth.
    tentative.recordingCommit = true;
    try {
      await this.storeState([], undefined, undefined, metadata);
    } finally {
      tentative.recordingCommit = false;
    }
    this.metadata = metadata;
  }

  /**
   * TM only: what this manager knows of the transaction's fate — the query a
   * prepared participant makes (Orleans `TransactionQueue.NotifyOfPing`).
   * `committed` once the commit record is held; `undetermined` while this
   * manager still holds the transaction's own tentative or prepared state (it
   * has not decided — the participant keeps its record and asks again);
   * `presumedAbort` when it holds neither. That last answer is final because
   * {@link recordCommit} refuses a transaction the manager no longer holds.
   */
  status(transactionId: string): TransactionOutcome {
    if (this.metadata.commitRecords[transactionId] !== undefined) return "committed";
    if (this.tentative?.transactionId === transactionId) return "undetermined";
    return "presumedAbort";
  }

  async commit(transactionId: string): Promise<void> {
    try {
      const tentative = this.tentative;
      if (tentative?.transactionId === transactionId) {
        await this.storeState([], tentative.sequenceId);
        this.committed = tentative.value;
        this.committedSequenceId = tentative.sequenceId;
        this.tentative = undefined;
        this.clearPreparedTimer();
      } else {
        // A record this activation inherited in doubt (it replaced the one
        // that prepared): the coordinator commits only after the TM recorded
        // the commit, so this is the decision itself — apply it now rather
        // than waiting for the confirmation worker to learn it.
        const inDoubt = this.inDoubt.get(transactionId);
        if (inDoubt !== undefined) await this.applyOutcome(inDoubt, "committed");
      }
    } finally {
      // Always release, even if the durable write threw: an unreleased lock
      // would deadlock this grain state's ReaderWriterLock permanently.
      this.lock.release(transactionId);
    }
  }

  /**
   * The hold bound broke `transactionId`'s lock (it had not prepared): roll back
   * its tentative state, so a later prepare finds nothing to stage and votes no.
   */
  private onLockBroken(transactionId: string): void {
    if (this.tentative?.transactionId === transactionId && !this.tentative.prepared) {
      this.tentative = undefined;
    }
  }

  /** Drop an unprepared transaction's tentative state and its lock (Orleans `ReaderWriterLock.Rollback`). */
  private rollback(transactionId: string): void {
    if (this.tentative?.transactionId === transactionId && !this.tentative.prepared) {
      this.tentative = undefined;
    }
    this.lock.release(transactionId);
  }

  async abort(transactionId: string): Promise<void> {
    try {
      const tentative = this.tentative;
      if (tentative?.transactionId === transactionId) {
        if (tentative.prepared) {
          // Drop this transaction's staged record and nothing else: an abort
          // discards its own write, never one belonging to a transaction whose
          // fate is still open — an unresolved record below this id must stay
          // for recovery to resolve, and dropping it would lose that write even
          // if its TM reports it committed. (`abortAfter` drops everything with
          // a *strictly larger* id, so our own id is the threshold.)
          await this.storeState([], undefined, tentative.sequenceId - 1);
        }
        this.tentative = undefined;
        this.clearPreparedTimer();
      } else {
        // An inherited in-doubt record: the coordinator aborts only before any
        // commit was recorded, so the record can go.
        const inDoubt = this.inDoubt.get(transactionId);
        if (inDoubt !== undefined) await this.applyOutcome(inDoubt, "presumedAbort");
      }
    } finally {
      // Always release, even if the durable write threw: an unreleased lock
      // would deadlock this grain state's ReaderWriterLock permanently.
      this.lock.release(transactionId);
    }
  }

  /**
   * Resolve in-doubt pending records left by a mid-commit crash: ask each one's
   * TM whether it committed, then promote it to committed or drop it. Without a
   * resolver (e.g. a direct unit test) the records are left untouched. A record
   * whose TM cannot be reached (the TM itself crashed) is queued for the
   * confirmation worker instead of failing `load()`.
   */
  private async recover(pendingStates: readonly PendingTransactionState<T>[]): Promise<void> {
    if (this.resolveStatus === undefined) return;
    for (const pending of pendingStates) await this.resolveOne(pending);
    if (this.inDoubt.size > 0) this.scheduleConfirmationWorker();
  }

  /**
   * Attempt to resolve a single in-doubt record. Returns whether it resolved
   * (committed or aborted, either way persisted); a `false` return means the TM
   * is unreachable or has not decided yet (`undetermined`), and the record
   * remains queued in {@link inDoubt} for the confirmation worker. Only a
   * `presumedAbort` answer drops the record: an undecided TM may still commit.
   */
  private async resolveOne(pending: PendingTransactionState<T>): Promise<boolean> {
    const manager = pending.transactionManager;
    // If this resource is the TM, read its own commit record directly — routing
    // a status query to ourselves while activating would deadlock. A record of
    // its own found here was left by an earlier activation, so this one does
    // not hold the transaction and `status` answers committed or presumed
    // abort, never undetermined; `recordCommit` then refuses a late commit.
    const isSelf =
      manager !== undefined &&
      manager.grainId.equals(this.grainId) &&
      manager.stateName === this.stateName;
    let outcome: TransactionOutcome = "presumedAbort";
    if (manager !== undefined && !isSelf) {
      try {
        outcome = await this.resolveStatus!(manager, pending.transactionId);
      } catch {
        // TM unreachable (e.g. crashed too): stay in-doubt for the confirmation worker.
        outcome = "undetermined";
      }
    } else if (isSelf) {
      outcome = this.status(pending.transactionId);
    }
    if (outcome === "undetermined") {
      this.inDoubt.set(pending.transactionId, pending);
      return false;
    }
    await this.applyOutcome(pending, outcome);
    return true;
  }

  /** Promote (`committed`) or drop (`presumedAbort`) one in-doubt record, and forget it. */
  private async applyOutcome(
    pending: PendingTransactionState<T>,
    outcome: "committed" | "presumedAbort",
  ): Promise<void> {
    if (outcome === "committed") {
      // Promote only a record that is still above the committed version. One at
      // or below it has already been superseded — a later transaction's commit
      // promoted past it and took its place, or dropped it — so promoting it
      // would roll the committed version back to an older write and leave this
      // activation reporting a state the durable record does not hold.
      if (pending.sequenceId > this.committedSequenceId) {
        await this.storeState([], pending.sequenceId);
        this.committed = pending.state;
        this.committedSequenceId = pending.sequenceId;
      }
    } else {
      // Drop the record this resolution was raised for, and nothing below it:
      // lower ids belong to other transactions whose fates are still open, and
      // dropping one would lose its write even if its TM reports it committed.
      // (`abortAfter` drops everything with a *strictly larger* id, so this
      // record's own id is the threshold.)
      await this.storeState([], undefined, pending.sequenceId - 1);
    }
    this.inDoubt.delete(pending.transactionId);
  }

  /** Schedule the next confirmation-worker pass, backing off exponentially up to the configured cap. */
  private scheduleConfirmationWorker(): void {
    if (this.unloaded || this.confirmationTimer !== undefined) return; // gone, or already scheduled
    const base = this.options.confirmationIntervalMs;
    const max = this.options.confirmationMaxIntervalMs;
    this.confirmationDelayMs = Math.min(
      max,
      this.confirmationDelayMs === 0 ? base : this.confirmationDelayMs * 2,
    );
    this.confirmationTimer = this.time.setTimer(() => {
      this.confirmationTimer = undefined;
      void this.runConfirmationPass();
    }, this.confirmationDelayMs);
  }

  /** Re-query the TM for every still in-doubt record; reschedule while any remain unresolved. */
  private async runConfirmationPass(): Promise<void> {
    try {
      for (const pending of [...this.inDoubt.values()]) await this.resolveOne(pending);
    } catch {
      // A resolution's durable write can fail (an etag conflict with a live
      // activation, or any provider error). Letting it escape would make it an
      // unhandled rejection and, worse, skip the reschedule below — leaving
      // every still in-doubt record with no timer behind it for the life of
      // the activation. Swallow it and retry on the backoff instead: the
      // record that failed is still queued in `inDoubt`, so the next pass
      // resolves it as soon as the store recovers.
    }
    if (this.inDoubt.size > 0) {
      this.scheduleConfirmationWorker();
    } else {
      this.confirmationDelayMs = 0;
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
