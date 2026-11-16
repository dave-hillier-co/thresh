import type { ParticipantId, TransactionalStatus } from "@thresh/core/transaction-info";

/**
 * The part a resource plays in one transaction's commit (Orleans `CommitRole`).
 * Known only once the agent's (or the TM's) message arrives: `prepareAndCommit`
 * makes it the TM (`localCommit`), `prepare` an ordinary participant
 * (`remoteCommit`), `commitReadOnly` a participant of a read-only transaction.
 */
export type CommitRole = "notYetDetermined" | "readOnly" | "remoteCommit" | "localCommit";

/** A promise with its settlers, settled at most once (.NET `TaskCompletionSource.TrySet*`). */
export interface Deferred<T> {
  readonly promise: Promise<T>;
  resolve(value: T): void;
  reject(error: unknown): void;
}

export function deferred<T>(): Deferred<T> {
  let settled = false;
  let resolveFn!: (value: T) => void;
  let rejectFn!: (error: unknown) => void;
  const promise = new Promise<T>((resolve, reject) => {
    resolveFn = resolve;
    rejectFn = reject;
  });
  // Nobody may be listening (a one-way caller gone, a confirmation abandoned).
  promise.catch(() => undefined);
  return {
    promise,
    resolve: (value) => {
      if (settled) return;
      settled = true;
      resolveFn(value);
    },
    reject: (error) => {
      if (settled) return;
      settled = true;
      rejectFn(error);
    },
  };
}

/**
 * What a resource keeps for each transaction it takes part in (Orleans
 * `TransactionRecord<TState>`): first while the transaction holds the lock,
 * then, once its role is known, in the commit queue until it commits or aborts.
 */
export class TransactionRecord<T> {
  transactionId = "";
  /** The wait-die priority (the transaction's start time stamp). */
  priority = 0;
  /** The transaction's commit time stamp. */
  timestamp = 0;
  numberReads = 0;
  numberWrites = 0;

  /** The state this transaction reads and writes, and that state's sequence number. */
  state: T | undefined = undefined;
  sequenceNumber = 0;
  /** Whether `state` is this transaction's own copy (it has written). */
  hasCopiedState = false;

  role: CommitRole = "notYetDetermined";

  /** The answer to the agent (`readOnly` and `localCommit`). */
  promiseForTA: Deferred<TransactionalStatus> | undefined = undefined;

  /** The TM (`localCommit` and `remoteCommit`). */
  transactionManager: ParticipantId | undefined = undefined;

  /** `localCommit`: the write participants, the votes still awaited, and since when. */
  writeParticipants: ParticipantId[] = [];
  waitCount = 0;
  waitingSince = 0;

  /**
   * `remoteCommit`: when the vote or the last ping went to the TM (undefined
   * before the vote), whether the prepare record is durable, and the answer to
   * the TM's `confirm` once it has arrived.
   */
  lastSent: number | undefined = undefined;
  prepareIsPersisted = false;
  confirmationResponsePromise: Deferred<void> | undefined = undefined;

  /** Whether the record can be committed once every record below it has been. */
  get readyToCommit(): boolean {
    switch (this.role) {
      case "readOnly":
        return true;
      case "localCommit":
        // Every other participant has voted.
        return this.waitCount === 0;
      case "remoteCommit":
        // The TM has confirmed, or this participant only read and has voted.
        return (
          this.confirmationResponsePromise !== undefined ||
          (this.numberWrites === 0 && this.lastSent !== undefined)
        );
      default:
        throw new Error(`${this.role} is not a supported commit role`);
    }
  }

  get isReadOnly(): boolean {
    switch (this.role) {
      case "readOnly":
        return true;
      case "localCommit":
        return false;
      case "remoteCommit":
        return this.numberWrites === 0;
      default:
        throw new Error(`${this.role} is not a supported commit role`);
    }
  }
}
