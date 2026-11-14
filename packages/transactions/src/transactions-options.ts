/**
 * Tunables for the transactions subsystem, mirroring Orleans'
 * `TransactionalStateOptions` (`Orleans.Transactions/State/TransactionalStateOptions.cs`).
 * Only the options we exercise are modelled, under Orleans' names and defaults.
 */
export interface TransactionsOptions {
  /**
   * Orleans `LockTimeout`: the longest a group of transactions may hold a
   * state's lock, in milliseconds. When it passes, the holders that have not
   * prepared lose the lock and their tentative state is rolled back, so their
   * later prepare votes no and waiting transactions proceed. Extended whenever
   * another transaction joins the holders, as Orleans extends its lock group's
   * deadline.
   */
  readonly lockTimeoutMs: number;
  /**
   * Orleans `LockAcquireTimeout`: the longest a transaction may wait to
   * acquire a state's lock before it is aborted with a deadline-exceeded reason.
   */
  readonly lockAcquireTimeoutMs: number;
  /**
   * Orleans `PrepareTimeout`: the longest the TM's commit queue waits for the
   * `prepared` votes of a transaction at its head before it aborts it.
   */
  readonly prepareTimeoutMs: number;
  /**
   * Orleans `RemoteTransactionPingFrequency`: how often a prepared participant
   * that has not heard the outcome pings its TM.
   */
  readonly remoteTransactionPingFrequencyMs: number;
  /**
   * Orleans `ConfirmationRetryDelay`: how long the TM waits before it resends
   * a `confirm` a participant did not acknowledge, or retries removing a
   * commit record whose write failed.
   */
  readonly confirmationRetryDelayMs: number;
}

export const DEFAULT_LOCK_TIMEOUT_MS = 8_000;
export const DEFAULT_LOCK_ACQUIRE_TIMEOUT_MS = 10_000;
export const DEFAULT_PREPARE_TIMEOUT_MS = 20_000;
export const DEFAULT_REMOTE_TRANSACTION_PING_FREQUENCY_MS = 60_000;
export const DEFAULT_CONFIRMATION_RETRY_DELAY_MS = 30_000;

export const defaultTransactionsOptions: TransactionsOptions = {
  lockTimeoutMs: DEFAULT_LOCK_TIMEOUT_MS,
  lockAcquireTimeoutMs: DEFAULT_LOCK_ACQUIRE_TIMEOUT_MS,
  prepareTimeoutMs: DEFAULT_PREPARE_TIMEOUT_MS,
  remoteTransactionPingFrequencyMs: DEFAULT_REMOTE_TRANSACTION_PING_FREQUENCY_MS,
  confirmationRetryDelayMs: DEFAULT_CONFIRMATION_RETRY_DELAY_MS,
};
