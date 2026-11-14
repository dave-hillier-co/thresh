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
   * Orleans `PrepareTimeout`: the longest a TM that has prepared its own
   * write waits for the commit to be recorded before it decides abort.
   */
  readonly prepareTimeoutMs: number;
  /**
   * Orleans `RemoteTransactionPingFrequency`: how often a prepared participant
   * that has not heard the outcome asks its TM.
   */
  readonly remoteTransactionPingFrequencyMs: number;
  /**
   * Initial delay before the confirmation worker retries an in-doubt pending
   * record whose TM could not be reached, or had not decided yet. Doubles on
   * each further unresolved attempt, capped at `confirmationMaxIntervalMs`.
   */
  readonly confirmationIntervalMs: number;
  /** Cap on the confirmation worker's backoff delay, in milliseconds. */
  readonly confirmationMaxIntervalMs: number;
}

export const DEFAULT_LOCK_TIMEOUT_MS = 8_000;
export const DEFAULT_LOCK_ACQUIRE_TIMEOUT_MS = 10_000;
export const DEFAULT_PREPARE_TIMEOUT_MS = 20_000;
export const DEFAULT_REMOTE_TRANSACTION_PING_FREQUENCY_MS = 60_000;
export const DEFAULT_CONFIRMATION_INTERVAL_MS = 1_000;
export const DEFAULT_CONFIRMATION_MAX_INTERVAL_MS = 60_000;

export const defaultTransactionsOptions: TransactionsOptions = {
  lockTimeoutMs: DEFAULT_LOCK_TIMEOUT_MS,
  lockAcquireTimeoutMs: DEFAULT_LOCK_ACQUIRE_TIMEOUT_MS,
  prepareTimeoutMs: DEFAULT_PREPARE_TIMEOUT_MS,
  remoteTransactionPingFrequencyMs: DEFAULT_REMOTE_TRANSACTION_PING_FREQUENCY_MS,
  confirmationIntervalMs: DEFAULT_CONFIRMATION_INTERVAL_MS,
  confirmationMaxIntervalMs: DEFAULT_CONFIRMATION_MAX_INTERVAL_MS,
};
