/**
 * Tunables for the transactions subsystem, mirroring Orleans'
 * `TransactionalStateOptions`. Only the options we exercise are modelled.
 *
 * `lockTimeoutMs` bounds how long a transaction may wait to acquire a state's
 * reader-writer lock before it is aborted with a deadline-exceeded reason.
 * Orleans' default is 8s; we default to 30s to match the value referenced in
 * the durable-transactions roadmap.
 */
export interface TransactionsOptions {
  /** Maximum wait time when acquiring a state's lock, in milliseconds. */
  readonly lockTimeoutMs: number;
  /**
   * Initial delay before the confirmation worker retries an in-doubt pending
   * record whose TM could not be reached during activation `load()`. Doubles
   * on each further failed attempt, capped at `confirmationMaxIntervalMs`.
   */
  readonly confirmationIntervalMs: number;
  /** Cap on the confirmation worker's backoff delay, in milliseconds. */
  readonly confirmationMaxIntervalMs: number;
}

export const DEFAULT_LOCK_TIMEOUT_MS = 30_000;
export const DEFAULT_CONFIRMATION_INTERVAL_MS = 1_000;
export const DEFAULT_CONFIRMATION_MAX_INTERVAL_MS = 60_000;

export const defaultTransactionsOptions: TransactionsOptions = {
  lockTimeoutMs: DEFAULT_LOCK_TIMEOUT_MS,
  confirmationIntervalMs: DEFAULT_CONFIRMATION_INTERVAL_MS,
  confirmationMaxIntervalMs: DEFAULT_CONFIRMATION_MAX_INTERVAL_MS,
};
