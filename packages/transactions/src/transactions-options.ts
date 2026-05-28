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
}

export const DEFAULT_LOCK_TIMEOUT_MS = 30_000;

export const defaultTransactionsOptions: TransactionsOptions = {
  lockTimeoutMs: DEFAULT_LOCK_TIMEOUT_MS,
};
