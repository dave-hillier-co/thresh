import type { Duration } from "./duration";
import type { TransactionOption } from "./transaction-info";

/**
 * Per-method invocation flags controlling reentrancy and response handling,
 * mirroring Orleans' InvokeMethodOptions.
 */
export interface InvokeMethodOptions {
  /** May interleave with other read-only turns on the same activation. */
  readOnly?: boolean;
  /** May interleave with any turn on the same activation. */
  alwaysInterleave?: boolean;
  /** Fire-and-forget; resolves once accepted, no response awaited. */
  oneWay?: boolean;
  /**
   * A per-method ceiling on how long the caller waits for a response (Orleans
   * `[ResponseTimeout]`). Overrides any silo-wide default response timeout.
   * The callee is not interrupted when this elapses — only the caller's wait
   * is abandoned, and it rejects with `GrainCallTimeoutError`; the grain's
   * in-flight turn keeps running to completion, since JS has no cooperative
   * cancellation for an already-dispatched call. Ignored on a `oneWay` call.
   */
  responseTimeout?: Duration;
  /**
   * How this call relates to the ambient transaction. Absent means
   * the method is non-transactional: an ambient transaction still flows through
   * to nested calls, but this method's state does not participate.
   */
  transaction?: TransactionOption;
}
