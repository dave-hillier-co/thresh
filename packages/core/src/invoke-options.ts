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
   * How this call relates to the ambient transaction (Phase 7,
   * [ADR 0008](../../docs/adr/0008-cross-grain-transactions.md)). Absent means
   * the method is non-transactional: an ambient transaction still flows through
   * to nested calls, but this method's state does not participate.
   */
  transaction?: TransactionOption;
}
