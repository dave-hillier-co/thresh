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
}
