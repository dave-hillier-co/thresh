/**
 * Signals a recoverable delivery failure — the consumer is deactivating
 * (Orleans `DeactivateOnIdle` inside a fault-injecting `OnNextAsync`) to
 * resume from its own persisted checkpoint rather than have the framework
 * retry-then-skip the event that failed. `resumeToken` is the token the
 * consumer's checkpoint already covers (its `StreamCheckpoint<TState>.RecoveryToken`);
 * `QueuePullingAgent` rewinds the owning queue's cursor there instead of
 * advancing past the failure, so the reactivated consumer sees every event
 * from its checkpoint forward again — a full-cursor rewind rather than a
 * single-event skip. Throw this instead of a plain `Error` from a
 * `STREAM_SUBSCRIPTION_OBSERVER` handler's `onNext` when the fault is one the
 * consumer expects to fully recover from (as opposed to a permanent,
 * non-transient failure, which should just propagate a plain error and let
 * the pulling agent's normal retry-then-skip behavior drop the one event).
 */
export class RecoverableStreamDeliveryError extends Error {
  constructor(
    message: string,
    readonly resumeToken: number,
  ) {
    super(message);
    this.name = "RecoverableStreamDeliveryError";
  }
}
