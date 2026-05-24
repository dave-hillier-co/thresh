import type { GrainKey } from "./grain-key";

/** Identifies one stream: `(provider, namespace, key)`. */
export interface StreamId {
  provider: string;
  namespace: string;
  key: string;
}

/** A consumer's position in a stream; lets it resume exactly where it left off. */
export class SequenceToken {
  constructor(readonly value: number) {}
}

export interface StreamHandler<T> {
  onNext(event: T, token: SequenceToken): Promise<void>;
  onError?(err: unknown): Promise<void>;
  onCompleted?(): Promise<void>;
}

export interface StreamSubscriptionHandle<T> {
  /** Re-attach a handler after reactivation, resuming from the saved cursor. */
  resume(handler: StreamHandler<T>): Promise<void>;
  unsubscribe(): Promise<void>;
}

export interface SubscribeOptions {
  /** Rewind to a position the backing store still retains. */
  startToken?: SequenceToken;
  /**
   * Identifies the subscribing consumer. The runtime binds this to the calling
   * grain's activation; it scopes `getSubscriptions` so that, when many consumers
   * share one stream, each reacquires only its own durable subscription.
   */
  consumerId?: string;
}

/** A managed event stream addressed by identity; producers publish, consumers subscribe. */
export interface AsyncStream<T> {
  readonly id: StreamId;
  publish(event: T): Promise<void>;
  subscribe(
    handler: StreamHandler<T>,
    options?: SubscribeOptions,
  ): Promise<StreamSubscriptionHandle<T>>;
  /**
   * Re-bind existing durable subscriptions after reactivation. When `consumerId`
   * is given, only that consumer's subscriptions are returned (others sharing the
   * stream are excluded); omitting it returns every subscription on the stream.
   */
  getSubscriptions(consumerId?: string): Promise<StreamSubscriptionHandle<T>[]>;
}

export interface StreamProvider {
  getStream<T>(namespace: string, key: GrainKey): AsyncStream<T>;
}
