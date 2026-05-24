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
}

/** A managed event stream addressed by identity; producers publish, consumers subscribe. */
export interface AsyncStream<T> {
  readonly id: StreamId;
  publish(event: T): Promise<void>;
  subscribe(
    handler: StreamHandler<T>,
    options?: SubscribeOptions,
  ): Promise<StreamSubscriptionHandle<T>>;
  /** Re-bind existing durable subscriptions after reactivation. */
  getSubscriptions(): Promise<StreamSubscriptionHandle<T>[]>;
}

export interface StreamProvider {
  getStream<T>(namespace: string, key: GrainKey): AsyncStream<T>;
}
