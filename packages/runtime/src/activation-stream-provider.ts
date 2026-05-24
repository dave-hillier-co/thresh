import type { GrainKey } from "@tsva/core/grain-key";
import type {
  AsyncStream,
  StreamHandler,
  StreamId,
  StreamProvider,
  StreamSubscriptionHandle,
  SubscribeOptions,
} from "@tsva/core/stream";

/** Runs a stream callback as a turn on the consumer's activation. */
export type RunTurn = (callback: () => Promise<void>) => Promise<unknown>;

/**
 * Wraps a silo-level `StreamProvider` so that events delivered to a grain's
 * handler run as turns on that grain's activation — preserving single-threaded
 * execution. Handlers attached via `subscribe`/`resume` are wrapped; the
 * underlying durable subscription (and its cursor) lives in the base provider,
 * so it survives deactivation and resumes on reactivation.
 */
export class ActivationStreamProvider implements StreamProvider {
  constructor(
    private readonly base: StreamProvider,
    private readonly runTurn: RunTurn,
  ) {}

  getStream<T>(namespace: string, key: GrainKey): AsyncStream<T> {
    return new TurnDeliveringStream(this.base.getStream<T>(namespace, key), this.runTurn);
  }
}

class TurnDeliveringStream<T> implements AsyncStream<T> {
  constructor(
    private readonly inner: AsyncStream<T>,
    private readonly runTurn: RunTurn,
  ) {}

  get id(): StreamId {
    return this.inner.id;
  }

  publish(event: T): Promise<void> {
    return this.inner.publish(event);
  }

  async subscribe(
    handler: StreamHandler<T>,
    options?: SubscribeOptions,
  ): Promise<StreamSubscriptionHandle<T>> {
    const handle = await this.inner.subscribe(wrapHandler(handler, this.runTurn), options);
    return new TurnDeliveringHandle(handle, this.runTurn);
  }

  async getSubscriptions(): Promise<StreamSubscriptionHandle<T>[]> {
    const handles = await this.inner.getSubscriptions();
    return handles.map((h) => new TurnDeliveringHandle(h, this.runTurn));
  }
}

class TurnDeliveringHandle<T> implements StreamSubscriptionHandle<T> {
  constructor(
    private readonly inner: StreamSubscriptionHandle<T>,
    private readonly runTurn: RunTurn,
  ) {}

  resume(handler: StreamHandler<T>): Promise<void> {
    return this.inner.resume(wrapHandler(handler, this.runTurn));
  }

  unsubscribe(): Promise<void> {
    return this.inner.unsubscribe();
  }
}

function wrapHandler<T>(handler: StreamHandler<T>, runTurn: RunTurn): StreamHandler<T> {
  const onError = handler.onError;
  const onCompleted = handler.onCompleted;
  return {
    onNext: (event, token) => runTurn(() => handler.onNext(event, token)) as Promise<void>,
    ...(onError !== undefined
      ? { onError: (err) => runTurn(() => onError.call(handler, err)) as Promise<void> }
      : {}),
    ...(onCompleted !== undefined
      ? { onCompleted: () => runTurn(() => onCompleted.call(handler)) as Promise<void> }
      : {}),
  };
}
