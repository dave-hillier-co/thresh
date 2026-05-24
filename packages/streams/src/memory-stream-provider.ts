import { keyToString, type GrainKey } from "@tsva/core/grain-key";
import {
  SequenceToken,
  type AsyncStream,
  type StreamHandler,
  type StreamId,
  type StreamProvider,
  type StreamSubscriptionHandle,
  type SubscribeOptions,
} from "@tsva/core/stream";

interface StreamEvent<T> {
  token: number;
  event: T;
}

class MemorySubscription<T> implements StreamSubscriptionHandle<T> {
  handler: StreamHandler<T> | undefined;
  cursor: number;
  pumping = false;
  readonly consumerId: string | undefined;

  constructor(
    private readonly stream: MemoryStream<T>,
    handler: StreamHandler<T> | undefined,
    cursor: number,
    consumerId?: string,
  ) {
    this.handler = handler;
    this.cursor = cursor;
    this.consumerId = consumerId;
  }

  async resume(handler: StreamHandler<T>): Promise<void> {
    this.handler = handler;
    // Deliver asynchronously: a grain may call resume from within a turn, and
    // each onNext runs as a turn on that same activation — awaiting here would
    // deadlock the consumer against its own queue.
    void this.stream.deliverTo(this);
  }

  async unsubscribe(): Promise<void> {
    this.stream.removeSubscription(this);
  }
}

class MemoryStream<T> implements AsyncStream<T> {
  private readonly events: StreamEvent<T>[] = [];
  private readonly subscriptions: MemorySubscription<T>[] = [];

  constructor(readonly id: StreamId) {}

  async publish(event: T): Promise<void> {
    this.events.push({ token: this.events.length, event });
    for (const sub of this.subscriptions) void this.deliverTo(sub);
  }

  async subscribe(
    handler: StreamHandler<T>,
    options?: SubscribeOptions,
  ): Promise<StreamSubscriptionHandle<T>> {
    // Default to "from now"; a startToken rewinds to a retained position.
    const cursor = options?.startToken?.value ?? this.events.length;
    const sub = new MemorySubscription<T>(this, handler, cursor, options?.consumerId);
    this.subscriptions.push(sub);
    // Deliver asynchronously (see resume): the subscriber may be mid-turn.
    void this.deliverTo(sub);
    return sub;
  }

  async getSubscriptions(consumerId?: string): Promise<StreamSubscriptionHandle<T>[]> {
    if (consumerId === undefined) return [...this.subscriptions];
    return this.subscriptions.filter((s) => s.consumerId === consumerId);
  }

  removeSubscription(sub: MemorySubscription<T>): void {
    const i = this.subscriptions.indexOf(sub);
    if (i >= 0) this.subscriptions.splice(i, 1);
  }

  /**
   * Deliver pending events to one subscription in order, one at a time. The
   * cursor only advances once `onNext` resolves, so a handler that throws is
   * redelivered (at-least-once).
   */
  async deliverTo(sub: MemorySubscription<T>): Promise<void> {
    if (sub.pumping || sub.handler === undefined) return;
    sub.pumping = true;
    try {
      while (sub.cursor < this.events.length && sub.handler !== undefined) {
        const entry = this.events[sub.cursor]!;
        try {
          await sub.handler.onNext(entry.event, new SequenceToken(entry.token));
        } catch (err) {
          await sub.handler.onError?.(err);
          break; // leave the cursor so the event is redelivered
        }
        sub.cursor++;
      }
    } finally {
      sub.pumping = false;
    }
  }
}

/** In-memory, single-silo stream provider for development and tests. */
export class MemoryStreamProvider implements StreamProvider {
  private readonly streams = new Map<string, MemoryStream<unknown>>();

  constructor(private readonly name: string = "default") {}

  getStream<T>(namespace: string, key: GrainKey): AsyncStream<T> {
    const keyString = keyToString(key);
    const mapKey = `${namespace}/${keyString}`;
    let stream = this.streams.get(mapKey);
    if (stream === undefined) {
      stream = new MemoryStream<unknown>({ provider: this.name, namespace, key: keyString });
      this.streams.set(mapKey, stream);
    }
    return stream as AsyncStream<T>;
  }
}
