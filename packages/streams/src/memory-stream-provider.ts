import { GrainId } from "@tsva/core/grain-id";
import { keyToString, type GrainKey } from "@tsva/core/grain-key";
import type { GrainType } from "@tsva/core/grain-type";
import {
  SequenceToken,
  type AsyncStream,
  type BatchedStreamItem,
  type StreamHandler,
  type StreamId,
  type StreamProvider,
  type StreamSubscriptionHandle,
  type StreamSubscriptionManager,
  type SubscribeOptions,
} from "@tsva/core/stream";
import { implicitSubscriberIds } from "@tsva/streams/implicit-subscriptions";
import { MemorySubscriptionManager } from "@tsva/streams/memory-subscription-manager";
import type { StreamDeliver } from "@tsva/streams/stream-deliver";

interface StreamEvent<T> {
  token: number;
  event: T;
}

class MemorySubscription<T> implements StreamSubscriptionHandle<T> {
  pumping = false;

  constructor(
    private readonly stream: MemoryStream<T>,
    public handler: StreamHandler<T> | undefined,
    public cursor: number,
    public readonly consumerId?: string,
  ) {}

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

  constructor(
    readonly id: StreamId,
    private readonly notifyOutOfProcess: (streamKey: string, event: T, token: number) => Promise<void>,
  ) {}

  async publish(event: T): Promise<void> {
    await this.publishBatch([event]);
  }

  /**
   * Append a batch of events in one shot (Orleans `OnNextBatchAsync`), then fan
   * them out. An explicit subscriber whose handler declares `onNextBatch`
   * receives the whole batch in one call; one that only implements `onNext`
   * (the common case) still receives every event, one at a time, in order —
   * batching only changes how many events arrive per callback, never whether
   * one is dropped. Implicit subscribers (routed through the dispatcher, one
   * event per call — see `deliverStreamEvent`) always receive events
   * individually, regardless of how they were published.
   */
  async publishBatch(events: readonly T[]): Promise<void> {
    const entries: StreamEvent<T>[] = events.map((event) => {
      const entry = { token: this.events.length, event };
      this.events.push(entry);
      return entry;
    });
    for (const sub of this.subscriptions) void this.deliverTo(sub);
    for (const entry of entries) {
      await this.notifyOutOfProcess(`${this.id.namespace}/${this.id.key}`, entry.event, entry.token);
    }
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
   * Deliver pending events to one subscription in order. A handler that
   * declares `onNextBatch` receives every currently-pending event in one call;
   * otherwise events are delivered one at a time via `onNext`. Either way, the
   * cursor only advances once the handler's call resolves, so a handler that
   * throws is redelivered (at-least-once).
   */
  async deliverTo(sub: MemorySubscription<T>): Promise<void> {
    if (sub.pumping || sub.handler === undefined) return;
    sub.pumping = true;
    try {
      while (sub.cursor < this.events.length && sub.handler !== undefined) {
        if (sub.handler.onNextBatch !== undefined) {
          const pending = this.events.slice(sub.cursor);
          const items: BatchedStreamItem<T>[] = pending.map((e) => ({
            event: e.event,
            token: new SequenceToken(e.token),
          }));
          try {
            await sub.handler.onNextBatch(items);
          } catch (err) {
            await sub.handler.onError?.(err);
            break; // leave the cursor so the batch is redelivered
          }
          sub.cursor += pending.length;
          continue;
        }
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

/**
 * In-memory, single-silo stream provider for development and tests. Explicit
 * subscribers (`stream.subscribe`) are delivered to directly, in-process — no
 * activation binding needed, since the subscribing grain already holds the
 * handler closure. Implicit subscribers (Orleans `[ImplicitStreamSubscription]`,
 * a grain that never calls `subscribe`) have no such closure to call, so they
 * are reached the same way the pulling-agent providers reach them: `setDeliver`/
 * `setImplicitSubscribers` are wired by `SiloBuilder.build()` to route each
 * published event through `ClusterNode.deliverStreamEvent`, which resolves
 * (and reactivates, if needed) the subscriber's activation via the dispatcher.
 */
export class MemoryStreamProvider implements StreamProvider {
  private readonly streams = new Map<string, MemoryStream<unknown>>();
  private deliver: StreamDeliver = async () => undefined;
  private implicitTypesFor: (namespace: string) => Iterable<GrainType> = () => [];
  // Every memory provider exposes administrative subscription management
  // (Orleans gates this behind `StreamSubscriptionManagerAdmin`, a separate
  // DI service every streaming-configured silo registers regardless of a
  // given provider's own pub-sub mode — there is no "explicit-subscribe-only"
  // provider config to opt into upstream either).
  private readonly subscriptionManager = new MemorySubscriptionManager();

  constructor(private readonly name: string = "default") {}

  /** Wire how a published event reaches an implicit subscriber's activation. */
  setDeliver(deliver: StreamDeliver): void {
    this.deliver = deliver;
  }

  /** Wire the implicit-subscription table (`namespace → grain types`). */
  setImplicitSubscribers(typesFor: (namespace: string) => Iterable<GrainType>): void {
    this.implicitTypesFor = typesFor;
  }

  /**
   * Wire how an administratively-added subscription is confirmed with its
   * target grain (Orleans `IStreamSubscriptionObserver.OnSubscribed`).
   * Defaults to always-accept if never wired (e.g. a provider built without a
   * hosting silo, as in a unit test that only exercises the registry).
   */
  setConfirmSubscription(confirm: (grainId: GrainId, streamKey: string) => Promise<boolean>): void {
    this.subscriptionManager.setConfirm(confirm);
  }

  getStream<T>(namespace: string, key: GrainKey): AsyncStream<T> {
    const keyString = keyToString(key);
    const mapKey = `${namespace}/${keyString}`;
    let stream = this.streams.get(mapKey);
    if (stream === undefined) {
      stream = new MemoryStream<unknown>(
        { provider: this.name, namespace, key: keyString },
        (streamKey, event, token) => this.fanOut(streamKey, event, token),
      );
      this.streams.set(mapKey, stream);
    }
    return stream as AsyncStream<T>;
  }

  getStreamSubscriptionManager(): StreamSubscriptionManager {
    return this.subscriptionManager;
  }

  private async fanOut(streamKey: string, event: unknown, token: number): Promise<void> {
    const implicit = implicitSubscriberIds(streamKey, this.implicitTypesFor);
    const admin = this.subscriptionManager.subscribersFor(streamKey);
    const seen = new Set<string>();
    for (const subscriber of [...implicit, ...admin]) {
      const id = subscriber.toString();
      if (seen.has(id)) continue;
      seen.add(id);
      await this.deliver(subscriber, streamKey, event, token);
    }
  }
}
