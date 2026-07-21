import type { GrainId } from "@tsva/core/grain-id";
import { keyToString, type GrainKey } from "@tsva/core/grain-key";
import type { GrainType } from "@tsva/core/grain-type";
import {
  SequenceToken,
  type AsyncStream,
  type BatchedStreamItem,
  type StreamFilter,
  type StreamHandler,
  type StreamId,
  type StreamProvider,
  type StreamSubscriptionHandle,
  type StreamSubscriptionManager,
  type SubscribeOptions,
} from "@tsva/core/stream";
import { emitStreamingEvent } from "@tsva/core/streaming-diagnostics";
import { systemTimeProvider, type TimeProvider, type TimerHandle } from "@tsva/core/time-provider";
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
  private inactivityTimer: TimerHandle | undefined;

  constructor(
    readonly id: StreamId,
    private readonly notifyOutOfProcess: (
      streamId: StreamId,
      event: T,
      token: number,
    ) => Promise<void>,
    private readonly time: TimeProvider,
    private readonly streamInactivityPeriodMs: number | undefined,
    private readonly providerName: string,
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
    this.armInactivityTimer();
    const entries: StreamEvent<T>[] = events.map((event) => {
      const entry = { token: this.events.length, event };
      this.events.push(entry);
      return entry;
    });
    for (const sub of this.subscriptions) void this.deliverTo(sub);
    for (const entry of entries) {
      await this.notifyOutOfProcess(this.id, entry.event, entry.token);
    }
  }

  /**
   * (Re)arm the per-stream inactivity timer (Orleans `StreamInactivityPeriod`,
   * a pulling-agent option): each publish counts as activity and pushes the
   * "went inactive" signal out by another period; if nothing is published
   * again before it fires, `emitStreamingEvent` reports the stream inactive.
   * A no-op unless the provider was configured with a period.
   */
  private armInactivityTimer(): void {
    if (this.streamInactivityPeriodMs === undefined) return;
    if (this.inactivityTimer !== undefined) this.time.clearTimer(this.inactivityTimer);
    this.inactivityTimer = this.time.setTimer(() => {
      this.inactivityTimer = undefined;
      emitStreamingEvent({
        kind: "streamInactive",
        providerName: this.providerName,
        streamId: this.id,
      });
    }, this.streamInactivityPeriodMs);
  }

  /** Cancel the inactivity timer (silo/provider teardown), so it never leaks past disposal. */
  dispose(): void {
    if (this.inactivityTimer !== undefined) this.time.clearTimer(this.inactivityTimer);
    this.inactivityTimer = undefined;
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
          for (const _item of items) {
            emitStreamingEvent({
              kind: "itemDelivered",
              providerName: this.providerName,
              streamId: this.id,
            });
          }
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
        emitStreamingEvent({
          kind: "itemDelivered",
          providerName: this.providerName,
          streamId: this.id,
        });
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
  private time: TimeProvider = systemTimeProvider;
  // Orleans' `StreamInactivityPeriod` (a pulling-agent option): how long a
  // stream may go without a publish before it is reported inactive. This
  // provider has no pulling agent, but the *timer* is still real work a test
  // can synchronize on (see `MemoryStream.armInactivityTimer`), so the option
  // is honored even though nothing else about the pulling-agent model exists
  // here.
  private streamInactivityPeriodMs: number | undefined;
  // Orleans `IStreamFilter` (`ISiloBuilder.AddStreamFilter`): a server-side
  // predicate applied before an event reaches any implicit subscriber. Only
  // the implicit path is filtered — a stream's explicit subscribers
  // (`stream.subscribe`) are delivered to directly and are not gated by this
  // provider-wide filter upstream either (the filter is a pulling-agent/queue
  // concern, and implicit subscription is the only path this provider routes
  // through anything resembling one).
  private streamFilter: StreamFilter | undefined;

  constructor(readonly name: string = "default") {}

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

  /**
   * Inject the clock every stream created from this point on uses for its
   * inactivity timer (defaults to the system clock). `SiloBuilder.build()`
   * calls this before any `getStream` is reachable from a running silo, so a
   * `FakeTimeProvider` passed to `TestCluster.start` reaches every stream.
   */
  setTimeProvider(time: TimeProvider): void {
    this.time = time;
  }

  /**
   * Configure `StreamInactivityPeriod` (Orleans pulling-agent option): every
   * stream created from this point on (re)arms an inactivity timer of this
   * length on each publish. Unset (the default) means no inactivity timer —
   * existing behavior for every caller that never opts in.
   */
  setStreamInactivityPeriod(ms: number): void {
    this.streamInactivityPeriodMs = ms;
  }

  /** Install a server-side filter (Orleans `AddStreamFilter`); see the field doc. */
  setStreamFilter(filter: StreamFilter): void {
    this.streamFilter = filter;
  }

  getStream<T>(namespace: string, key: GrainKey): AsyncStream<T> {
    const keyString = keyToString(key);
    const mapKey = `${namespace}/${keyString}`;
    let stream = this.streams.get(mapKey);
    if (stream === undefined) {
      stream = new MemoryStream<unknown>(
        { provider: this.name, namespace, key: keyString },
        (streamId, event, token) => this.fanOut(streamId, event, token),
        this.time,
        this.streamInactivityPeriodMs,
        this.name,
      );
      this.streams.set(mapKey, stream);
    }
    return stream as AsyncStream<T>;
  }

  getStreamSubscriptionManager(): StreamSubscriptionManager {
    return this.subscriptionManager;
  }

  /** Cancel every stream's inactivity timer (silo shutdown) so none leak past disposal. */
  stop(): void {
    for (const stream of this.streams.values()) stream.dispose();
  }

  /**
   * Deliver to every implicit/admin subscriber concurrently rather than one
   * at a time. A subscriber reached through `deliver` may need to reactivate
   * (a real, possibly slow, activation) before it can accept the event; a
   * sequential `await` there would let one slow-activating subscriber stall
   * every other subscriber sharing the same stream (Orleans
   * `ResumeAfterSlowSubscriber`). `Promise.allSettled` so one subscriber's
   * rejection doesn't stop delivery to the rest.
   */
  private async fanOut(streamId: StreamId, event: unknown, token: number): Promise<void> {
    if (this.streamFilter !== undefined && !this.streamFilter.shouldDeliver(streamId, event))
      return;
    const streamKey = `${streamId.namespace}/${streamId.key}`;
    const implicit = implicitSubscriberIds(streamKey, this.implicitTypesFor);
    const admin = this.subscriptionManager.subscribersFor(streamKey);
    const seen = new Set<string>();
    const subscribers = [];
    for (const subscriber of [...implicit, ...admin]) {
      const id = subscriber.toString();
      if (seen.has(id)) continue;
      seen.add(id);
      subscribers.push(subscriber);
    }
    await Promise.allSettled(
      subscribers.map(async (subscriber) => {
        await this.deliver(subscriber, streamKey, event, token);
        emitStreamingEvent({ kind: "itemDelivered", providerName: this.name, streamId });
      }),
    );
  }
}
