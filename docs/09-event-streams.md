# 09 — Event streams

Streams let grains and clients publish and consume sequences of events in near real-time, decoupled
from one another. Like Orleans, streams are **managed** — you address one by identity and
publish/subscribe, with no pre-registration — durable, and **default to Redis Streams**.

> Orleans references: `Orleans.Streaming/Core/{IAsyncStream,StreamSubscriptionHandle}.cs`,
> `Orleans.Streaming/Providers/IStreamProvider.cs`,
> `Orleans.Streaming/PersistentStreams/{PersistentStreamProvider,PersistentStreamPullingAgent,IStreamQueueBalancer,IStreamQueueCheckpointer}.cs`.

## Concepts

- **Stream identity** — `(provider, namespace, key)`; stable and meaningful, like a `GrainId`.
- **Producer** — anything holding a handle calls `publish(event)`.
- **Consumer** — a grain or client `subscribe`s with a handler and receives events in order.
- **Subscription** — a durable record of interest; survives deactivation and is re-established on
  reactivation.
- **Cursor / checkpoint** — the position a consumer has processed up to, for exact resume.

## API

```ts
interface StreamProvider {
  getStream<T>(namespace: string, key: GrainKey): AsyncStream<T>;
}
interface AsyncStream<T> {
  readonly id: StreamId;
  publish(event: T): Promise<void>;
  subscribe(handler: StreamHandler<T>, options?: SubscribeOptions): Promise<StreamSubscriptionHandle<T>>;
  getSubscriptions(consumerId?: string): Promise<StreamSubscriptionHandle<T>[]>; // re-bind after reactivation
}
interface StreamHandler<T> { onNext(event: T, token: SequenceToken): Promise<void>; onError?(e): Promise<void>; onCompleted?(): Promise<void>; }
interface StreamSubscriptionHandle<T> { resume(handler: StreamHandler<T>): Promise<void>; unsubscribe(): Promise<void>; }
interface SubscribeOptions { startToken?: SequenceToken; consumerId?: string; }
```

This mirrors Orleans' `IStreamProvider` / `IAsyncStream<T>` / `StreamSubscriptionHandle<T>`. A producer
calls `getStream(ns, key).publish(event)`; a consumer subscribes in `onActivate`, re-binding an
existing durable subscription or creating one:

```ts
onActivate: async () => {
  const stream = ctx.runtime.getStreamProvider().getStream<Reading>("telemetry", ctx.id.key);
  const subs = await stream.getSubscriptions();
  if (subs.length > 0) await subs[0].resume(handler());
  else await stream.subscribe(handler());
};
```

Delivery into a consuming grain happens **as a turn** on its activation, so handlers respect
single-threaded execution ([02](02-actor-model.md)). `subscribe`/`resume` don't await delivery —
a grain may subscribe from within a turn and each `onNext` is itself a turn, so awaiting would
deadlock the consumer against its own queue.

### Fan-out and per-consumer subscriptions

Many grains can subscribe to the same stream; each gets every event as a turn on its own activation.
`getSubscriptions()` is **scoped to the calling grain** (the runtime binds the consumer identity), so a
consumer that idled and was collected reacquires *its own* subscription and cursor on reactivation —
the `subs[0]` re-bind is correct even with thousands of consumers. [`examples/chat`](11-public-api-and-examples.md)
exercises this: a collected member resumes from where it left off, recovering only the messages it
missed.

## Architecture: pulling agents over physical queues

Following Orleans' persistent-stream design, many logical streams are multiplexed over a smaller set
of **physical queues** (Redis Streams), and pulling those queues is distributed across the cluster.

- **Physical queues** — a fixed set of Redis Streams; a logical stream id hashes to one queue, so its
  events stay ordered there.
- **Pulling agents** — each silo runs agents for the queues it owns, reading batches and delivering
  each event to subscribed consumers.
- **Queue ownership** — queues are distributed across silos by the same consistent-hash ring as the
  directory/reminders (Orleans' `IStreamQueueBalancer`); ownership rebalances on membership change and
  a new owner resumes from the last committed cursor.
- **Cursors** — each subscription's position is checkpointed durably (Orleans' `IStreamQueueCheckpointer`),
  giving **at-least-once** delivery.

## Delivery semantics

- **At-least-once** — events are redelivered after a crash up to the last committed cursor; consumers
  should be idempotent (Redis Streams consumer groups give per-queue acknowledgement).
- **Per-stream ordering** — a logical stream maps to one queue, so its events arrive in publish order.
- **Rewind** — a consumer may subscribe from an earlier `SequenceToken` if the store still retains it
  (governed by Redis Stream trimming).

## Implicit subscriptions

A grain type can be **implicitly subscribed** to a namespace (Orleans' `[ImplicitStreamSubscription]`),
auto-subscribed by key with no `subscribe` call. A grain of that type with key `K` receives every event
on stream `(namespace, K)`; the agent reactivates it on demand to deliver.

```ts
@grain() @implicitStreamSubscription("chat")
class ArchiveGrain extends Grain implements IArchive {
  // Called the first time an event for one of this grain's implicit streams arrives
  // (Orleans' IStreamSubscriptionObserver); returns the handler for that stream.
  [STREAM_SUBSCRIPTION_OBSERVER](namespace: string, key: string): StreamHandler<Message> {
    return { onNext: async (msg) => { /* archive msg for room `key` */ } };
  }
}
// Functional: defineGrain(..., { implicitSubscriptions: ["chat"] }) with the same observer member.
```

When an agent pulls an event for `(ns, K)` it fans out to the registry's explicit subscribers **and**
every grain type implicitly subscribed to `ns`, addressing each at key `K` (deduplicated). Implicit
subscriptions are a pulling-agent feature (Redis provider); the in-memory provider is explicit-only.

## Providers

- **Redis Streams (default)** — physical queues are Redis Streams; consumer groups give
  acknowledgement and redelivery; cursors and subscriptions live in Redis, surviving deactivation and
  silo failure; delivery routes through the dispatcher as a `StreamConsumer` system call (the path
  reminders use) and the queue cursor commits only after delivery. Queue ownership follows the ring and
  rebalances on membership change. Configured with `addRedisStreams(name, { url, keyPrefix? })`.
  See [ADR 0005](adr/0005-redis-default-providers.md), [ADR 0007](adr/0007-stream-pulling-agents.md).
- **In-memory** — dev/tests: single-silo, non-durable, explicit subscriptions only, with ordered
  delivery, a per-subscription cursor that advances only after `onNext` resolves (so a thrown handler
  is redelivered), resume, and rewind. Configured with `useMemoryStreams()`.

The provider interface keeps other queue backings open, as Orleans does behind `PersistentStreamProvider`.
