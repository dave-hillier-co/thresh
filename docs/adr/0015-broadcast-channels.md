# ADR 0015 — Broadcast channels (lightweight in-cluster pub/sub)

- Status: Accepted — implemented (provider, writer, implicit-subscriber fan-out, observer delivery)
- Context docs: [08 — Event streams](../08-event-streams.md),
  [13 — Roadmap](../13-roadmap-and-phases.md)

> Orleans references: `Orleans.BroadcastChannel/*` —
> `IBroadcastChannelProvider` / `BroadcastChannelProvider`, `IBroadcastChannelWriter<T>` /
> `BroadcastChannelWriter`, `ChannelId`, `BroadcastChannelConsumerExtension`,
> `IOnBroadcastChannelSubscribed` / `BroadcastChannelSubscription`,
> `[ImplicitChannelSubscription]` (`SubscriberTable/Predicates/ImplicitChannelSubscriptionAttribute.cs`),
> `BroadcastChannelOptions`.

## Context

Streams ([ADR 0007](0007-stream-pulling-agents.md)) are durable and decoupled: a producer writes to a
queue, ring-owned pulling agents poll it, cursors track per-consumer position, delivery is
at-least-once. That machinery is the right cost when events must survive a producer or consumer being
down — but it is overkill for "tell every grain currently subscribed to this namespace, right now."
Orleans 10 ships **broadcast channels** for exactly that: in-cluster pub/sub with **no backing store,
no pulling agents, no cursors** — a publish is a direct fan-out to the channel's subscribers.

Subscription is **implicit only**, exactly as in Orleans: a grain type marked
`[ImplicitChannelSubscription(namespace)]` receives every item published to `(namespace, key)` on the
grain whose key is `key`. There is no explicit `subscribe()` call and no subscription record.

## Decision

- **`ChannelId` is `(namespace, key)`.** No provider field (a `StreamId` has none either at this
  layer); the provider is whichever one handed out the writer. Wire form is `namespace/key`,
  identical to a stream key, so the existing implicit-subscriber resolution (`namespace → grain
  types`, addressed by key) is reused verbatim.
- **A separate implicit-subscription registry.** `@implicitChannelSubscription(namespace)` /
  `defineGrain({ implicitChannelSubscriptions })` records into a `broadcastSubscriptions` grain-metadata
  field, kept **distinct** from streams' `implicitSubscriptions` — broadcast channels and streams are
  separate subsystems in Orleans, and a grain may be an implicit subscriber of one but not the other.
- **The consumer is an observer, not a method.** A subscribing grain exposes a handler under the
  `BROADCAST_CHANNEL_OBSERVER` symbol (`{ onPublished, onError? }`), resolved lazily on first delivery
  — mirroring Orleans' `IOnBroadcastChannelSubscribed.OnSubscribed` + `Attach`, and exactly parallel
  to streams' `STREAM_SUBSCRIPTION_OBSERVER`. The grain never declares a wire method.
- **Delivery is a system extension over the dispatcher.** `ClusterNode.publishToBroadcastChannel`
  resolves the namespace's implicit subscriber grain ids and invokes `BroadcastConsumerInterface`
  (`system.BroadcastConsumer`) `onPublished(channelKey, item)` on each, through the normal directory →
  placement path — reactivating an idle subscriber. The activation intercepts that interface (like
  `StreamConsumer`) and runs the observer's `onPublished` as a turn. No new transport, no queue.
- **Providers are named, store nothing.** `createSilo(...).useBroadcastChannels(name)` registers a
  name; the silo creates a `BroadcastChannelProvider` per name on demand whose writers delegate
  `publish` back to the silo fan-out. Grains reach it via `runtime.getBroadcastChannelProvider(name)`
  (Orleans `IBroadcastChannelProvider`). Nothing to connect or tear down.

## Divergence from Orleans: awaited delivery

Orleans' `BroadcastChannelOptions.FireAndForgetDelivery` **defaults to `true`** — `Publish` does not
await the subscribers. This port instead **awaits all deliveries** so a failing subscriber surfaces to
the publisher, and so a publish is deterministically observable (the test asserts effects right after
`await publish`). The rationale: in a new library, silent drop on the default path is a worse failure
mode than a propagated error, and a fire-and-forget option can be layered on later without changing
the contract. This is the one deliberate behavioural divergence; the surface (`ChannelId`, writer,
provider, implicit subscription, observer) is faithful.

## Consequences

- "Notify all live subscribers now" no longer pays for stream durability — no queue, no agent
  ownership, no cursor storage. The cost is one dispatcher call per implicit subscriber.
- Broadcast channels reuse the stream implicit-subscriber model and the `StreamConsumer`-style system
  extension seam, so they add little surface: one core contract file, one provider class, a metadata
  field, a decorator, a builder method, and an activation dispatch case.
- Because delivery rides the dispatcher, a published item reactivates an idle subscriber and reaches
  it wherever it is placed — broadcast is cluster-wide, not silo-local.
- No durability means an item published while a subscriber is mid-restart is lost; that is the
  Orleans semantics — use a stream when delivery must survive downtime.

## Scope boundary

- **Implicit subscription only**, matching Orleans (there is no explicit broadcast `subscribe`).
- **No custom `ChannelIdMapper`** — the channel key maps directly to the subscriber grain key
  (string keys), as with the existing implicit-stream port. Integer/Guid-key mapping is out of scope.
- **No fire-and-forget option yet** — delivery awaits (see divergence above).
