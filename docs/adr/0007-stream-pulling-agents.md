# ADR 0007 — Stream pulling agents and ring-based queue ownership

- Status: Accepted — implemented (queue model, pub-sub registry, consumer-extension delivery, and
  ring-based queue ownership with rebalance)
- Context docs: [09 — Event streams](../09-event-streams.md),
  [06 — Grain directory and placement](../06-grain-directory-and-placement.md),
  [ADR 0005 — Redis defaults](0005-redis-default-providers.md)

## Context

The shipped Redis stream provider ([ADR 0005](0005-redis-default-providers.md)) is durable and
resumable, but it polls **per consumer**: each subscribing activation runs its own XRANGE loop over
the stream and persists its own cursor. That satisfies most of the Phase 6 exit criteria
([13](../13-roadmap-and-phases.md)) but not the last one:

> Queue ownership rebalances on membership change; a new owner resumes from the committed cursor
> with at-least-once delivery (no gaps, possible idempotent redelivery).

Per-consumer polling has no notion of a silo *owning* a slice of the stream space, so there is
nothing to rebalance when the cluster changes, every consumer reads independently, and N consumers
on one stream mean N reads. Orleans solves this with **persistent stream pulling agents**: streams
are multiplexed onto a fixed set of physical queues, each silo runs an agent per queue it owns, and
queue ownership moves with the ring.

## Decision

Introduce a pulling-agent stream provider behind the unchanged grain-facing `AsyncStream` API
(`publish` / `subscribe` / `getSubscriptions` / `resume`). The `MemoryStreamProvider` keeps its
direct in-process fan-out for dev/tests; this is the durable, cluster-correct Redis path.

1. **Physical queues.** All streams are partitioned over a fixed set of `N` physical queues (Redis
   Streams), `queue = hash(streamId) mod N`. `publish` appends to the stream's queue (XADD under a
   monotonic id, as today). A stream maps to exactly one queue, so per-stream order is preserved.

2. **Queue ownership from the ring.** Each queue index `i` maps to a ring point `hash(queueName(i))`.
   The silo whose arc (`ConsistentHashRing.rangesFor`) contains that point owns the queue — the same
   mechanism reminders already use for hash-range ownership. On a membership change the host calls
   `refreshOwnership(node.ownedHashRanges())` and the agent manager starts agents for newly-owned
   queues and stops agents for released ones. This mirrors `LocalReminderService`.

3. **Durable subscriptions.** A Redis pub-sub registry maps `streamId → set of subscriber grain ids`.
   `subscribe` / `unsubscribe` update it (replacing the per-consumer poll). The registry is read by
   agents, so a subscription survives deactivation and the subscriber's silo.

4. **Delivery via a consumer extension.** A pulling agent reads a batch from its queue after the
   committed cursor, and for each event reads the subscriber set for that event's stream. It delivers
   to each subscriber by routing through the **dispatcher** to the grain's single activation,
   invoking a system `StreamConsumer` extension (`deliverStreamEvent(streamId, event, token)`) — the
   exact pattern reminders use (`RemindableInterface` via `node.deliverReminder`). The runtime
   translates that system call into the activation's registered handler `onNext`, run as a turn. A
   subscriber on another silo is reached over the transport; an idle one is reactivated.

5. **At-least-once and handoff.** The agent commits the queue cursor in Redis **only after** a batch
   is delivered. A crash before commit redelivers (consumers should be idempotent). On rebalance the
   new owner resumes from the committed cursor — no gaps. Ordering is per-queue (hence per-stream).

## Consequences

- One read per queue regardless of consumer count; the per-consumer poll is superseded for the Redis
  path. Consumers no longer hold their own cursor — the queue does.
- Cross-silo delivery adds a hop for consumers not on the owning silo (same trade-off as reminders),
  in exchange for cluster-correct ownership and lossless handoff.
- Idempotent consumers are required (at-least-once), as the exit criterion allows.

## Alternatives considered

- **Per-consumer polling (the v1 Redis provider).** Simple, but no ownership to rebalance and no
  shared cursor; kept only as the conceptual predecessor — superseded here for the durable path.
- **Redis consumer groups (XREADGROUP/XACK).** Give per-message ack and redelivery for free. Viable,
  but we want explicit committed cursors per queue so ring handoff is a plain cursor read by the new
  owner; consumer groups can be adopted later behind the same agent.

## Implementation slices

1. Queue model + durable per-queue cursor + `QueuePullingAgent` (pulls a queue, delivers to a
   supplied sink, commits the cursor). Tested against real Redis, single process.
2. Durable pub-sub subscription registry (`streamId → subscribers`).
3. `StreamConsumer` system extension + runtime handler registry + `node.deliverStreamEvent`; wire
   agent delivery through the dispatcher. Single-silo end-to-end (produce → agent → consumer turn).
4. Ring-based queue ownership + rebalance (`PullingAgentManager`, wired in hosting like reminders).
   Multi-silo end-to-end: owner leaves the view, a survivor resumes from the committed cursor.
