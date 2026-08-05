# Stream backings: Postgres and Kafka

Current implementation notes for the additional Postgres and Kafka stream backings delivered
behind the existing streaming interfaces ([#39](https://github.com/dave-hillier-co/thresh/issues/39)).
The programming model (`StreamProvider`, subscriptions, implicit subscriptions, producer handles)
did not change; each backing is a provider slice behind the shared pulling-provider seams.

> **Status (updated 2026-08-04):** Phase 0 (shared provider core), Phase 1 (Postgres),
> and Phase 2 (Kafka) are implemented. Phase 3 remains optional polish: LISTEN/NOTIFY
> wake-up for Postgres, consumer-lag gauges, and worked examples.

## Where the seams are today

The pulling-stream architecture is already split so that almost everything is backend-neutral:

| Concern | Contract | Implementations |
| --- | --- | --- |
| Physical queue | `PullableQueue` (`getCursor`/`readAfter`/`commit`) + `append` — `packages/streams/src/queue-pulling-agent.ts` | `RedisStreamQueue`, `PostgresStreamQueue`, `KafkaStreamQueue` |
| Pulling agent | `QueuePullingAgent` — poll, deliver, retry/backoff, poison skip, `StreamFailureHandler`, `RecoverableStreamDeliveryError` rewind | Backend-agnostic |
| Queue ownership | Hash ring — `ownedQueueIndices` (`queue-ownership.ts`), driven by `PullingStreamProviderHost.refreshOwnership` on membership change | Backend-agnostic |
| Subscription registry | Durable explicit subscriptions per stream | `RedisSubscriptionRegistry`, `PostgresSubscriptionRegistry` |
| Failure store | `DurableStreamFailureStore` / `DurableStreamFailureHandler` (`stream-failure-store.ts`) | `MemoryStreamFailureStore`, `PostgresStreamFailureStore` |
| Provider orchestration | `PullingStreamProviderCore`: agent lifecycle per owned queue, fan-out to explicit + implicit subscribers, `StreamProducerRegistry`, config validation (`StreamProviderConfigurationError`) | Shared by `RedisPullingStreamProvider`, `PostgresPullingStreamProvider`, and `KafkaPullingStreamProvider` |
| Host wiring | Silo builder methods plus `refreshOwnership` on membership change; delivery through the incoming-call-filter pipeline | `addRedisStreams`, `addPostgresStreams`, `addKafkaStreams` |

Delivery is at-least-once with per-queue ordering: many logical streams multiplex over a fixed
set of physical queues; an agent per owned queue pulls strictly after the durably committed
cursor and commits after delivery, so a successor resumes losslessly on handoff.

## Phase 0 — shared provider core (shipped)

`RedisPullingStreamProvider` couples the neutral orchestration to Redis only through its
constructor. The implementation extracted:

- **`AppendableQueue`** — `PullableQueue` + `append(streamKey, event): Promise<number>` (the
  shape `RedisStreamQueue` already has).
- **`SubscriptionRegistry`** — interface extracted from `RedisSubscriptionRegistry`'s public
  surface.
- **`PullingStreamProviderCore`** (or a base class) — everything currently in
  `RedisPullingStreamProvider` except construction: agent start/stop per owned queue index,
  publish→queue selection (`stableHash32(streamKey) % queueCount`), fan-out to registry +
  implicit subscribers, producer registry, failure-handler forwarding, config validation.

Each backing supplies `{ queues, registry, name, options }` and re-exports its own builder
method — two files per backing instead of a copy of the provider. `RedisPullingStreamProvider`
is now a thin composition; its existing test suite remains the non-regression proof for the
refactor. `MemoryStreamProvider` (push-based, not a pulling provider) is untouched.

## Phase 1 — Postgres backing (shipped)

Rationale: Postgres grain-storage and reminder providers already ship, so the Postgres stream
backing closes the former Redis-only streams gap for Postgres deployments.

### Storage model

Four tables, provisioned lazily the way `PostgresGrainStorage` bootstraps its schema, with a
configurable prefix (default `thresh_stream`):

- **`<p>_events`** — `(id BIGSERIAL PRIMARY KEY, provider TEXT, queue_idx INT, stream_key TEXT,
  payload TEXT, created_at TIMESTAMPTZ DEFAULT now())`. `append` is
  `INSERT … RETURNING id`; the global bigserial is strictly monotonic, so it is a valid
  per-queue token (the `QueueEntry.token` contract requires monotonic-within-queue only).
  Payload uses `serializeValue` from the value codec, same as Redis.
- **`<p>_cursors`** — `(provider TEXT, queue_idx INT, cursor BIGINT, PRIMARY KEY (provider,
  queue_idx))`; `commit` is an upsert, `getCursor` defaults to 0.
- **`<p>_subscriptions`** — the `SubscriptionRegistry` rows (stream key, subscription id,
  subscriber grain reference), mirroring `RedisSubscriptionRegistry` semantics.
- **`<p>_failures`** — a `PostgresStreamFailureStore` implementing `DurableStreamFailureStore`,
  the first durable implementation (Orleans' `AzureTableStorageStreamFailureHandler` analog).

`readAfter(cursor, count)` is `SELECT … WHERE provider=$ AND queue_idx=$ AND id>$ ORDER BY id
LIMIT $`. Index `(provider, queue_idx, id)`.

### Behavioral notes

- **Polling** matches the Redis provider (default 50 ms via `QueuePullingAgent`); a
  LISTEN/NOTIFY wake-up remains Phase 3 optional polish.
- **Retention**: delivered events are dead rows. Opportunistic trim on `commit`
  (`DELETE WHERE queue_idx=$ AND id<=$cursor`), with an optional `retainFor` duration to keep a
  replay window. Configurable off.
- **Cancellation**: all queries take the optional trailing `AbortSignal` following the
  `PostgresGrainStorage` race-based pattern.

### Surface and tests

- `SiloBuilder.addPostgresStreams(name, { connectionString, queueCount?, pollIntervalMs?,
  tablePrefix?, failureHandler?, retainFor? })`, validating options through
  `StreamProviderConfigurationError` like `addRedisStreams`.
- Unit/integration tests live in `postgres-pulling-stream-provider.test.ts` and
  `postgres-stream-failure-store.test.ts`; they use `POSTGRES_URL` and skip cleanly when no
  database is available.
- The hosting-level `postgres-streams-cluster.test.ts` covers multi-silo ownership rebalance and
  cursor-resumed delivery after silo stop.

## Phase 2 — Kafka backing (shipped)

Rationale: teams with Kafka as their event backbone publish/consume streams without a second
durable system for the transport.

### Mapping

- **Topic ↔ provider**: one topic per provider, `<prefix>.<name>` (default `thresh.streams`).
  **Partition ↔ physical queue**: `queueCount` must equal the topic's partition count —
  validated at startup via the admin API, mismatch throws
  `StreamProviderConfigurationError`.
- **`append`**: idempotent producer, `acks=all`, explicit partition = queue index (the core
  already picks the queue by `stableHash32(streamKey)`); token = the record's offset. Offsets
  fit JS numbers (< 2^53). Per-partition ordering satisfies the per-queue ordering contract.
- **`readAfter(cursor)`**: fetch from offset `cursor + 1` on that partition.

### Ownership and cursors — the two deliberate decisions

1. **The ring stays in charge, not Kafka consumer groups.** Group rebalancing would fight the
   port's membership-driven hash-ring ownership (`refreshOwnership`), which reminders, durable
   jobs and Redis streams all share. The consumer uses manual partition assignment: on
   acquiring queue *i*, seek to committed cursor + 1; on losing it, stop the agent. This is the
   same model Orleans' EventHubs provider uses (its own lease-based ownership, not Event Hubs
   consumer-group balancing).
2. **Cursors, subscriptions and failures live in a metadata store, not in Kafka.** Kafka has no
   KV surface for the subscription registry, and committing offsets through the group protocol
   reintroduces the coordination we bypassed. Instead the Kafka backing composes with a
   pluggable metadata store — precisely the registry/cursor/failure trio Phase 1 builds for
   Postgres (Redis equivalents already exist for registry). Precedent: Orleans EventHubs keeps
   checkpoints in Azure Table storage. `addKafkaStreams` therefore takes
   `metadata: { postgres: {…} } | { redis: {…} }`.

### Client library and pull adaptation

`kafkajs` (pure JS, no native build). Its consumer is push-styled, so the queue adapter wraps
it: a per-partition in-memory buffer fed by the paused/resumed consumer implements
`readAfter`'s pull contract, seeking to the cursor on ownership acquire and pausing the
partition when the buffer passes a high-water mark. Kafka is a true boundary — tests run
against a real broker (`KAFKA_BROKERS` env, `describe.skipIf` when unreachable), no mocks.

### Edge cases

- **Retention vs cursor lag**: if the topic's retention deletes records past the committed
  cursor (long outage), the adapter detects the out-of-range offset, seeks to earliest, reports
  the gap through the `StreamFailureHandler`, and logs loudly. At-least-once is preserved for
  what still exists; the gap is surfaced, not silent.
- **Handoff races**: two agents may briefly own a partition across a membership change; both
  deliver (at-least-once, consistent with the existing contract) and the cursor upsert is
  last-writer-wins, same as the Redis backing today.

The Kafka provider suite lives in `kafka-pulling-stream-provider.test.ts`; it runs against a real
broker when `KAFKA_BROKERS` is configured and skips cleanly otherwise. The suite covers publish/
deliver, ownership handoff, durable metadata, poison handling, producer registration,
partition-count validation, and the retention-gap path.

## Phase 3 — polish (optional, after both land)

- LISTEN/NOTIFY wake-up for the Postgres queue (cut poll latency without tightening the loop).
- Consumer-lag gauge per backing under the existing `thresh.streams.*` meter namespace.
- A worked example under `examples/` per backing; README provider matrix update.

## What every backing must keep working

The shared core owns these, and the per-backing suites assert them: implicit subscriptions, delivery through the incoming
call-filter pipeline, `StreamFilter` support, `RecoverableStreamDeliveryError` checkpoint
rewind, producer registration, the durable failure handler, and `TestCluster` sharing.

## Remaining work

Only Phase 3 remains: LISTEN/NOTIFY wake-up for the Postgres queue, consumer-lag gauges, and
worked examples. Those items are optional polish tracked from `todo.md`; the core Postgres and
Kafka backings are already in place.
