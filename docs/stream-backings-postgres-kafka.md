# Stream backings: Postgres and Kafka

Design and delivery plan for two additional stream backings behind the existing streaming
interfaces ([#39](https://github.com/dave-hillier-co/ts-virtual-actors/issues/39)). The
programming model (`StreamProvider`, subscriptions, implicit subscriptions, producer handles)
does not change; each backing is a new provider package slice behind seams that already exist.

## Where the seams are today

The pulling-stream architecture is already split so that almost everything is backend-neutral:

| Concern | Contract | Redis implementation |
| --- | --- | --- |
| Physical queue | `PullableQueue` (`getCursor`/`readAfter`/`commit`) + `append` — `packages/streams/src/queue-pulling-agent.ts`, `redis-stream-queue.ts` | `RedisStreamQueue` (Redis Stream + Lua-allocated monotonic token + cursor key) |
| Pulling agent | `QueuePullingAgent` — poll, deliver, retry/backoff, poison skip, `StreamFailureHandler`, `RecoverableStreamDeliveryError` rewind | backend-agnostic already |
| Queue ownership | hash ring — `ownedQueueIndices` (`queue-ownership.ts`), driven by `PullingStreamProviderHost.refreshOwnership` on membership change | backend-agnostic already |
| Subscription registry | durable explicit subscriptions per stream | `RedisSubscriptionRegistry` |
| Failure store | `DurableStreamFailureStore` / `DurableStreamFailureHandler` (`stream-failure-store.ts`) | `MemoryStreamFailureStore` (Redis store not yet needed) |
| Provider orchestration | `ActivationBoundStreamProvider`: agent lifecycle per owned queue, fan-out to explicit + implicit subscribers, `StreamProducerRegistry`, config validation (`StreamProviderConfigurationError`) | `RedisPullingStreamProvider` — mostly backend-neutral code |
| Host wiring | `SiloBuilder.addRedisStreams(name, options)`; `refreshOwnership` on membership change; delivery through the incoming-call-filter pipeline | — |

Delivery is at-least-once with per-queue ordering: many logical streams multiplex over a fixed
set of physical queues; an agent per owned queue pulls strictly after the durably committed
cursor and commits after delivery, so a successor resumes losslessly on handoff.

## Phase 0 — extract the shared provider core (behavior-preserving)

`RedisPullingStreamProvider` couples the neutral orchestration to Redis only through its
constructor. Before adding a second durable backing, extract:

- **`AppendableQueue`** — `PullableQueue` + `append(streamKey, event): Promise<number>` (the
  shape `RedisStreamQueue` already has).
- **`SubscriptionRegistry`** — interface extracted from `RedisSubscriptionRegistry`'s public
  surface.
- **`PullingStreamProviderCore`** (or a base class) — everything currently in
  `RedisPullingStreamProvider` except construction: agent start/stop per owned queue index,
  publish→queue selection (`stableHash32(streamKey) % queueCount`), fan-out to registry +
  implicit subscribers, producer registry, failure-handler forwarding, config validation.

A backing then supplies `{ queues, registry, name, options }` and re-exports its own builder
method — two files per backing instead of a copy of the provider. `RedisPullingStreamProvider`
becomes a thin composition; its existing test suite is the non-regression proof for the
refactor. `MemoryStreamProvider` (push-based, not a pulling provider) is untouched.

Exit criterion: all existing stream tests pass unchanged; no public API change.

## Phase 1 — Postgres backing (`@tsva/streams` + `packages/persistence` conventions)

Rationale: Postgres grain-storage and reminder providers already ship, so a Postgres-only
deployment currently needs Redis solely for streams. This closes that gap.

### Storage model

Four tables, provisioned lazily the way `PostgresGrainStorage` bootstraps its schema, with a
configurable prefix (default `tsva_stream`):

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
  LISTEN/NOTIFY wake-up is a later optimization, not part of this slice.
- **Retention**: delivered events are dead rows. Opportunistic trim on `commit`
  (`DELETE WHERE queue_idx=$ AND id<=$cursor`), with an optional `retainFor` duration to keep a
  replay window. Configurable off.
- **Cancellation**: all queries take the optional trailing `AbortSignal` following the
  `PostgresGrainStorage` race-based pattern.

### Surface and tests

- `SiloBuilder.addPostgresStreams(name, { connectionString, queueCount?, pollIntervalMs?,
  tablePrefix?, failureHandler?, retainFor? })`, validating options through
  `StreamProviderConfigurationError` like `addRedisStreams`.
- Unit/integration tests against real Postgres, `describe.skipIf(pool === undefined)` with
  `POSTGRES_URL` (existing convention in `postgres-grain-storage.test.ts`); the suite mirrors
  `redis-pulling-stream-provider.test.ts` (publish/deliver, cursor handoff, implicit
  subscribers, poison skip + failure store, producer registration).
- A hosting-level multi-silo cluster test mirroring the Redis streams cluster test: ownership
  rebalances on silo stop, delivery resumes from the committed cursor.

Exit criterion: the Redis streams test suite ported to Postgres passes against a real database;
a multi-silo cluster delivers across silo failure with no gaps.

## Phase 2 — Kafka backing

Rationale: teams with Kafka as their event backbone publish/consume streams without a second
durable system for the transport.

### Mapping

- **Topic ↔ provider**: one topic per provider, `<prefix>.<name>` (default `tsva.streams`).
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

Exit criterion: the same ported provider suite plus the multi-silo cluster test pass against a
real broker; partition-count validation and the retention-gap path are covered.

## Phase 3 — polish (optional, after both land)

- LISTEN/NOTIFY wake-up for the Postgres queue (cut poll latency without tightening the loop).
- Consumer-lag gauge per backing under the existing `tsva.streams.*` meter namespace.
- A worked example under `examples/` per backing; README provider matrix update.

## What every backing must keep working

The shared core owns these, so they hold by construction once Phase 0 lands — the per-backing
suites assert them anyway: implicit subscriptions, delivery through the incoming
call-filter pipeline, `StreamFilter` support, `RecoverableStreamDeliveryError` checkpoint
rewind, producer registration, the durable failure handler, and `TestCluster` sharing.

## Order and dependencies

Phase 0 → Phase 1 → Phase 2 (Kafka reuses Phase 1's metadata store); Phase 3 anytime after its
target backing. Each phase is a vertical slice with its own exit criterion, matching the
repo's TDD/slice workflow.
