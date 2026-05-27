# ADR 0017 — Browser state replication and browser-hosted grains

- Status: Proposed (design only — a beyond-parity direction, no implementation in this commit)
- Context docs: [13 — Roadmap (Beyond parity)](../13-roadmap-and-phases.md),
  [11 — Public API and examples (external client)](../11-public-api-and-examples.md),
  [09 — Event streams](../09-event-streams.md), [07 — Persistence](../07-persistence.md),
  [ADR 0002 — WebSocket transport](0002-websocket-transport.md),
  [ADR 0008 — Cross-grain transactions](0008-cross-grain-transactions.md),
  [ADR 0012 — Grain call filters](0012-grain-call-filters.md)

> Orleans references: Orleans grain **observers** (`Orleans.Core.Abstractions/IGrainObserver.cs`,
> `Orleans.Runtime/Utilities/ObserverManager.cs`) — a server pushing updates to an external,
> untrusted, disconnectable subscriber — are the closest prior art for the live read-view, and
> ts-virtual-actors already has the equivalent in event streams ([09](../09-event-streams.md)).
> Orleans has **no** browser-hosted grains and no client-placement trust model; that layer is a
> deliberate extension *beyond* Orleans (this is a "Beyond parity" item, not a parity port).

## Context

[Roadmap 13 — Beyond parity](../13-roadmap-and-phases.md) and [EPICS.md](../../EPICS.md) name a
post-parity direction: **replicate grain state to the browser as a live read-view, and eventually run
a subset of grains in a lightweight browser-side runtime, with a server-enforced policy for which
grain types are permitted there.** Both docs say it "needs an ADR" before implementation; this is
that ADR. It is design only — it settles the shape and the trust model so a later vertical slice can
implement it, and changes no code.

[Roadmap 13](../13-roadmap-and-phases.md) frames three layers of ambition:

1. **State replication / live read-views** — the server stays the source of truth and the browser
   holds a derived, live replica, built on the external client
   ([11](../11-public-api-and-examples.md)) and event streams ([09](../09-event-streams.md)).
2. **Browser-hosted grains** — a partial silo hosting some activations client-side and forwarding the
   rest.
3. **Permission model (the crux)** — "permitted to run there" is a **trust/authority
   classification**, not merely placement, because the browser is untrusted: pair a grain-type marker
   with a gate **enforced on the silo**, never self-granted by the client. Per-user / view-model
   state may live client-side; **shared, authoritative or secret state must not** — the transactional
   grains of [ADR 0008](0008-cross-grain-transactions.md) assume a trusted single activation (wait-die
   locking, durable commit), so a browser replica of authoritative state needs a different consistency
   model.

The roadmap says the **intended motivation** must be settled first. The three motivations — offline,
optimistic-UI latency, reduced server load — are all in view, but this ADR optimizes **latency-first**
(fast local reads of a user's own view-model) and, deliberately, commits v1 to **read-only replicas
only**. Read-only is the decisive scoping choice: with the server activation as the sole writer there
is **no write-conflict problem to solve**, so the hard consistency question (optimistic/etag vs CRDT
reconciliation of *client* writes) and offline/local-first are deferred to a follow-up ADR. This lets
v1 establish the trust boundary — the genuinely novel and security-critical part — against a simple,
eventually-consistent read model.

The runtime already has the pieces this builds on, and reusing them is a goal:

- **Client → gateway transport.** The external client (`@tsva/client`) is not a silo; it forwards
  every `getGrain` call to a **gateway silo** over the WebSocket transport and the gateway routes to
  the activation ([11](../11-public-api-and-examples.md), [ADR 0002](0002-websocket-transport.md)).
  ADR 0002 already anticipates browser clients on this path. The browser never connects to arbitrary
  silos — only to a gateway.
- **Observer analogue.** Event streams ([09](../09-event-streams.md)) give snapshot-then-subscribe,
  at-least-once delivery, and consumer-scoped durable subscriptions that resume after a consumer
  deactivates (the `chat` example) — the ts-virtual-actors equivalent of Orleans grain observers.
- **Versioned state.** `PersistentState` carries an **etag** ([07](../07-persistence.md)); a replica
  can use a monotonic version to detect gaps and trigger a re-sync.
- **A cross-cutting interception seam.** Grain call filters ([ADR 0012](0012-grain-call-filters.md))
  run silo-wide or per-grain, *inside* the activation turn — the natural enforcement point for the
  trust gate, and already cited as the place auth belongs.
- **Single activation.** The directory compare-and-set ([06](../06-grain-directory-and-placement.md))
  keeps exactly one authoritative activation; this ADR does not weaken it.

The one thing the runtime lacks is **per-client identity**: today connections are isolated only by
`clusterId` ([ADR 0002](0002-websocket-transport.md)), with no notion of *which* user/session a call
comes from. The trust model below needs that, so this ADR specifies the seam (without fixing an auth
scheme).

## Decision

Adopt a **server-authoritative, read-only live read-view** as v1, on the existing client→gateway
WebSocket path, gated by a default-deny eligibility marker enforced on the silo. Design — but do not
implement — browser-hosted grains and writable client state as later layers behind the same trust
boundary.

### 1. Scope and layered ambition

v1 is **Layer 1, read-only**: the browser holds a derived, eventually-consistent replica of grain
state it is authorized to observe; all writes continue to go through ordinary `getGrain` calls to the
authoritative server activation. Layer 2 (browser-hosted grains) and writable/optimistic/CRDT client
state are explicitly out of scope here and get their own ADRs — but the trust boundary defined below
is designed so those layers slot in behind it without rework.

### 2. Eligibility and the trust boundary (server-authoritative)

- **Type-level declaration.** A grain *type* opts in via a marker on grain metadata — a
  `browserReplication` field on `GrainOptions` (the `@grain({ ... })` / `defineGrain` options bag, see
  [11](../11-public-api-and-examples.md)). It is a **capability the server declares**, recorded in the
  per-silo grain registry, **never** asserted by the client.
- **Default-deny.** A grain type is not replicable unless explicitly marked. Shared, authoritative or
  secret state is simply never marked. Transactional grains ([ADR 0008](0008-cross-grain-transactions.md))
  are **ineligible** — their trusted-single-activation assumptions (wait-die locking, durable commit)
  are incompatible with an untrusted replica.
- **Gate enforced on the silo.** A browser's request to *open a replication subscription* passes
  through an **incoming grain call filter on the gateway** ([ADR 0012](0012-grain-call-filters.md)),
  which (a) confirms the target type is marked replicable and (b) authorizes *this client* for *this
  key*. This is the same seam ADR 0012 names for auth, and it runs inside the turn so it sees the same
  ordering as the grain.
- **Per-key authorization.** Eligibility is per type *and* per key: even for a replicable type, the
  server decides which keys a client may observe (e.g. a per-user grain keyed by the authenticated
  user id — a client may observe only its own). The decision is a policy callback the gate consults,
  not a client claim.
- **Client identity seam.** The gate needs to know who the client is. This ADR specifies that a
  client **session identity** is carried in the connection preamble / request context
  ([ADR 0002](0002-websocket-transport.md), [04](../04-messaging-and-serialization.md)) and made
  available to the gateway filter. The concrete authentication scheme (token format, issuer,
  revocation) is **application-pluggable and out of scope** — the runtime provides the seam and the
  default-deny posture.

### 3. Replication and consistency

- **Server stays the source of truth.** The single authoritative activation (directory CAS,
  [06](../06-grain-directory-and-placement.md)) is unchanged and remains the sole writer.
- **Replica as a server-side observer.** Once the gate admits a subscription, the gateway maintains a
  server-side subscription to the grain's state changes on the browser's behalf, modeled on event
  streams ([09](../09-event-streams.md)) — the grain-observer analogue. The browser receives a
  **snapshot on subscribe**, then **versioned deltas** as the grain's state changes.
- **Gap detection via version.** Each snapshot/delta carries a monotonic version derived from the
  grain's state version (the `PersistentState` etag, [07](../07-persistence.md)). Stream delivery is
  at-least-once ([09](../09-event-streams.md)), so the browser **dedups by version** and, on a
  detected gap, requests a **re-snapshot** rather than applying an out-of-order delta.
- **Consistency guarantee.** The replica is **per-grain monotonic and eventually consistent** with the
  authoritative activation. Stale reads are possible and acceptable for view-models; because the
  browser cannot write the replica, there is no conflict to reconcile. Authoritative mutation is the
  unchanged `getGrain` call path.
- **Lifecycle.** Subscriptions survive server activation deactivation/migration by re-subscribing on
  reactivation (the consumer-scoped durable resume pattern from the `chat` example,
  [09](../09-event-streams.md)). A browser disconnect tears the server-side subscription down so it is
  resource-bounded.

### 4. Transport

Reuse the existing **WebSocket transport** ([ADR 0002](0002-websocket-transport.md)) and the
client→gateway model ([11](../11-public-api-and-examples.md)) unchanged — no new transport. Add a
**replication message kind** to the envelope ([04](../04-messaging-and-serialization.md)): subscribe /
snapshot / delta / resync frames, multiplexed on the existing `correlationId` / a subscription id and
MessagePack-serialized like every other message. The gateway is the trust boundary on the wire; the
browser reaches only the gateway, never an arbitrary silo.

### 5. Security model — what a compromised browser can and cannot do

Treat the browser as **hostile**: every privilege is re-checked server-side, per subscription and per
call.

- **Can:** read replicas of the grain types/keys its authenticated identity is authorized for; issue
  ordinary authorized `getGrain` calls; send malformed or forged frames at the gateway.
- **Cannot:**
  - replicate a grain type that is not server-marked eligible — default-deny, decided on the silo;
  - observe a key it is not authorized for — per-key check at the gateway gate;
  - mutate authoritative state except through authorized server-side grain calls — the replica is
    read-only and any local edit is view-only and never trusted;
  - break the single-activation guarantee — the directory CAS is server-side
    ([06](../06-grain-directory-and-placement.md));
  - read shared, secret, or transactional state — those types are never marked eligible
    ([ADR 0008](0008-cross-grain-transactions.md) grains excluded);
  - self-grant client placement or eligibility — the capability is server-declared metadata, not a
    client claim.

Net: a compromised browser is contained to exactly the data its authenticated identity was already
permitted to read, and cannot affect server state beyond the calls it was already allowed to make.

## Consequences

- **Rides existing seams.** Replication reuses the client→gateway transport, event streams, the etag
  version, and the call-filter pipeline; the only genuinely new transport surface is the replication
  message kind.
- **Introduces client identity.** A per-client session identity (today absent — only `clusterId`
  isolation exists) becomes a first-class concept on the gateway path. This is the main new building
  block and is reusable beyond replication (e.g. per-client auth on ordinary calls).
- **Read-only v1 sidesteps conflict resolution.** No optimistic/CRDT machinery is needed yet; the hard
  consistency work is deferred to a follow-up ADR with a clear seam.
- **Server stays authoritative.** Single activation and transactions are unweakened; the browser is a
  pure observer.
- **Per-browser cost.** Each replica is a live server-side subscription, bounded by disconnect
  teardown and by the per-key authorization the gate already enforces.
- **Safe by default.** Default-deny means a type is invisible to browsers until a developer
  deliberately marks it and the server authorizes the key — the trust boundary fails closed.
- **A path forward.** The boundary is designed so Layer 2 (browser-hosted grains) and writable client
  state slot in behind the same gate without redesigning trust.

## Scope boundary

- **Read-only replicas only.** Writable client state, optimistic local writes, and their
  reconciliation model (optimistic/etag vs CRDT) are deferred to a follow-up ADR.
- **No browser-hosted grains.** Layer 2 (running activations client-side) is designed-for — the
  catalog, scheduler and facet machinery are already host- and transport-agnostic — but not
  implemented here.
- **Transactional grains excluded.** [ADR 0008](0008-cross-grain-transactions.md) grains are
  ineligible by definition.
- **Offline / local-first deferred.** It needs writable replicas + a merge model.
- **No authentication scheme.** The ADR fixes the *seam* (session identity in preamble/request
  context, checked by the gateway gate) and the default-deny posture; the concrete auth mechanism is
  application-pluggable and out of scope.

## Alternatives considered

- **Direct browser-to-silo connections** (no gateway). Rejected: widens the trust surface to every
  silo; the gateway is the single, auditable trust boundary and already exists.
- **Client-declared eligibility / self-granted placement.** Rejected: violates the server-authoritative
  rule that is the whole point of the permission model — the browser is untrusted.
- **Full CRDT writable replicas in v1.** Rejected as premature: it front-loads the hardest problem
  before the trust boundary is proven; deferred behind the read-only v1.
- **Polling the server for state instead of a pushed subscription.** Rejected: it adds latency and
  load, defeating the latency-first motivation; the observer/stream push already exists.
- **Treating the browser as a trusted partial silo.** Rejected: the browser is untrusted by
  definition; any model that grants it silo trust is unsound.

## Implementation slices (design only — to be built later, test-first, as vertical slices)

1. **Client-session identity + gateway authorization seam.** Carry an authenticated client identity in
   the connection preamble / request context; a gateway incoming call filter
   ([ADR 0012](0012-grain-call-filters.md)) enforces a default-deny replication policy. First failing
   test: a replication request for an unmarked grain type is rejected at the gateway.
2. **Grain-type eligibility marker.** `browserReplication` on `GrainOptions`, recorded in the silo
   registry and read **server-side only**. Test: the marker is honoured from the registry and a client
   claim cannot override it.
3. **Read-view subscription protocol over WebSocket.** subscribe / snapshot / delta / resync frames
   on the existing transport ([ADR 0002](0002-websocket-transport.md)), built on event streams
   ([09](../09-event-streams.md)) and the state version/etag ([07](../07-persistence.md)). Test: the
   browser receives a snapshot then deltas, detects an induced gap and re-syncs, and converges with the
   authoritative activation.
4. **Subscription lifecycle and resource bounds.** Re-subscribe across server activation
   deactivation/migration; tear down on browser disconnect. Test: the activation migrates and the
   replica continues; a disconnect frees the server-side subscription.
5. **(Named only — follow-up ADRs.)** Layer 2 browser-hosted grains; writable client state with an
   optimistic/CRDT reconciliation model and offline support.
