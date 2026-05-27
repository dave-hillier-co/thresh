# ADR 0017 — Browser state replication and browser-hosted grains

- Status: Proposed (design only — a beyond-parity direction, no implementation)
- Context docs: [13 — Roadmap (Beyond parity)](../13-roadmap-and-phases.md),
  [11 — External client](../11-public-api-and-examples.md), [09 — Event streams](../09-event-streams.md),
  [07 — Persistence](../07-persistence.md), [ADR 0002](0002-websocket-transport.md),
  [ADR 0008](0008-cross-grain-transactions.md), [ADR 0012](0012-grain-call-filters.md)

> Orleans references: grain **observers** (`Orleans.Core.Abstractions/IGrainObserver.cs`,
> `Orleans.Runtime/Utilities/ObserverManager.cs`) — a server pushing updates to an untrusted,
> disconnectable subscriber — are the closest prior art, and event streams ([09](../09-event-streams.md))
> are our equivalent. Orleans has **no** browser-hosted grains or client-placement trust model; that
> layer is a deliberate extension *beyond* Orleans.

## Context

A post-parity direction: **replicate grain state to the browser as a live read-view, and eventually
run permitted grains client-side under a server-enforced policy.** Three layers of ambition: (1) live
read-views (server is source of truth, browser holds a derived replica); (2) browser-hosted grains;
(3) a **permission model** — "permitted to run there" is a trust/authority classification, not
placement, because the browser is untrusted.

This ADR optimizes **latency-first** and commits v1 to **read-only replicas only**. Read-only is the
decisive scoping choice: with the server activation as sole writer there is **no write-conflict
problem**, so optimistic/CRDT reconciliation and offline are deferred to a follow-up — letting v1
establish the trust boundary (the novel, security-critical part) against a simple eventually-consistent
read model. It builds on existing pieces: the client→gateway WebSocket path ([11](../11-public-api-and-examples.md),
[ADR 0002](0002-websocket-transport.md)), event streams as the observer analogue
([09](../09-event-streams.md)), the `PersistentState` etag as a version ([07](../07-persistence.md)),
and the grain-call-filter seam for the gate ([ADR 0012](0012-grain-call-filters.md)). The one missing
piece is **per-client identity** (today connections isolate only by `clusterId`); this ADR specifies
that seam without fixing an auth scheme.

## Decision

Adopt a **server-authoritative, read-only live read-view** (v1) on the existing client→gateway
WebSocket path, gated by a default-deny eligibility marker enforced on the silo. Browser-hosted grains
and writable client state are designed-for but deferred to later ADRs behind the same boundary.

**Eligibility / trust boundary (server-authoritative).** A grain *type* opts in via a
`browserReplication` marker on `GrainOptions`, recorded in the per-silo registry and **never** asserted
by the client; **default-deny** (unmarked types are invisible; transactional grains
([ADR 0008](0008-cross-grain-transactions.md)) are ineligible — their trusted-single-activation
assumptions are incompatible with an untrusted replica). A browser's request to open a replication
subscription passes an **incoming call filter on the gateway** ([ADR 0012](0012-grain-call-filters.md))
that confirms the type is replicable and authorizes *this client* for *this key* (per-key, via a policy
callback). The client **session identity** rides the connection preamble / request context; the
concrete auth scheme is application-pluggable and out of scope.

**Replication / consistency.** The single authoritative activation (directory CAS,
[06](../06-grain-directory-and-placement.md)) stays the sole writer. Once admitted, the gateway holds a
server-side subscription to the grain's state changes (modeled on streams) and the browser gets a
**snapshot on subscribe** then **versioned deltas**. Each carries a monotonic version (the etag); since
delivery is at-least-once, the browser dedups by version and, on a gap, requests a **re-snapshot**. The
replica is **per-grain monotonic and eventually consistent**; stale reads are acceptable for
view-models, and there is no conflict because the browser cannot write it. Subscriptions survive
activation deactivation/migration by re-subscribing (the chat durable-resume pattern) and tear down on
disconnect.

**Transport.** Reuse the WebSocket transport unchanged; add a **replication message kind** (subscribe /
snapshot / delta / resync) to the envelope ([04](../04-messaging-and-serialization.md)), multiplexed on
a subscription id. The gateway is the on-wire trust boundary; the browser reaches only the gateway.

**Security posture.** Treat the browser as hostile; re-check every privilege server-side per
subscription and per call. A compromised browser **can** read replicas it is authorized for and make
authorized `getGrain` calls; it **cannot** replicate an unmarked type, observe an unauthorized key,
mutate authoritative state except through authorized calls, break single-activation, read
shared/secret/transactional state, or self-grant eligibility. It is contained to exactly the data its
identity already had.

## Consequences

- Rides existing seams (transport, streams, etag version, call-filter pipeline); the only new transport
  surface is the replication message kind.
- Introduces **per-client session identity** on the gateway path — the main new building block, reusable
  for per-client auth on ordinary calls.
- Read-only v1 sidesteps conflict resolution; the server stays authoritative; per-browser cost is one
  live subscription, bounded by disconnect teardown.
- **Fails closed**: a type is invisible to browsers until deliberately marked and the key authorized.
- The boundary is designed so Layer 2 (browser-hosted grains) and writable state slot in behind the
  same gate.

## Scope boundary & alternatives

Out of scope: writable client state and its reconciliation model, browser-hosted grains, offline /
local-first, and a concrete authentication scheme (the ADR fixes only the seam + default-deny posture);
transactional grains are excluded by definition. Rejected alternatives: direct browser-to-silo
connections (widens the trust surface — the gateway is the single auditable boundary); client-declared
eligibility (violates server-authoritative); full CRDT writable replicas in v1 (front-loads the hardest
problem); polling instead of pushed subscriptions (adds latency); treating the browser as a trusted
partial silo (unsound).

## Implementation slices (design only — later, test-first)

1. **Client-session identity + gateway authorization seam** — identity in the preamble/request context;
   a default-deny gateway filter. First test: a replication request for an unmarked type is rejected.
2. **Eligibility marker** — `browserReplication` on `GrainOptions`, server-side only; a client claim
   cannot override it.
3. **Read-view subscription protocol** — subscribe/snapshot/delta/resync on the existing transport,
   built on streams + the etag version; the browser snapshots, applies deltas, detects an induced gap,
   re-syncs, and converges.
4. **Subscription lifecycle** — re-subscribe across activation migration; tear down on disconnect.
5. **(Follow-up ADRs)** — browser-hosted grains; writable client state with optimistic/CRDT
   reconciliation and offline.
