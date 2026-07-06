# Design notes — parity gaps requiring a design pass

The parity-gap items in [`todo.md`](../todo.md) split into two groups:

1. **Targeted follow-ups** — small, single-package, mechanical. Tracked under "Parity follow-ups" in `todo.md` and not covered here.
2. **Design-first items** — touch public surface, span multiple packages, or have several defensible shapes. Each item below describes the problem, the Orleans reference, the current TS state, the realistic options with tradeoffs, and a recommended direction. The recommendations are starting points, not commitments.

Read [`deviations.md`](deviations.md) for what stays Orleans-faithful and what is deliberately TypeScript-idiomatic. Anything below that conflicts with `deviations.md` is wrong — flag it.

---

## 1. Cancellation tokens and per-call deadlines

**Problem.** A grain call cannot be cancelled. A hung downstream blocks the calling turn forever; `onDeactivate` cannot be aborted; graceful drain has no upper bound on time-to-quiesce. The client now backs off between failed gateways, but the cumulative `callTimeoutMs` still covers everything — backoff eats the caller's budget.

**Orleans.** Threads `CancellationToken` through the call chain; deactivation uses `cts.CancelAfter(DeactivationTimeout)`; the chain-reentrant grain context exposes `CancelToken` so sub-calls of a cancelled parent abort. `GrainCancellationToken` is a system-target backed by a sourceToken graph.

**TS today.** No `CancellationToken` analogue. `AbortSignal` is the natural Node primitive but is not threaded anywhere. Per-attempt deadline plumbing is also missing in `@tsva/client`.

**Options.**
- **(A) Pervasive `AbortSignal` parameter.** Every grain method gets an optional trailing `AbortSignal`. Idiomatic Node, but breaks the "grain method signature = interface" contract because callers can't see the signal in the interface declaration unless they add it manually.
- **(B) Context-bound `AbortSignal`.** Pull the signal off the grain runtime context (`AsyncLocalStorage`-backed). Method signatures stay clean; user code calls `runtime.signal` and chains it via `AbortSignal.any([userSignal, runtime.signal])`. Matches Orleans' implicit-via-context model.
- **(C) Deadline only, no cancellation.** Caller passes a deadline; the dispatcher rejects the response when the deadline expires; the activation's turn keeps running. Simpler but doesn't recover resources.

**Recommendation.** **(B)** — context-bound `AbortSignal` on the grain runtime, plus a per-attempt deadline in the dispatcher. Activation lifecycle and turn scheduler get their own internal signals that the runtime can compose. Avoids interface drift, matches Orleans semantics, and is the minimum that unblocks the next three items (back-pressure, TM keepalive, stream redelivery).

**Open questions.** How does cancellation interact with the durable-job RunId tombstone — does cancelling mid-run mark complete or leave the job for retry? (Orleans: leaves for retry.) Should `AbortSignal` reach into `onActivate` so a long activation can be force-killed? (Orleans: yes, via the deactivation timeout pattern reused for activation.)

---

## 2. Turn-scheduler back-pressure and deactivation timeout

**Problem.** Per-activation queue is unbounded. A grain that stops draining its queue grows memory without limit; `onDeactivate` has no upper bound so silo shutdown can stall on a single hung activation. There is no stuck-turn detection — an infinite loop in user code wedges the activation silently.

**Orleans.** `WorkItemGroup` has `MaxEnqueuedRequestsSoftLimit` (warn) and `MaxEnqueuedRequestsHardLimit` (reject with a transient rejection). `ActivationData` tracks the current turn's start time and deactivates if `MaxRequestProcessingTime` is exceeded. Deactivation uses `CollectionOptions.DeactivationTimeout` via `CancelAfter`.

**TS today.** Single unbounded queue per activation; no timing on individual turns; `activation.deactivate()` schedules `onDeactivate` as one turn with no timeout.

**Options.**
- **(A) Soft+hard queue limits + transient rejection.** New callers get a retryable rejection when the queue exceeds the soft limit and the activation is busy. Mirrors Orleans exactly.
- **(B) Stuck-turn watcher.** A periodic check (driven by the existing collector loop) reads the current turn's start timestamp; if `now - start > MaxRequestProcessingTime`, the activation is marked invalid and re-created. Combines with cancellation (#1) to actually interrupt the offending turn.
- **(C) Deactivation timeout.** `onDeactivate` runs under a deadline; on expiry, the activation is force-invalidated (state lost, message-center entries cleared, directory entry removed via the same path used for crashes).

**Recommendation.** Implement all three, in this order: (C) first (no dependency, immediately fixes shutdown stalls); then (A) once cancellation exists so the rejected caller can propagate; then (B) once cancellation exists so the stuck turn can actually be interrupted rather than orphaned.

**Open questions.** How does the soft limit interact with the dispatcher's caller-side retry — re-resolve the activation or retry on the same one? (Orleans: re-resolves.) Does a force-invalidated activation pre-rehydrate its persistent state, or restart cold? (Orleans: cold restart.)

---

## 3. Versioned serializer (polymorphism, schema evolution, cycles, `Map`/`Set`)

**Problem.** The 6-tag value codec silently degrades unknown types to plain objects. Field additions can be safe by accident but field renames, type changes, polymorphic hierarchies, `Map`/`Set`, and circular references are unsafe. This is the single largest blocker for rolling upgrades and event-sourced payloads (journaled state stays on disk across schema changes).

**Orleans.** Source-generated codecs with explicit `[Id(n)]` field tags; surrogate types for built-ins; polymorphism via type-discriminator tags; circular references via a session-scoped reference table; unknown types preserved on the wire so older readers can pass-through.

**TS today.** Runtime `@serializable` decorator, MessagePack default, no codec generation. The codec does not track field IDs, has no discriminator scheme, and has no reference table.

**Constraint.** `deviations.md` requires runtime registration, not source-gen. Whatever we build must be a runtime registry.

**Options.**
- **(A) Field-tagged runtime registry.** `@serializable({ id: 1 })` on the class, `@field(1)` on properties. The codec emits a `{ tag: typeId, fields: { 1: ..., 2: ... } }` shape (effectively protobuf-on-MessagePack). Field renames and re-orderings are safe; adding a field is safe; removing a field requires a tombstone (`@retired(3)`). Sketches well; adds decoration burden.
- **(B) JSON-Schema-driven evolution.** Each type registers a schema; the codec validates on read and runs explicit upgrade functions per version. Heavier authoring; better safety; very explicit migration story (matches the journaling field-retirement gap).
- **(C) Two-codec layering.** Keep the existing value codec for transient wire traffic; introduce a separate durable codec for journaled and persistent state with versioning. Reduces blast radius; risks divergence.

**Recommendation.** **(A)** with the discipline that journaled and persistent state require explicit field IDs (lint rule). Polymorphism via a registered type-discriminator (a string set on the codec, not a global). Cycles via a per-call reference table in the codec session. **(C)** is a tempting half-step but the analysis already shows two codecs is one too many.

**Open questions.** How are anonymous (object literal) types handled — fall back to current behaviour, or refuse? (Recommend refuse for durable state, allow for wire.) Do we keep MessagePack as the framing or move to a self-describing format for durable state?

---

## 4. Grain observers and `IGrainExtension`

**Problem.** Grains cannot push notifications back to clients. There is no surface for extension objects bound to a grain (Orleans uses this for cancellation tokens, management APIs, system targets). Several other items below depend on extensions.

**Orleans.** `CreateObjectReference<T>(observer)` creates a typed proxy the grain calls back through; `IGrainExtension` lets you bolt orthogonal call surfaces onto a grain (the management API, cancellation, durable-job receiver) without changing the grain's declared interface.

**TS today.** Stream handlers and broadcast subscriptions exist (one-way push), but no typed observer surface and no extension mechanism.

**Options.**
- **(A) Observer = client-hosted grain.** The client registers a grain identity backed by a local activation; grains call it like any other reference. Reuses the entire grain/dispatcher machinery; needs reverse-direction routing through the gateway.
- **(B) Observer = typed callback through a dedicated channel.** A new message kind (`observer-call`) carries the typed payload through the existing correlation table without going through the directory. Simpler to implement; doesn't compose with reentrancy or filters as cleanly.
- **(C) Extensions only, observers as a sugar layer.** Build `IGrainExtension` first (a registry of orthogonal interfaces per activation); then implement observers as a built-in extension. Composes well with cancellation (#1) and durable-jobs in-grain dedup.

**Recommendation.** **(C)** — extensions are the more general primitive and several other gaps need them. Observers fall out as a thin wrapper.

**Open questions.** How does an extension survive activation migration — re-bound on rehydrate, or torn down? Does the gateway need a reverse-direction routing table or do clients open a duplex connection? (Recommend duplex; the WebSocket transport already supports it.)

**Chosen design (observer half of option A, kept faithful to Orleans).** An observer is a client-hosted callback addressed by a client-typed `GrainId`: reserved type `$client`, key `clientKey + "+" + scope` (`packages/core/src/client-grain-id.ts`). `clientIdOf` strips the scope so any observer routes back to its owning client, while distinct observers still hash distinctly. Reference reduction/rehydration reuse the existing `GRAIN_REF` marker + serializer callback; rehydrate must build the reference from the wire `GrainId` as-is (`GrainFactory.getReference`) rather than re-resolving the type from the interface, so the `$client` type survives. The subscriber-set bookkeeping is `ObserverManager` (`packages/core/src/observer-manager.ts`).

Routing is faithful to Orleans rather than shortcutting on this codebase's directly-addressable client listeners: a client opens a **duplex** connection to its gateway and is reachable **only** through it. This requires a transport **accept hook** — `listen(address, onMessage, onAccept?)` surfacing the accepted duplex `Connection`, and an optional `clientId` on `ConnectionPreamble`. A gateway records `clientId → held connection` on accept (Orleans `RecordOpenedConnection`); a gossiped **client directory** maps `clientId → [gateway silos]`; a grain's silo resolves an observer's client to a gateway and forwards; the gateway's **deliver-to-proxy** sends over the held connection, with a reply-routing cache for client→observer request/response. The change to the core `Transport` abstraction is strictly additive — silo↔silo messaging keeps its reverse-connection replies; only clients use the held connection.

---

## 5. `StatelessWorker` enforcement

**Problem.** The option is parsed but never honored. Multi-activation-per-key placement (a major Orleans throughput pattern) is effectively absent.

**Orleans.** `StatelessWorkerPlacement` allows up to `MaxLocalWorkers` (default = `Environment.ProcessorCount`) activations per silo per key, with no directory entry — calls go to whichever local worker is least busy. The catalog tracks the local pool.

**TS today.** The placement strategy file exists but `Catalog.activateLocal` and the directory CAS path assume one activation per `(type, key)`. The `stateless` option in `GrainOptions` is read but the catalog and directory don't branch on it.

**Options.**
- **(A) Per-silo local pool bypassing the directory.** Stateless grains skip directory `register`/`lookup` entirely; the local dispatcher round-robins across a bounded pool. Matches Orleans semantics. Requires the catalog to maintain a pool keyed by `(type, key)`.
- **(B) Multiple directory entries per key.** Register N entries per stateless grain; the directory returns one at random. Simpler dispatcher; pollutes the directory; doesn't get cross-silo locality.

**Recommendation.** **(A)**. The catalog already has the activation map keyed by grain ID; widen the value to a pool when the grain is stateless. The directory contract is unchanged.

**Open questions.** Does the pool size depend on `node:os.availableParallelism()` or the silo's configured concurrency? Should the dispatcher prefer-local even harder for stateless workers (skip the placement director entirely)?

---

## 6. `@readOnly` runtime mutation check

**Problem.** `@readOnly` and `alwaysInterleave` are advisory. Authors can declare a method `@readOnly` and mutate state inside it; nothing flags this in dev. The cost will surface as a heisenbug under load.

**Orleans.** `[ReadOnly]` is used by the scheduler to allow interleaving but the .NET runtime doesn't enforce non-mutation either — Orleans relies on convention.

**TS today.** Same: convention only.

**Options.**
- **(A) Dev-mode `Proxy` over `this.state`.** When the activation is running a `@readOnly` turn, wrap mutable state in a `Proxy` that throws on `set`/`deleteProperty`. Off in production. Catches the obvious cases; doesn't catch mutations to nested objects unless the proxy is deep.
- **(B) Snapshot+compare.** Take a structural hash of state at turn start; compare at turn end; warn if changed. Cheap; only catches mutations that actually persisted; runs at end of turn so the bug has already happened.
- **(C) Don't enforce.** Match Orleans. Document the convention more loudly.

**Recommendation.** **(A)** in dev mode only, behind a flag (default on in test, off in prod). Shallow proxy on the persistent-state value and on `this`'s own enumerable properties. Cheap enough for tests; meaningful diagnostic.

**Open questions.** Does the same proxy catch `@reentrant` violations? Where does the dev-mode flag live — silo options, environment variable, or the test harness?

---

## 7. Runtime instrumentation breadth

**Problem.** `@tsva/observability` defines meters and exporters; the runtime doesn't emit on them. Operators have no visibility into activation latency, storage tails, directory hit rates, message flow, or exception attribution on spans.

**Orleans.** Runtime emits a curated set of meters (activation count, turn duration, queue depth, storage roundtrip, directory cache hit, message backlog, etc.) and decorates spans with `exception.type`/`message`/`stacktrace`.

**TS today.** Call-filter seam emits per-call counters and histograms. Inside the runtime, almost nothing is instrumented.

**Options.**
- **(A) Instrument every package directly.** Each package imports the observability registry and calls it. Couples packages tightly to `@tsva/observability`.
- **(B) Event-bus.** The runtime emits structured events; `@tsva/observability` subscribes and translates to OTel. Looser coupling; aligns with the project's reducer/event-driven convention; one new abstraction.

**Recommendation.** **(B)**, with the event names and payloads frozen as a public surface (changing them is a breaking change for operators). Start with the meters the analysis called out: activation create/destroy, turn start/end, storage read/write/clear roundtrip, directory lookup hit/miss/stale, message dispatch/reject/timeout, reminder fire, stream pull/deliver/fail, transaction begin/commit/abort.

**Open questions.** Should the bus be sync or async (microtask-deferred)? Sync risks back-pressure into the runtime; async risks losing events on crash. (Recommend sync but cheap.)

---

## 8. Directory hardening (versioned range locks, ACK'd handoff, crash-broadcast recovery)

**Problem.** Recovery is best-effort one-shot pull. If the pull fails, entries are silently lost. Handoff snapshots are retained indefinitely if the successor never pulls. There is no versioned coordination across partition transitions, so a register racing with a view change can see incomplete state.

**Orleans.** `RangeChangeNotification` wedges block range access until the prior owner has finished transfer; `GrainDirectoryHandoffManager` orchestrates ACK-and-delete; on crash, all live silos broadcast recovery requests for ranges they newly own.

**TS today.** Single post-join `recovery` Promise gates owned reads; one `recover` RPC per source; no ACK or expiration on handoff entries.

**Constraint.** `deviations.md` explicitly says K8s is the membership SSOT and we don't reintroduce gossip. Whatever we build must layer on K8s view changes, not replace them.

**Options.**
- **(A) Versioned range gate.** Replace the single `recovery` Promise with a per-range gate keyed by `(membershipVersion, rangeId)`. On view change, opens a gate per range that must transfer; closes when the source acknowledges drain or a timeout fires (rebuild lazily on miss). Targets the real race window without adding broadcast traffic.
- **(B) ACK'd handoff with tombstone TTL.** Source retains handoff entries until the successor sends an explicit ACK; entries TTL out after `handoffTimeoutMs` if no ACK. Layers cleanly on top of (A).
- **(C) Broadcast crash recovery.** On crash, every live silo enumerates ranges it newly owns and pulls from any silo that might have a copy. Matches Orleans but reintroduces a quasi-gossip pattern. Skip unless lazy reactivation proves insufficient.

**Recommendation.** **(A) + (B)**. Skip (C) until we have evidence lazy reactivation is losing user-visible state. Add a metric (per item #7) for "lookups that resolved via lazy reactivation" so we can quantify the gap before adding broadcast.

**Open questions.** What is the right `handoffTimeoutMs` default? (Orleans-equivalent is on the order of seconds.) How does the range gate interact with `StatelessWorker` (which bypasses the directory entirely)?

---

## 9. K8s active peer probing

**Problem.** `@tsva/clustering-k8s` relies entirely on the kubelet's readiness probe. A silo whose readiness endpoint still returns 200 but whose grain dispatcher hangs (deadlock, exhausted thread pool equivalent, GC stall) is undetectable.

**Orleans.** Probe-graph: every silo actively probes a subset of peers and gossips suspicions; majority agreement marks a silo dead.

**Constraint.** `deviations.md` makes K8s the membership authority. We don't reintroduce gossip. So "active probing" here is local: a silo can detect *its own* hang or *its neighbour's* hang, but the membership truth still comes from K8s.

**Options.**
- **(A) Self-probe.** Each silo periodically calls a no-op system grain on itself with a short deadline. On timeout, fail readiness (kubelet then removes the endpoint). Adds latency cost but is fully consistent with K8s as SSOT.
- **(B) Peer-probe + K8s deletion.** Each silo probes N peers; on probe failure, calls the K8s API to mark the pod's endpoint not-ready (requires RBAC for endpoint mutation, fragile across K8s versions). Closer to Orleans semantics, fights the platform.
- **(C) Application-level dispatch timeout with retry on peer.** No active probing; the dispatcher's per-call deadline (depends on #1) treats repeated timeouts to a peer as a localised dead-peer signal and falls back to a different replica or to lazy reactivation. The cluster view is still K8s.

**Recommendation.** **(A) + (C)**. Self-probe is cheap, K8s-native, and catches the precise failure mode the analysis flagged. Dispatch-level dead-peer fallback (C) covers the case where the peer's self-probe is itself broken. Skip (B) — fighting the platform.

**Open questions.** What does the self-probe call (echo grain? activation-collector ping?), and what's the right cadence (Orleans defaults around 10s)? Should the readiness gate distinguish "warming" from "draining" so a probe failure during drain doesn't immediately rip the silo out?

---

## How to use these notes

Each item is a starting point. The right cadence is:

1. Pick one. Re-read the relevant Orleans source (`~/repos/orleans/src`) and the corresponding TS package.
2. Decide whether the recommendation here still holds. If not, write the dissent in this file and update the option list — the doc is meant to track *current best thinking*, not historical state.
3. Spike the smallest end-to-end slice that proves the design. Land it behind a feature flag if the public surface changes.
4. Promote to the parity items in `todo.md` once the slice is real code.

Items with the strongest dependencies on each other: **#1 (cancellation)** unblocks **#2 (back-pressure)**, **TM keepalive**, and stream redelivery backoff. **#4 (extensions)** unblocks several smaller items. Doing #1 first is almost always the right call.
