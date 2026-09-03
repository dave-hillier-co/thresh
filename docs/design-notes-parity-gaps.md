# Design notes — parity gaps requiring a design pass

> **Historical design archive.** These notes were written while the 2026 parity backlog was still
> open. The recommendations remain useful design provenance, but many sections now describe shipped
> work rather than current gaps. For current status, use [`EPICS.md`](../EPICS.md) and [`todo.md`](../todo.md):
> the 2026-07-24 burn-down delivered cancellation/deadlines, scheduler back-pressure, observability
> breadth, serializer versioning, stream failure handling, directory hardening, transaction
> deadlines/keepalive, durable-job `RunId` dedup/claim ramp-up, and full-facet `@readOnly` guard
> coverage.

The parity-gap items in [`todo.md`](../todo.md) split into two groups:

1. **Targeted follow-ups** — small, single-package, mechanical. Tracked under "Parity follow-ups" in `todo.md` and not covered here.
2. **Design-first items** — touch public surface, span multiple packages, or have several defensible shapes. Each item below describes the problem, the Orleans reference, the current TS state, the realistic options with tradeoffs, and a recommended direction. The recommendations are starting points, not commitments.

Read [`deviations.md`](deviations.md) for what stays Orleans-faithful and what is deliberately TypeScript-idiomatic. Anything below that conflicts with `deviations.md` is wrong — flag it.

---

## 1. Cancellation tokens and per-call deadlines

**Problem.** A grain call cannot be cancelled. A hung downstream blocks the calling turn forever; `onDeactivate` cannot be aborted; graceful drain has no upper bound on time-to-quiesce. `@thresh/client`'s `callTimeoutMs` is now a cumulative wall-clock budget across gateway failover — each retry attempt (and the backoff between attempts) draws down the same budget rather than each getting a fresh one — but nothing downstream of a single attempt (the grain's own turn, `onDeactivate`) can be aborted mid-flight.

**Orleans.** Threads `CancellationToken` through the call chain; deactivation uses `cts.CancelAfter(DeactivationTimeout)`; the chain-reentrant grain context exposes `CancelToken` so sub-calls of a cancelled parent abort. `GrainCancellationToken` is a system-target backed by a sourceToken graph.

**TS today.** No `CancellationToken` analogue. `AbortSignal` is the natural Node primitive but is not threaded anywhere.

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

**Extension design (option C, the general primitive).** An `IGrainExtension` binds an orthogonal method surface to an activation, dispatched by interface rather than on the grain instance — the same interfaceId-keyed dispatch the activation already uses for stream/broadcast/durable-job/transaction-resource "system extensions" (`ActivationData.callMethod`). Pieces: an `extension: true` marker on the `GrainInterface` so the activation distinguishes an uninstalled extension (→ `GrainExtensionNotInstalledException`) from an unknown method; a per-activation `Map<interfaceId, object>` registry with get-or-set binding; a `callMethod` branch (bound → invoke on the extension object; a silo-registered auto-install factory → install then invoke; known-but-unbound → throw); a `GrainRuntime.getOrSetExtension(interfaceId, factory)` for a grain's `InstallExtension`; and a silo-builder `addGrainExtension(interfaceId, factory)` for auto-installed extensions. `AsReference<TExtension>()` reuses the existing `GRAIN_REF_CAST` (same grainId, extension interfaceId). Observers are a special case of this hosting pattern; generic-grain extensions stay gapped under generics.

**Cancellation design (cooperative, on the extension substrate).** Orleans' `GrainCancellationToken` is cooperative — the callee observes the token and stops itself; there is no thread interruption — which maps directly onto JS `AbortController`/`AbortSignal`. A `GrainCancellationTokenSource` holds a `tokenId`, an `AbortController`, and the set of target grain ids the token has been sent to; its `.token` is a `GrainCancellationToken` (caller-side wrapping the source's signal). The token serializes (value-codec `$thresh:"cancellationToken"`) to `{ tokenId, cancelled }` and decodes to a placeholder; the target activation, before invoking the grain method, replaces each placeholder arg with a live token bound to a per-`tokenId` `AbortController` held by an auto-installed `ICancellationSourcesExtension` (pre-aborted if the placeholder was already cancelled, covering the cancel-before-arrival race). `source.cancel()` aborts the local controller and calls `cancelRemoteToken(tokenId)` on each target's extension (a normal grain-extension call), aborting the callee's controller so its awaited work rejects and the grain call fails with a cancellation error. Dispatching a request carrying a token records the target on the source. Execution-context / task-scheduler-context cases are .NET-specific and stay excluded; JS's cooperative-only model means a callee that never observes its token is not force-cancelled (as in Orleans).

**Client-side cancellation (for observers).** A grain can forward a cancellation token to a client-hosted observer; cancelling it must reach the observer, which is not an activation and so has no cancellation extension. The client gains the activation-side binding, mirrored: a per-client `Map<tokenId, AbortController>`; when it dispatches to a hosted object it binds each `CancellationTokenPlaceholder` arg to a live token backed by that store (pre-aborted if already cancelled); and it handles an inbound `ICancellationSourcesExtension.cancelRemoteToken(tokenId)` request by aborting the store's controller rather than dispatching it to an object. Propagation reuses the built machinery unchanged — the grain's forward records the observer as a target on the token's source (the source survives the in-process test→grain hop), and `source.cancel()` routes `cancelRemoteToken` to the observer's client through the ordinary observer routing (`routeToClient` → deliver-to-proxy).

**Cascading cancellation (client-originated / multi-hop).** The original target-tracking followed the token's source object, which only survives in-process hops — so a token forwarded through a *second* grain (after a serialization boundary, e.g. a client→grain call) could not be reached by `cancel()`. Orleans handles this by cascading: each hop's cancellation infrastructure tracks the grains *it* forwarded the token to, and cancellation propagates hop-by-hop. Mirrored here: `GrainCancellationToken` carries an `onDispatchToTarget(target)` hook — a caller-side token records the target on its source (as before); a callee-side bound token records it as a *forwarded target* on its activation's `CancellationSourcesExtension`. `cancelRemoteToken(tokenId)` then aborts the local controller **and cascades** to each forwarded target (via an injected canceller that builds an extension reference routed cluster-wide), idempotently. So `source.cancel()` need only reach direct targets; each target propagates onward. This un-gaps the client-originated `In/InterSiloClientCancellationTokenPassing` cases. Token-callback registration is separate and already built (`GrainCancellationToken.register`).

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

**Problem.** `@thresh/observability` defines meters and exporters; the runtime doesn't emit on them. Operators have no visibility into activation latency, storage tails, directory hit rates, message flow, or exception attribution on spans.

**Orleans.** Runtime emits a curated set of meters (activation count, turn duration, queue depth, storage roundtrip, directory cache hit, message backlog, etc.) and decorates spans with `exception.type`/`message`/`stacktrace`.

**TS today.** Call-filter seam emits per-call counters and histograms. Inside the runtime, almost nothing is instrumented.

**Options.**
- **(A) Instrument every package directly.** Each package imports the observability registry and calls it. Couples packages tightly to `@thresh/observability`.
- **(B) Event-bus.** The runtime emits structured events; `@thresh/observability` subscribes and translates to OTel. Looser coupling; aligns with the project's reducer/event-driven convention; one new abstraction.

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

> **Option (A) shipped 2026-09-02**: the built-in `ISiloProbe` grain (prefer-local, immovable) plus
> `SelfProbeWorker` in `@thresh/hosting` — a `dispatcherResponsive` readiness signal that flips
> after 3 consecutive missed self-probes and recovers on success. Option (C), the dispatch-level
> dead-peer fallback, is designed but not built: the design below is decided and implementable
> as written, not a survey.

**Problem.** `@thresh/clustering-k8s` relies entirely on the kubelet's readiness probe. A silo whose readiness endpoint still returns 200 but whose grain dispatcher hangs (deadlock, exhausted thread pool equivalent, GC stall) is undetectable.

**Orleans.** Probe-graph: every silo actively probes a subset of peers and gossips suspicions; majority agreement marks a silo dead.

**Constraint.** `deviations.md` makes K8s the membership authority. We don't reintroduce gossip. So "active probing" here is local: a silo can detect *its own* hang or *its neighbour's* hang, but the membership truth still comes from K8s.

**Options.**
- **(A) Self-probe.** Each silo periodically calls a no-op system grain on itself with a short deadline. On timeout, fail readiness (kubelet then removes the endpoint). Adds latency cost but is fully consistent with K8s as SSOT.
- **(B) Peer-probe + K8s deletion.** Each silo probes N peers; on probe failure, calls the K8s API to mark the pod's endpoint not-ready (requires RBAC for endpoint mutation, fragile across K8s versions). Closer to Orleans semantics, fights the platform.
- **(C) Dispatch-level dead-peer fallback.** No active probing; the messaging layer treats repeated *transport silence* toward a peer as a local, private suspicion, and the dispatcher uses it to fail fast and to steer *new* placement elsewhere. The cluster view is still K8s. The original wording for (C) — "falls back to a different replica or to lazy reactivation" — is wrong and is superseded by the design below: there is no replica, and lazy reactivation of a grain whose activation may still be live is a double activation.

**Recommendation.** **(A) + (C)**. Self-probe is cheap, K8s-native, and catches the precise failure mode the analysis flagged. Dispatch-level dead-peer fallback (C) covers the case where the peer's self-probe is itself broken. Skip (B) — fighting the platform.

### Decision — the shape of (C)

**Local peer suspicion, fed only by transport silence, consumed only by fail-fast and placement-candidate suppression.** Re-placement authority stays exclusively with membership plus the directory CAS. Suspicion is a private opinion about *where to send new work*, never a liveness fact, never a `SiloStatus`, and it never crosses the wire.

Concretely, three consequences and nothing else:

1. **Fail fast** on an application call to a suspect peer, instead of hanging for `callTimeoutMs`. Note this covers calls to *existing* activations on that peer as well as new placements, since `routeTo` sends through the same `remote.send`; the decay argument below depends on that.
2. **Suppress** the peer from the placement candidate seed and from cluster-load gathering, so new grains and rebalancer/repartitioner moves do not land on it.
3. **Report** it — a counter and a diagnostics event, which is the only thing that tells an operator "my peers cannot reach this pod while the kubelet still calls it ready".

What (C) deliberately does **not** buy: it does not recover grains already activated on the wedged peer. Those calls fail fast and keep failing until the pod's endpoint goes away. Eviction remains option (A)'s job (the peer's own readiness) or the kubelet's. Buying faster eviction needs (B)'s endpoint mutation, which is rejected above.

**Why the window is narrow, and why the sensor still earns its place.** (C) covers exactly `[peer's dispatcher wedged] ∧ [peer's endpoint still ready]` — i.e. the case where the peer's *own* self-probe failed to fire. It is a backstop for a backstop. The sensor is ~20 lines and is independently valuable as observability, so it lands with suppression behind a default-off flag and is enabled on evidence — the same discipline item #8 applies to lazy reactivation.

### Signal: silence only, observed in the messaging layer

Every cross-silo send funnels through two chokepoints in `ClusterNode`, and both already tag the peer on the correlation entry (`correlation.register(id, callTimeoutMs, silo.toString())`): `sendRemote` (application calls) and `sendSystemMessage` (the single implementation behind directory, migration, silo-ping, manifest, load/stats, force-collect, provider-control and repartition-exchange traffic). `deliverToProxy`/`proxyRequestToClient` register *without* a peer tag — client legs, out of scope, since a client is neither a placement nor a directory participant.

The sensor is therefore fed by hooks in `@thresh/messaging`, **not** by classifying errors at the call site:

- **`observeSilence(peer)`** — from a new `onTimeout(peer)` hook in `CorrelationTable.register`'s timer callback (the one that rejects with `GrainCallTimeoutError`). This is the only timeout that counts: `callTimeoutMs` elapsed with no bytes back at all.
- **`observeUnreachable(peer)`** — from the existing `ConnectionManager` connection-lost callback (already wired in `ClusterNode`'s constructor to `correlation.rejectFor`), plus a rejected dial from `connections.get(silo)`.
- **`observeAlive(peer)`** — from a new `onComplete(peer)` hook in `CorrelationTable.complete`. Any response resets the counter.

**Why hooks and not call-site classification.** `rejectFor` synthesises `RejectionError(..., "unknownTarget")` for a lost connection, and `"unknownTarget"` is also a rejection a perfectly live peer sends (`routeToClient` with no gateway, the partition's dangling-pointer paths). At the `await pending` in `sendRemote` the two are indistinguishable — the synthesised error is thrown *before* `interpretResponse` ever runs. Feeding suspicion from the connection-lost callback, which carries the peer address, removes the ambiguity by construction.

**Any answer proves liveness.** `observeAlive` fires on every `responseKind`: `"success"`, `"error"` (a grain method threw), and every rejection kind — `siloDraining`, `noActivation`, `unknownTarget`, `overloaded` (surfaced as `GatewayTooBusyException`), `deserialization`, `staleView`, `noCandidates`. A peer that can serialise and send a rejection is not hung. Counting `overloaded` is precisely how you starve a hot-but-healthy peer, which is the failure mode to avoid.

**Two things must never count, and both are excluded for free by instrumenting `CorrelationTable` rather than the call site:** per-call response deadlines (`defaultResponseTimeoutMs` / `options.responseTimeout`, raced client-side in `GrainFactory.raceResponseDeadline`, and `withCallOptions({ deadlineMs })`) never touch the correlation timer, so a 200ms application deadline firing against a 500ms peer registers nothing; and a caller-side cancellation is the caller's clock, not the peer's health.

### State, threshold, decay

`PeerSuspicion` lives in `@thresh/runtime` (`peer-suspicion.ts`), keyed by `SiloAddress.toString()` — which includes `podUid`, so a restarted pod starts clean. Per peer: `{ consecutiveSilences, firstSilenceAt, firstSeenAt, suspectUntil }`. Clock is the injected `TimeProvider`, the same discipline as `SelfProbeWorker`, so it is deterministic under a fake clock.

- **Trip** requires *both* `consecutiveSilences >= missedThreshold` (default 3, matching `SelfProbeWorker`'s `DEFAULT_MISSED_THRESHOLD` and Orleans `NumMissedProbesLimit`) **and** `now - firstSilenceAt >= minSuspicionMs` (default `2 × callTimeoutMs`). The dwell guard exists because timeouts are not serialised: ten calls in flight to one peer all expire in the same millisecond and would otherwise trip a count-only threshold off a single blip.
- **`consecutiveSilences` is epoch-scoped, and the dwell guard does not substitute for that.** Timeouts fire in *registration* order, not in "is the peer alive now" order: a call registered before an `observeAlive` can time out after it, so a batch of pre-reset timeouts would otherwise re-trip suspicion moments after a proven-live response — and the dwell guard makes that *worse*, since those stale timeouts carry an old `firstSilenceAt` and so arrive already past `minSuspicionMs`. So: `PeerSuspicion` keeps a per-peer `epoch`, `observeAlive` bumps it (and clears the counter), `CorrelationTable.register`'s `onTimeout(peer, epoch)` hook carries the epoch stamped when the call was registered, and a timeout from a stale epoch is discarded rather than counted. Only silences of calls registered *after* the last proof of life count. Same rule for `observeUnreachable`, which can also land late (the connection-lost callback drains a queue).
- **Minimum trip latency, stated as an accepted limit.** With the defaults (`missedThreshold` 3, `callTimeoutMs` 30s, `minSuspicionMs` = `2 × callTimeoutMs` = 60s), a peer cannot be suspected sooner than 60s after its first silence, and — because the three silences must be same-epoch, i.e. concurrent or strictly after the last success — a peer carrying less than three concurrent in-flight application calls needs three *serial* timeout windows, ~90s. A low-traffic peer may never accumulate three at all and is then simply never suspected. That is the intended trade: (C) is a backstop whose false positives cost placement quality, so it is deliberately slower and blinder than the self-probe (A), which detects a wedge in ~30s regardless of traffic.
- **Decay is a TTL, not a probe gate.** Tripping sets `suspectUntil = now + suspicionTtlMs` (default 30s ≈ three self-probe cycles). It expires with no evidence required, and this is the adjudication between the two candidate recovery designs. The reason is stronger than "suppression removes traffic": consequence #1 fails fast in `sendRemote` *before* `connections.get`, and `routeTo` sends calls to existing activations through that same `remote.send` (`distributed-dispatcher.ts:239`), so **while a peer is suspect, application traffic to it provides no recovery evidence by construction** — zero application calls reach the peer, so no `observeAlive` can come from one. What survives fail-fast is `sendSystemMessage` traffic, which is where recovery evidence actually comes from: directory ops to the peer as a range owner (routed by the ring, never suppressed), `pingSilo`, manifest fetches, migration and handoff/recovery pulls, force-collect and provider-control. Cluster-load queries are the one system-message source the design removes, and only that one. That residual channel is traffic-dependent and may be empty for a quiet peer, which is exactly why the deadline is a hard TTL rather than probe-gated: a probe-gated flag with no guaranteed probe is absorbing state. Any `observeAlive` from the surviving channel clears counter and deadline immediately and bumps the epoch.
- **No half-open probe in v1.** Both obvious probe targets are unsound. `pingSilo` is answered by `handleSiloPingRequest` straight off the message loop and never enters the dispatcher, so it would report a wedged silo as recovered. `getGrain(ISiloProbe, peer.ringKey).ping()` is only a *peer* probe if the directory entry already points at the peer: the probe grain is `preferLocal` + `immovable: "any"`, so an unregistered one activates locally and returns a false "healthy". A confirmation probe would need a directory-lookup guard first; it is not worth the surface until the metric says false positives are real.
- **Cold-start grace.** A peer whose `firstSeenAt` is younger than `coldStartGraceMs` is never suspected. A peer mid-`start()` is not dialable, and a rollout would otherwise suspect every new pod exactly when placement is most sensitive. `PeerSuspicion` subscribes to `membership.updates()` to stamp `firstSeenAt` and to evict entries for silos out of the view (bounded memory across a crash-looping pod), plus a hard entry cap.
- **Floors.** Never suspect the local silo — that is (A)'s job. Never suppress if suppression would empty the candidate set. Cap concurrent suppression at a minority, `floor((n-1)/2)`, most-recently-failing first; at `n = 2` the cap is 0, because with one peer "I cannot reach anyone" is indistinguishable from "I am the broken one".
- **Drain.** Mirror `SelfProbeWorker`'s `health.isDraining()` guard: no suspicion trips while this silo is draining, and a peer whose membership status is `draining` is not suspected — it answers `siloDraining` anyway, and `stop()` deactivates before closing the listener precisely so `onDeactivate` hooks can still make cross-silo calls.

### What the dispatcher does

**Fail fast, on application calls only.** In `sendRemote`, before `connections.get(silo)`, a suspect peer throws a new `RejectionError(msg, "siloUnavailable")` — a new member of `RejectionKind` in `@thresh/core`'s `errors.ts`, Orleans' `SiloUnavailableException`. It converts a 30s hang into an immediate, classifiable, retriable error and stops calls piling up against a wedged peer.

`sendSystemMessage` does **not** fail fast. This is the second explicit adjudication: fail-fast there is superficially attractive (a wedged directory-range owner becomes unavailable faster and louder) but the same code path carries `beginRecovery`'s `recover` pulls and handoff traffic, where a fast local failure silently drops entries that item #8's ACK'd handoff is specifically designed to preserve. A directory op to a wedged owner already fails after `callTimeoutMs` with no state change; making that sharper buys nothing and risks losing durable state.

Two further rules on fail-fast:

- It applies only to calls **not yet written to the wire**. Never bulk-`rejectFor` a connected-but-hung peer's in-flight calls on suspicion: the peer may still be *executing* the turn, and rejecting the caller while the callee runs is an at-most-once hazard for a non-idempotent call. Connection *loss* already rejects those, which is correct because the reply channel is provably gone.
- `oneWay` sends reject rather than silently drop, so reminder/stream/broadcast delivery sees the failure.

**`"siloUnavailable"` must not be added to `isStaleRejection`** in `distributed-dispatcher.ts` (currently `noActivation | unknownTarget | staleView`). If it were, `invoke`'s cached-address path would invalidate the location cache and fall through to `directory.lookup` → `placeAndInvoke` — local suspicion would quietly become re-placement. A test asserts that set by name. Cache invalidation on suspicion is useless anyway: it forces a lookup that returns the same address (the owner's partition still lists the peer while membership calls it active), routes there, and fails fast again — one extra round trip, plus it drags the flow onto the re-place path.

**Suppression, at exactly two call sites.** Adjudicating between "a built-in placement filter prepended by `filtersFor`" and "wrap the candidate seed": wrap the seed. It is one line, and it is the narrowest possible blast radius.

- `ClusterNode`'s `DistributedDispatcher` deps: `activeSilos: () => suspicion.suppress(activeSilos(this.options.membership.current()))`. `DistributedDispatcher` needs no change at all — this dep is consumed only by `placeAndInvoke`, and the metadata filters, version filter, placement hint and strategy all compose downstream of it unchanged. A filter in `filtersFor` would work too, but that chain's contract is *hard* metadata constraints that legitimately fail with `noCandidates`, and suspicion must never be able to produce that.
- `ClusterNode.gatherClusterLoad`. This is the highest-value suppression in the design and it fixes an existing hazard: `gatherClusterLoad` does `sendLoadQuery(silo).catch(() => 0)`, so a wedged peer reports **zero activations** and the rebalancer's `planCycle` sees the emptiest silo in the cluster and moves *existing* activations onto it. A suspect peer is excluded from both `counts` and `silos`, so no move can target it. The activation repartitioner's exchange-partner list gets the same suppression.

**Report.** A `thresh.runtime.peer.suspicion` counter (attributes: peer, reason) and a "placements suppressed" counter, following `messaging-metrics.ts`'s lazy-instrument pattern, plus `peerSuspected`/`peerRecovered` on the diagnostics surface (item #7) and a `suspectPeers()` accessor for tests and `IManagementGrain`.

### How the single-activation guarantee survives — the crux

The guarantee rests on one fact: **the only authority that may replace a directory entry pointing at a live-in-membership silo is that silo leaving the membership view.** `LocalDirectoryPartition.register` overwrites an existing entry in exactly two cases, and there is deliberately no "force" register:

1. `!isSiloLive(existing.silo)` — Orleans `RegisterCore`'s dead-silo overwrite. `ClusterNode` wires that predicate to `activeSilos(membership.current())`.
2. The caller passes the exact `previous` address it means to replace (`grainAddressEquals`) — the known-stale-after-a-move case, used by migration.

Case (2) is not available to a suspecting caller: passing the hung peer's address as `previous` would be a **forged CAS**, asserting "I know that activation is gone", which is exactly what a local timeout does not establish. Case (1) is membership-gated by construction — and note it is evaluated on the *range owner's* view, not the suspector's, which is why admitting suspicion as an input would make the CAS outcome depend on which caller arrived first.

So suspicion is barred from every input to that decision:

- **Never into `LocalDirectoryPartition`'s `isSiloLive` predicate.** Two independent disqualifications. It gates the dead-silo overwrite, so a local suspicion would hand out a second activation of a grain still live and serving. And that same partition answers *remote* lookups through `handleDirectoryRequest`, so a "local" suspicion injected there is published to every peer that asks — gossip in a different hat, failing `deviations.md` literally.
- **Never into `buildRing()`.** A locally-suspicious ring makes this silo compute a different `ownerOf` than its peers, so registers and lookups for one grain reach two owners and the single-winner CAS stops being one.
- **Never `directory.unregister` an entry pointing at a suspect peer.** This is the most dangerous available action and it is one line away: `claimAndActivateLocally` already does this shape of repair for a stale *local* self-pointer. Applied to a remote entry on a merely-suspected peer it is a straight double activation.
- **Never route a directory op away from the ring owner.** `DistributedGrainDirectory.route` derives the owner from `this.ring()`, which every silo computes identically from the same view; that agreement is *why* the register CAS is authoritative.
- **Never redirect a call to an existing activation.** `routeTo` follows the directory. A caller's private opinion of the host does not move state.
- **Never suppress `otherActiveSilos()`, `beginRecovery`'s sources, or `pruneHandoffSnapshot`'s live set** — that silently drops handed-off entries destined for a silo that is still alive.
- **Never suppress `isClientOriginatedRequest`'s membership check** — a suppressed peer's requests would be misread as external-client traffic and gated as a gateway call.
- **Never suppress the durable-job manager's `activeRingKeys`, or any other ownership set derived from membership.** This is the one re-placement authority in the tree that is about *work* rather than grains, and it is not covered by any bullet above. `LocalDurableJobManager` is handed `activeRingKeys: activeSilos(membership.current()).map(s => s.ringKey)` at `silo-builder.ts:1545` and again on every view change at `:1561`, and `refreshOwnership` (`local-durable-job-manager.ts:159-180`) claims any shard whose recorded `owner` is *absent from that set* — "owned by a dead silo" is decided entirely by set membership, with no CAS against the current owner's liveness. The same set picks the cross-silo forwarding target at `silo-builder.ts:1551-1556`. An implementer applying this design's stated spirit ("suppress the suspect peer wherever we pick a target") there would have two silos running executors for the same shard and firing the same jobs — the same class of break as a forged directory CAS, and with no etag to catch it, since two executors firing the same job is two legitimate deliveries, not one conflicting write. The pulling-stream ownership hook alongside it is protected only *indirectly*, by the ban on suppressing `buildRing()` (its ranges come from the ring); say so explicitly rather than relying on the reader to notice.

This is also why suspicion must not be implemented as a `MembershipService` decorator that downgrades a member's status: every one of the sites above reads through `activeSilos()`, so a decorator hits them all at once. A decorator has an unrelated concrete break too — `SiloBuilder` derives the shutdown grace from `this.membership instanceof KubernetesMembership`, so wrapping the service silently collapses the drain grace to zero.

What *does* authorise re-placement already works today without (C): K8s drops the endpoint → `KubernetesMembership` emits a new snapshot → `LocalDirectoryPartition.lookup`'s live check turns the peer's entries into misses (Orleans `IsSiloDead`), `updateView`'s `drain` classifies them `"drop"`, and the next call re-places through `placeAndInvoke`. Every directory op carries the sender's `appliedVersion`, which is the fencing token that linearises a register against a concurrent view change — a stale caller gets `staleView` and re-resolves.

**The Orleans mechanism this port lacks.** In Orleans, re-placement after a declared-dead silo is safe not because of the directory but because the dead silo *commits suicide*: a silo that reads its own status as dead in the membership table terminates rather than continuing to serve. There is no analogue for a pod that is wedged but that K8s has not killed. It is tempting to close this by having `SelfProbeWorker` fence itself when the silo observes *itself absent* from the active view — reject that: `KubernetesMembership.onSlices` always includes the local silo in the snapshot by design (a transient empty watch must not convince a silo the cluster vanished), so "absent from my own view" is unreachable under the only membership provider that has this problem. Persistence gives no cross-incarnation fencing beyond etags (`InconsistentStateError`), which catches a duplicate only if both activations write.

**Stage 3: self-fencing, and why it must be suicide-shaped rather than readiness-shaped.** The reachable self-fencing is the *inverted* one — evict the observer, never the observed — and it is the third stage of this design rather than part of the first: when suspected peers reach `ceil((n-1)/2)` and `n >= 3`, this silo stops suppressing and fences *itself*. It catches what the self-probe structurally cannot: the self-probe never leaves the process, so a wedged *egress* path (the exact case stage 3 exists for) is invisible to it while the local dispatcher stays perfectly healthy. And it is the suppression cap read from the other end, so the two compose with no dead zone.

"Self-eviction is always permissible under K8s-as-authority" is *not* true of readiness alone, and an earlier draft of this note said it was. Failing readiness removes the endpoint; it does **not** stop the silo serving, and the two together are a double activation. The concrete break, entirely from code that exists today:

1. Silo `S` has a wedged egress path and a healthy dispatcher. It suspects `ceil((n-1)/2)` peers and flips its readiness signal false; the kubelet drops `S`'s endpoint.
2. Peer `P`'s `KubernetesMembership` snapshot loses `S`. The range owner `O` (which is not `S`) runs `updateView`, whose `partition.drain` classifies grain `G`'s entry for `S` as `"drop"` (`cluster-node.ts:1140-1141`, "host gone — grain reactivates"). The next call to `G` re-places it on `P`. This is correct and is the mechanism the design relies on.
3. Meanwhile `S` has not stopped. **Nothing in this port deactivates on readiness failure** — `SelfProbeWorker` only calls `health.update({ dispatcherResponsive: false })` (`self-probe.ts:103`), and the only caller of `catalog.deactivateAll` is `stop()` (`cluster-node.ts:1103-1108`). And as established two paragraphs above, `KubernetesMembership.onSlices` always includes the local silo, so `S` still reads itself as active. So `S` still holds `G`'s activation and still serves it: to any client using `S` as its gateway (`deliverToProxy`), to any locally-originated call, and — if `S` owns `G`'s range — through `S`'s own directory partition, whose `isSiloLive` predicate says `S` is live. Two live activations of `G`, both serving.

So stage 3 is defined as a **local, unforced replica of Orleans' suicide**, not as a readiness flip:

- The trigger runs the *existing* shutdown path, in its existing order: `health.update({ draining: true })` (`graceful-shutdown.ts:45`, which already fails `ready()`), then the host's stop, which is `catalog.deactivateAll` before `listener.close()`/`connections.closeAll()` (`cluster-node.ts:1103-1108`). Draining also makes the gateway answer `siloDraining` and refuses new local placements, so nothing new activates while the drain runs and no activation outlives the endpoint removal.
- `peerReachability` on `HealthSignals`/`ready()` stays, but only as the *reason* — the named signal an operator sees in the probe body and the thing the hysteresis gate reads — not as the mechanism. Fencing that only flips this signal is the bug above.
- **The flip is terminal for the incarnation.** Once the drain runs, "recover on the first success to any suspected peer" is not available: the activations are gone. Recovery therefore lives entirely *before* the decision (hysteresis, the TTL, the cap), and after it the process exits non-zero so the kubelet restarts the container and the Deployment brings back a pod with a fresh `podUid` — which is the clean-slate assumption `SiloAddress`-keyed suspicion state already depends on. That exit is the faithful part: Orleans' suicide is process termination, and a wedged-egress process is not something we can argue back to health in-place.
- The rejected alternative is bounding stage 3 to configurations that cannot resurface a stranded activation — no client gateway and no locally-originated calls. That is not a real configuration (every silo originates calls: timers, reminders, streams, durable jobs), so it is rejected rather than offered.

Guards, mirroring `SelfProbeWorker`: no trip while already draining (`health.isDraining()`); require `n >= 3` so a two-silo cluster cannot empty itself; hysteresis over at least one full `suspicionTtlMs` before deciding; and under a true split, the `SiloAddress.compare`-lowest silo in the reachable subset holds its readiness (that comparator is already documented as ordinal-and-locale-free, so every silo derives the same order from the same view). Stage 3 lands only once stage 1's metric shows this silo suspecting a majority of peers in a real cluster.

**Said plainly: stage 3 makes peer opinion an indirect input to K8s membership.** A peer's silence causes this silo's endpoint to disappear, and that endpoint removal is what authorises re-placement — so the chain from "I could not reach P" to "grains move" does close, transitively, at stage 3. That is a real widening of the blast radius and is why it is staged last and evidence-gated. It is still not gossip, and the difference is structural, not a matter of degree: the opinion never crosses the wire; it never names another silo as dead in any form a peer can read; it is never aggregated with any other silo's opinion, so there is no vote to win and no quorum to forge; and its only possible effect is to remove **the observer**. No silo's opinion can change any other silo's status, which is the specific property `deviations.md` is protecting — the gossip failure mode it rules out is a set of silos evicting a healthy peer, and with no vote and no outbound claim that is unreachable. A silo that is wrong about the cluster can only take itself down, which is the same trade the readiness probe already makes.

### Out of scope

No K8s API writes and no RBAC for endpoint mutation (that is (B)). No cross-silo exchange of suspicion in any form — no gossip, no vote, no suspicion piggybacked on a directory answer. No change to `SiloMember.status` or to any `MembershipSnapshot`. No recovery of grains stranded on a wedged peer. No suspicion input to the directory, the ring, the location cache, handoff or recovery. No fail-fast on system messages. No `"siloUnavailable"` on the wire — it is raised locally only, so `serializeError`/`interpretResponse` wire compatibility is untouched during a rolling upgrade.

### Risks and mitigations

- **Scope creep in review** — "we know the peer is dead, why not re-place?" The guardrail must be a test plus a named comment at `isSiloLive`, `buildRing`, and the durable-job manager's `activeRingKeys`, because the enabling code is already present at all three and reads as an obvious simplification. The third is the one a future implementer is most likely to miss: it is about work, not grains, and it lives in `@thresh/hosting` rather than anywhere near the directory.
- **`isStaleRejection` as a silent re-placement trigger.** Adding `"siloUnavailable"` to that set is an easy, plausible-looking future edit that turns fail-fast into cache invalidation plus `placeAndInvoke`, with no test failing unless one asserts the set explicitly.
- **Slow-but-alive peer starved.** Mitigated by counting only transport silence, never a reply of any kind, by keying off `callTimeoutMs` (30s default) rather than any application deadline, and by the epoch rule, which stops the late timeouts of pre-recovery calls from immediately re-tripping. Residual, stated accurately: a peer under sustained GC pressure that misses three same-epoch `callTimeoutMs` windows is suppressed for one full TTL and **cannot shorten it with application traffic** — fail-fast intercepts calls to its existing activations too (`routeTo` → `remote.send` → `sendRemote`), so recovery evidence comes only from system messages (directory ops as range owner, `pingSilo`, manifest, migration/recovery) or from the TTL expiring. The mitigation is therefore the TTL's size (30s), not "it recovers on the first success".
- **Asymmetric partition** (A→B broken, B→A fine). A stops placing on B; B keeps placing on A. Suspicion neither causes nor fixes the partition — A's registers into ranges B *owns* fail regardless — it only stops A adding to the pile. Stage 3 is what resolves it, by draining and restarting A, and only at `n >= 3`; the `n >= 3` guard is load-bearing, since at `n = 2` both silos would otherwise fence themselves and empty the cluster.
- **Rollout herd** — mitigated by `coldStartGraceMs`/`firstSeenAt`.
- **Placement decisions differ silo-to-silo.** Harmless for correctness (the CAS is untouched) but it breaks the property that every silo derives the same candidates from the same view, which makes "why did this land here" harder to reason about. Priced in; it is the reason for default-off.
- **Transactions.** `sendRemote` merges the callee's enlisted participants off the *response*, so a fail-fast that produces no response merges nothing. The transaction manager must treat `"siloUnavailable"` as abort-and-release, not as an unknown outcome. Needs a targeted test against the Phase 7 TM.
- **Test flakiness.** `TestCluster` runs many silos in-process and a slow CI box could trip suspicion and perturb placement assertions. Clock is the injected `TimeProvider`, never wall-clock, and the feature defaults off.

### Implementation sketch

1. `packages/runtime/src/peer-suspicion.ts` — `PeerSuspicion` with `observeSilence`/`observeUnreachable`/`observeAlive`/`isSuspect`/`suppress`/`suspectPeers`, `TimeProvider`-driven, plus its test.
2. `packages/messaging/src/correlation-table.ts` — optional `onTimeout(peer, epoch)` / `onComplete(peer)` hooks on `CorrelationTable`, fired from the existing timer callback and `complete`. `register` stamps each pending entry with the epoch its `PeerSuspicion` supplies at registration time and hands it back on timeout, so a stale-epoch silence is discarded rather than counted.
3. `packages/core/src/errors.ts` — add `"siloUnavailable"` to `RejectionKind`, documented as locally-raised-only.
4. `packages/runtime/src/cluster-node.ts` — construct `PeerSuspicion` when `options.peerSuspicion` is set; feed the connection-lost callback and the correlation hooks; fail fast at the top of `sendRemote`; wrap the dispatcher's `activeSilos` dep and `gatherClusterLoad` in `suppress`.
5. `packages/runtime/src/placement/repartitioning/activation-repartitioner.ts` — the repartitioner has **no partner list in `ClusterNode` to wrap**: it is constructed with the whole `MembershipService` (`cluster-node.ts:657-668`) and computes its own partners at `activation-repartitioner.ts:225` via `activeSilos(this.deps.membership.current())`. Since a `MembershipService` decorator is forbidden above, the edit is: add `candidateSilos: () => SiloAddress[]` to `ActivationRepartitionerDeps` (defaulting to `activeSilos(this.deps.membership.current())` so existing construction sites are unaffected), replace the `activeSilos` call at `:225` with it, and have `ClusterNode` pass `() => suspicion.suppress(activeSilos(options.membership.current()))`.
6. `packages/directory/src/local-directory-partition.ts`, `ClusterNode.buildRing`, and `packages/hosting/src/silo-builder.ts` — comment only, in the style of the existing Orleans `IsSiloDead` citation: why suspicion must never reach `isSiloLive`, the ring, or the durable-job manager's `activeRingKeys` (`:1545`/`:1551`/`:1561`).
7. `packages/observability/src/runtime-metrics.ts` — the suspicion and suppression counters.
8. `packages/hosting/src/silo-builder.ts` — a `peerSuspicion` config block alongside `selfProbe`, off by default.
9. Stage 3 only: `packages/hosting/src/health-check.ts` — the `peerReachability` signal and its `ready()` check (the diagnosis), plus a self-fence trigger in `@thresh/hosting` that runs the *existing* shutdown path — `health.update({ draining: true })` then the host stop that calls `catalog.deactivateAll` before closing transport — and then exits non-zero. No new drain machinery; stage 3 is a caller of the shutdown path, which is why it is credible as suicide.
10. `docs/deviations.md` — record that local suspicion is placement-only and is never a membership status, and that stage 3 makes peer opinion an indirect input to *this silo's own* readiness only.

**Tests to write first** (sociable, fake clock, fake transport): a peer answering with an error or any rejection kind never becomes suspect; `missedThreshold` transport timeouts spread past `minSuspicionMs` trip it, while N simultaneous timeouts inside `minSuspicionMs` do not; a short per-call `responseTimeout` expiring repeatedly never trips it; **a success followed by the late timeouts of calls registered before it does not trip suspicion** (the epoch rule — the counter is scoped to calls registered after the last `observeAlive`, and those stale timeouts also arrive already past `minSuspicionMs`, so the dwell guard alone would not catch them); `suppress` returns the input unchanged when suppression would empty it, and never exceeds the minority cap; `isStaleRejection` does not admit `"siloUnavailable"`; suspicion expires on TTL with no traffic; a suspect peer is absent from `gatherClusterLoad`'s counts so no rebalance move targets it; a suspect peer is still adopted-from *never* by the durable-job manager, i.e. `activeRingKeys` is unaffected by suspicion.

Two load-bearing ones, and neither is optional:

- **Stage 1 (no re-placement from local opinion):** peer hangs, suspicion trips, membership still reports it active → the grain's directory entry is unchanged, no second activation exists anywhere, and the caller sees `"siloUnavailable"`; then drop the peer from membership → re-placement happens, exactly once.
- **Stage 3 (no activation outlives the fence):** silo `S` suspects a majority and self-fences; a peer re-places grain `G` after `S` leaves the peers' view → assert that **no call can reach the old activation on `S`** — not via a client using `S` as its gateway, not via a locally-originated call on `S`, and not via `S`'s own directory partition when `S` owns `G`'s range. Written against the readiness-only design this test fails, which is the point of writing it first.

**Open questions.** What does the self-probe call (echo grain? activation-collector ping?), and what's the right cadence (Orleans defaults around 10s)? Should the readiness gate distinguish "warming" from "draining" so a probe failure during drain doesn't immediately rip the silo out? For (C): what `suspicionTtlMs` and `coldStartGraceMs` defaults survive contact with a real rollout — and does the false-positive rate justify adding the directory-guarded `ISiloProbe` confirmation probe after all?

---

## How to use these notes

Each item is a starting point. The right cadence is:

1. Pick one. Re-read the relevant Orleans source (`~/repos/orleans/src`) and the corresponding TS package.
2. Decide whether the recommendation here still holds. If not, write the dissent in this file and update the option list — the doc is meant to track *current best thinking*, not historical state.
3. Spike the smallest end-to-end slice that proves the design. Land it behind a feature flag if the public surface changes.
4. Promote to the parity items in `todo.md` once the slice is real code.

Items with the strongest dependencies on each other: **#1 (cancellation)** unblocks **#2 (back-pressure)**, **TM keepalive**, and stream redelivery backoff. **#4 (extensions)** unblocks several smaller items. Doing #1 first is almost always the right call.
