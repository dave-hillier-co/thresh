# ADR 0014 — Grain-interface versioning (version-aware placement)

- Status: Accepted — implemented (interface version, manifest exchange, version-aware placement)
- Context docs: [02 — The actor model](../02-actor-model.md),
  [04 — Messaging and serialization](../04-messaging-and-serialization.md),
  [06 — Grain directory and placement](../06-grain-directory-and-placement.md),
  [10 — Kubernetes hosting](../10-kubernetes-hosting.md),
  [13 — Roadmap](../13-roadmap-and-phases.md)

> Orleans references: `Orleans.Serialization/Versioning/*` (interface versioning),
> `GrainVersionManifest` / `IClusterManifestProvider` (cluster manifest),
> `Orleans.Runtime/Versions/Compatibility/*` (`CompatibilityDirector`,
> `BackwardCompatible` / `StrictVersionCompatible`),
> `Orleans.Runtime/Versions/Selector/*` (`VersionSelectorStrategy`).

## Context

Until now the cluster assumed a **uniform image** ([10](../10-kubernetes-hosting.md)): a rolling
update is just a sequence of drain-and-rejoin events because no two silos could host incompatible
interface versions. Orleans-10 parity ([13](../13-roadmap-and-phases.md)) needs **heterogeneous
rolling upgrades** — v1 and v2 silos serving the same interface at once — with **version-aware
placement** so a new activation lands on a silo whose implemented version is compatible with the
caller's.

## Decision

- **Version on the interface.** `GrainInterface<T>` carries a `version` (default `1`), declared at
  `defineGrainInterface(name, { version })`. The interface **id stays name-derived**
  (`stableHash32(name)`), so versions of one interface share an id — the Orleans model. The caller's
  compiled version rides the wire as `InvocationRequest.interfaceVersion` → `Message.interfaceVersion`
  → back to `InvocationRequest`; absent ⇒ `1`.
- **Per-silo grain manifest.** Each silo derives a manifest (`interfaceId → implemented version`, with
  grain type) from its registered interfaces. Peers fetch a silo's manifest **lazily** over a new
  `system: "manifest"` RPC — the same transport/correlation path as the `system: "directory"` RPC —
  cache it, and drop the cache on any membership change. No gossip/push protocol.
- **Version-aware placement.** A pre-filter in `DistributedDispatcher.placeAndInvoke` prunes the
  candidate silos to those a `CompatibilityDirector` accepts, then narrows them with a
  `VersionSelectorStrategy`, before the existing `PlacementStrategy` runs. Directors:
  `backwardCompatible` (default — implemented ≥ requested) and `strict` (exact match). Selectors:
  `latest` (default), `all`, `minimum`. Configured on the host builder via
  `createSilo(...).useVersioning({ compatibility, selector })`.
- **Best-effort fallback.** When no active silo is compatible, placement falls back to the full
  candidate set rather than failing — placement never rejects on version grounds.
- **Inert by default.** The filter (and the manifest RPC) runs only when versioning is *active*: a
  registered interface declares a version > 1, or the host called `useVersioning`. A v1-only cluster
  with no policy runs exactly the pre-existing code path — no manifest messages, identical placement.

## Consequences

- A new activation for a versioned interface is steered to a compatible silo, enabling a heterogeneous
  rolling upgrade without a flag-day.
- Version selection is **orthogonal to placement**: every existing `PlacementStrategy` is untouched;
  the version filter is the same seam the roadmap's future "placement filters" item will reuse.
- The manifest is exchanged lazily and cached, so the steady-state hot path is unchanged once peers'
  manifests are known; membership changes re-fetch on demand.

## Scope boundary

- **Existing activations are not re-homed.** Version-aware selection applies at *placement* (when a
  grain has no activation). An activation already pinned to an older silo keeps serving there until it
  idles out; relocating a live activation onto a compatible silo is **grain migration**, a separate
  roadmap item. A v2-only method against such a grain surfaces the existing
  `GrainCallError "has no method …"` — consistent with the best-effort policy.
- **Reference rehydration stays version-agnostic** (it resolves by id to build a proxy; the proxy
  carries its own `version`).
- **Payload/field versioning is out of scope** — this is *interface*-version placement, not
  serializer schema evolution (that is the separate serializer-codec concern in
  [04](../04-messaging-and-serialization.md)).

## Alternatives considered

- **Version in the id hash.** Rejected — it would give each version a distinct id and break the shared
  routing/rehydration the Orleans model depends on.
- **Carry manifests on the membership snapshot.** Rejected — membership is Kubernetes-derived
  (EndpointSlices) and carries no per-silo metadata; a separate manifest exchange is the faithful
  analogue of Orleans' `IClusterManifestProvider`.
- **Push/gossip the manifest.** Deferred — lazy pull with caching is the minimal slice; a
  push-on-join refresh can replace it later without changing the placement contract.
- **Filter inside each `PlacementStrategy`.** Rejected — it would duplicate the logic across
  strategies and couple version selection to placement; a single pre-filter keeps both concerns
  separate.
- **Reject when no compatible silo exists.** Rejected per product decision in favour of best-effort
  placement (place anywhere) so a call never fails purely on version grounds.
