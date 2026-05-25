# ADR 0009 — Message dispatch as the substrate (typed interfaces are a compile-time view)

- Status: Accepted — implemented. Supersedes the `methodId` portion of
  [ADR 0001](0001-runtime-proxy-grain-references.md)'s wire mapping; the runtime-`Proxy` decision
  itself stands. Builds on [ADR 0007](0007-functional-grains.md) and
  [ADR 0008](0008-message-dispatch-reducer-grains.md).
- Context docs: [02 — The actor model](../02-actor-model.md),
  [04 — Messaging](../04-messaging-and-serialization.md),
  [ADR 0001](0001-runtime-proxy-grain-references.md)

## Context

[ADR 0001](0001-runtime-proxy-grain-references.md) built grain references as runtime `Proxy` objects
but kept a per-interface **ordered method table** (`defineGrainInterface(name, { methods: [...] })`)
whose index is the wire `methodId`. That table is hand-maintained boilerplate that duplicates the
TypeScript interface — exactly what an Orleans-style code generator emits — and method *order*
silently defines the wire protocol, so a careless reorder renumbers it.

[ADR 0008](0008-message-dispatch-reducer-grains.md) made single-dispatch reducer grains a thing and
forced the question: which is the primitive — typed RPC method interfaces, or message dispatch? The
runtime was *already* message dispatch underneath — an `InvocationRequest` is
`{ interfaceId, methodId, args }`, and an activation processes it as a turn — so the typed
multi-method interface was the higher abstraction all along. The method-id table was the only thing
making it look otherwise.

## Decision

Make **method-name dispatch** the substrate, and treat typed interfaces as a compile-time view over
it.

- **The wire carries `method: string`.** The receiving activation invokes `instance[method](...args)`
  directly. The numeric `methodId` and the ordered method table are removed.
- **A grain interface is a compile-time view.** `defineGrainInterface(name, { options? })` carries the
  TypeScript shape `T` plus the *sparse* per-method invocation options — no method list. The runtime
  `Proxy` dispatches by the accessed property name: `grain.deposit(5)` becomes
  `{ method: "deposit", args: [5] }`.
- **`interfaceId` survives only as internal plumbing** — a hash of the interface name used to route a
  `getGrain` to its hosting grain type and to rehydrate references. It is not developer-facing and
  carries no method table.
- **Every grain is uniformly a message handler** — class, functional ([ADR 0007](0007-functional-grains.md)),
  or reducer ([ADR 0008](0008-message-dispatch-reducer-grains.md)). A reducer grain is the canonical
  single-message handler (`dispatch`/`query`); a multi-method grain is a handler with several named
  messages. This settles the layering: there is no RPC method-table layer *beneath* the reducer —
  both sit directly on the same dispatch substrate, and `defineGrain` is the general message-handler
  authoring primitive.

## Consequences

- **The last generated-looking artifact — the per-grain method table — is gone.** Adding, removing or
  reordering methods never touches a wire table; the `Action` union (reducer grains) or the method
  names (multi-method grains) are the protocol.
- **The wire carries a method name rather than a small int.** Marginally larger, but it removes the
  cross-silo dependency on both sides agreeing on method *order* and makes a captured message
  self-describing.
- **Grain references must never appear thenable.** The proxy returns `undefined` for `then`, so
  awaiting or `Promise.resolve`-ing a reference cannot dispatch a phantom `then` call; `then` is a
  reserved method name.
- **Unknown-method errors surface on the receiver** (`grain X has no method Y`) instead of at
  proxy-construction time. TypeScript still prevents typos at the call site.
- **Per-method options are resolved by name** — from the interface token on the sending side, and from
  the process-wide registry (`interfaceId` → options) on the receiving side. A *rehydrated* reference
  (passed as an argument and reconstructed) uses default options, which is a minor behaviour change
  for `readOnly` / `oneWay` flags on forwarded references.
- **Routing is unchanged.** `interfaceId` → grain-type resolution and the registration
  `interfaces: [...]` mapping are retained, so the directory, placement and transport layers are
  untouched.

## Alternatives considered

1. **Keep the method-id table (ADR 0001 status quo).** Smaller wire, but the boilerplate and the
   method-order-is-protocol hazard remain, and it does not deliver "typed interfaces are a compile-time
   view."
2. **Generate the table from the interface (a TS transformer).** Removes the hand-maintenance but adds
   a build step and tooling; name dispatch removes it with neither.
3. **Collapse `interfaceId` entirely** — the token carries the grain *type* directly, dropping the
   registry, `resolveGrainType`, and the registration `interfaces: [...]` mapping. A cleaner end state,
   but a much larger change to routing, the directory and reference rehydration for no additional
   developer-facing benefit. Deferred.
