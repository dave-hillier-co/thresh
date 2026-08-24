# Grains and calls

## Identity and keys

Declare the key kind on the definition — `{ key: "integer" }` on `defineGrain` or
`defineGrainInterface`, one of `"string"` (the default), `"integer"`, `"guid"`, `"guid-compound"`,
or `"integer-compound"`. A separately declared contract can instead intersect `GrainKey<string>`,
`GrainKey<bigint>`, `GrainKey<Guid>`, or a compound marker; both paths resolve to the same key type,
so existing marked interfaces keep working unchanged. The definition is passed to `getGrain`; the
returned proxy implements the contract.

## Functional lifecycle

`defineGrain(name, setup)` runs `setup` once per activation. What `setup` returns is the grain's
message surface and nothing else — the interface methods, plus an optional self incoming-call filter
under `INCOMING_CALL_FILTER`. Lifecycle is registered with hooks, like the state facets:
`useOnActivate(handler)` and `useOnDeactivate(handler)`. Both compose — call them as often as you
like, from `setup` or from helpers it calls. Activate hooks run in registration order; deactivate
hooks unwind LIFO, so a hook always tears down before whatever it was set up on top of. Do not leak
closure state outside the grain.

The definition it returns **is** the grain's interface: the message surface is inferred from what the
factory returns, so `registerGrain(definition)` and `getGrain(definition, key)` both take it and
there is no separate interface to declare. This couples callers to the implementation module, which
is the right trade-off in-process and the wrong one across a process boundary — declare those
contracts separately with `defineGrainInterface` and register with
`registerGrain(ctor, { interfaces })`, which also stays the form for one grain under several
interfaces or per-silo interface versions.

Interface ids are derived from the interface *name*, so renaming one is a wire break. Any grain that
has ever been deployed must pin `{ interfaceName: "..." }`.

Each normal method call is one serialized turn. `readOnly` interface options declare calls that do
not mutate state and enable runtime optimizations; violating read-only state guards throws. Other
method options include response timeout, transaction behavior, interleaving, and one-way delivery.

## Calls and failure

Grain references are location transparent and serializable. Calls may fail because application code
throws, a deadline expires, cancellation propagates, load is shed, or a node disappears. Design
commands to be idempotent where retry is possible. Never rely on a specific activation or silo.

Use `RequestContext` for small cross-cutting string headers, incoming/outgoing call filters for
logging or policy, and `GrainCancellationToken` for cancellation across a grain boundary. Class
grains and decorators remain supported for Orleans ports; prefer functional grains for new code.
