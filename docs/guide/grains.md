# Grains and calls

## Identity and keys

Intersect the contract with `GrainKey<string>`, `GrainKey<bigint>`, `GrainKey<Guid>`, or a supported
compound key. Define the wire contract once with `defineGrainInterface<T>(name, config?)`. The
returned definition is passed to `getGrain`; the returned proxy implements `T`.

## Functional lifecycle

`defineGrain(name, setup)` runs `setup` once per activation. What `setup` returns is the grain's
message surface and nothing else — the interface methods, plus an optional self incoming-call filter
under `INCOMING_CALL_FILTER`. Lifecycle is registered with hooks, like the state facets:
`useOnActivate(ctx, handler)` and `useOnDeactivate(ctx, handler)`. Both compose — call them as often
as you like, from `setup` or from helpers it calls. Activate hooks run in registration order;
deactivate hooks unwind LIFO, so a hook always tears down before whatever it was set up on top of.
The setup context exposes the grain identity, runtime access, timers, reminders, streams, and other
activation services. Do not leak closure state outside the grain.

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
