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

## Interleaving

A turn is the whole `async` method, including every continuation after an `await`. By default a
grain runs one turn at a time: another call waits until the running turn's promise settles. These
let a call run while another turn is suspended at an `await`:

| Mechanism | Admits an incoming call while a turn is running when |
|---|---|
| `reentrant: true` on the grain (`@reentrant()`) | always |
| `alwaysInterleave` on the incoming method | always, whatever is running |
| `readOnly` on the incoming method | the running blocking turn is also `readOnly` — a `readOnly` call still waits behind a write |
| `mayInterleave` predicate on the grain | the predicate accepts the incoming call or the running one |
| `RequestContext.allowCallChainReentrancy(fn)` in a running turn | the incoming call is on the call chain that turn opened inside `fn` |

What an interleaved call may observe is a contract, pinned by
`packages/runtime/src/interleaving.test.ts`:

- It runs only while the other turn is suspended at an `await`, never concurrently with it. The code
  between two `await`s of a turn runs to completion before any other turn of the activation starts
  or resumes, so everything the suspended turn wrote before its `await` happens-before the
  interleaved call's first read.
- It sees exactly that: the suspended turn's state as of its last `await`, including writes the turn
  has not finished. The runtime does not hide a write's intermediate state. A grain whose
  interleaving reads must not see one keeps the in-progress write out of the fields those reads use
  and publishes it in one synchronous step after its last `await`. `JournaledGrain`'s `state` is
  such a confirmed view: it moves in one step once an append is durable, never through part of a
  batch.
- Methods without an interleave option never interleave each other, so a sequence of them stays
  atomic across its `await`s.

### Call-chain reentrancy

Call-chain reentrancy is off by default, as in Orleans. A non-reentrant grain that calls back into
itself — directly, or through other grains (`A -> B -> A`) — waits on its own running turn and fails
with `GrainCallTimeoutError` when the call's response timeout elapses. To allow it, wrap the calls in
`RequestContext.allowCallChainReentrancy` (Orleans' `AllowCallChainReentrancy`):

```ts
import { RequestContext } from "@thresh/core/request-context";

const total = await RequestContext.allowCallChainReentrancy(() => b.sumVia(selfKey));
```

While the callback's promise is pending, this activation admits calls on that chain, so `B`'s call
back into `A` runs during `A`'s `await`. Only the grain that opened the section is re-enterable:
`A -> B -> A -> B` still waits on `B` unless `B` opened its own section. The chain's id rides every
call made inside the callback and onward from there.
`RequestContext.suppressCallChainReentrancy(fn)` runs `fn` with no chain id, so its calls start
afresh.

## Calls and failure

Grain references are location transparent and serializable. Calls may fail because application code
throws, a deadline expires, cancellation propagates, load is shed, or a node disappears. Design
commands to be idempotent where retry is possible. Never rely on a specific activation or silo.

Use `RequestContext` for small cross-cutting string headers, incoming/outgoing call filters for
logging or policy, and `GrainCancellationToken` for cancellation across a grain boundary. Class
grains and decorators remain supported for Orleans ports; prefer functional grains for new code.
