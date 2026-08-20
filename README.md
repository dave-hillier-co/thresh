# Thresh

Thresh is a TypeScript implementation of the **virtual actor model** popularised by
[Microsoft Orleans](https://github.com/dotnet/orleans), designed to run on **Kubernetes**.

The runtime delegates cluster concerns — membership, failure detection, discovery, scaling and
rolling updates — to Kubernetes primitives instead of reimplementing Orleans' gossip and probing
machinery. What remains is the part that makes the programming model pleasant: location-transparent,
strongly-typed, single-threaded-per-actor objects with managed lifecycles.

## What is a grain?

A **grain** is a virtual actor: a uniquely identified object with behaviour and optional state that
is *always addressable*. You never create or destroy a grain — you simply obtain a reference to one
by its identity and call methods on it. The runtime activates the grain on some pod on demand,
keeps its state in memory while it is busy, processes its messages **one turn at a time** so you
never write a lock, and deactivates it when it goes idle. If a pod dies, the grain transparently
reactivates elsewhere on its next call. This is "distributed objects that just work".

## Quick example

A grain is written as a **factory closure** — the React-inspired functional style is the default.
The factory runs once per activation: per-activation state lives in closure variables, durable state
and other facets come from **hooks**, and the object it returns is the grain's message surface. The
definition *is* the grain's interface — there is nothing else to declare.

```ts
import { defineGrain, usePersistentState } from "@thresh/core/define-grain";

interface Reading {
  tempC: number;
  targetC: number;
}
type Command = { kind: "heat" } | { kind: "cool" };

const Thermostat = defineGrain(
  "Thermostat",
  () => {
    const state = usePersistentState<Reading>("thermostat", {
      defaultValue: (): Reading => ({ tempC: 20, targetC: 21 }),
    });

    return {
      onUpdate: async (tempC: number): Promise<Command[]> => {
        state.value.tempC = tempC;
        await state.write();
        if (tempC < state.value.targetC) return [{ kind: "heat" }];
        if (tempC > state.value.targetC) return [{ kind: "cool" }];
        return [];
      },

      getStatus: async (): Promise<Reading> => ({ ...state.value }),
    };
  },
  { interfaceOptions: { getStatus: { readOnly: true } } },
);

// Host it, then call it — the definition is both the registration and the contract.
const silo = createSilo({ clusterId: "demo", local })
  .useStaticMembership([local])
  .useInProcessTransport(new InProcessNetwork())
  .useMemoryStorage()
  .registerGrain(Thermostat)
  .build();

await silo.start();
const commands = await silo.getGrain(Thermostat, "kitchen").onUpdate(18); // [{ kind: "heat" }]
```

`getGrain` returns a lightweight proxy; the grain is activated on first call, wherever the cluster
decides to place it. The caller does not know — or need to know — which pod that is. Callers see
exactly the surface the factory returned, promise-lifted; `onActivate`/`onDeactivate` and symbol-keyed
system hooks are not part of it.

**Hooks follow the rules of hooks.** `usePersistentState`, `useReducerState`, `useTransactionalState`,
`useDurable*` and `useDurableJobHandler` resolve the activation being set up from an ambient slot that
is live only during the *synchronous* body of the factory. Call them at the top level of the factory,
never after an `await`, from a method body, or from `onActivate` — those throw. To reach the runtime
*after* setup, capture the factory's `ctx` parameter in the closure and use `ctx.runtime` / `ctx.id` /
`ctx.getGrain` inside method bodies; that is not a hook call and stays valid for the activation's life.

**Fused definitions couple callers to the implementation.** `getGrain(Thermostat, key)` means the
caller imports the implementation module — and transitively its storage, stream and job dependencies.
That is the right trade for grains called from inside the same silo or package. For a contract that
crosses a package or process boundary — an external `@thresh/client` app, an interface with several
implementations, a grain with no implementation at all — declare it separately instead, and register
the two together:

```ts
// contract module — the only thing a remote caller imports
interface Ledger {
  post(amount: bigint): Promise<bigint>;
}
const Ledger = defineGrainInterface<Ledger, "integer">("example.Ledger");

// implementation module — never imported by callers
const LedgerGrain = defineGrain<Ledger, "integer">("Ledger", () => {
  /* … */
});

// hosting: createSilo(…).registerGrain(LedgerGrain, { interfaces: [Ledger] })
const balance = await client.getGrain(Ledger, 42n).post(5n);
```

An explicit `interfaces` list is exactly the set of interfaces the grain answers to; the definition's
own interface is *not* added to it. The key kind is stated once, as a type argument
(`"string"` — the default — `"integer"`, `"guid"`, `"integer-compound"`, `"guid-compound"`), and
determines the type of the `key` argument to `getGrain`.

Every `defineGrain` also publishes an interface under its grain-type name, so `LedgerGrain` above puts
a `"Ledger"` entry in the process-wide interface registry alongside `"example.Ledger"`. Defining a
name more than once **merges** into the existing entry rather than replacing it — per-method options
union, and `extension` and `key` are inherited when omitted — which is what lets a hand-written
interface module and a same-named fused definition coexist.

> **Interface names are wire identity.** An interface id is `stableHash32(name)`, so renaming an
> interface — including letting a grain's fused interface take the grain-type name where a separately
> declared name was in use — silently repoints every deployed caller. There is no compile-time signal.
> Any grain that has been deployed must pin its old name with
> `defineGrain(name, factory, { interfaceName: "example.IThermostat" })`.

The class + decorator form this functional API is built on is still supported as an interop surface.

## Documentation

The target is **feature parity with Orleans 10**, so the model, persistence, reminders, streams and
transactions are deliberately the same as Orleans — read the Orleans source for their mechanics. The
docs cover only what is worth writing down here:

- [How this differs from Orleans](docs/deviations.md) — the deviations only (TypeScript idioms,
  Kubernetes hosting, functional authoring), each linked to its decision record.
- [`EPICS.md`](EPICS.md) — the live status board (shipped vs. remaining).

## Developing

A [pnpm](https://pnpm.io) workspace of `@thresh/*` packages. Requires Node 22+ and pnpm.

```sh
pnpm install      # install workspace dependencies
pnpm test         # run the Vitest suites
pnpm typecheck    # type-check every package
pnpm lint         # ESLint + Prettier
```

### Examples

Each example runs end-to-end over in-memory providers and the in-process transport, and is also
exercised as a smoke test in the suite so it can't rot. Start with the greeter.

```sh
pnpm --filter @thresh/example-greeter start     # core actor model: activation, turns, idle reset
pnpm --filter @thresh/example-chat start         # stream fan-out to many members + durable resume
pnpm --filter @thresh/example-cluster start      # 3 silos over WebSocket: cross-silo routing + failover
pnpm --filter @thresh/example-bank start         # reducer grains: events fold to immutable state
pnpm --filter @thresh/example-thermostat start   # durable state + a reminder + a telemetry stream
```

- [`examples/greeter`](examples/greeter) — the smallest grain: `onActivate` runs before the first
  call, concurrent calls are serialized turns, and volatile state resets when the grain reactivates
  after going idle.
- [`examples/chat`](examples/chat) — a room fans each message out to every member; a member that
  deactivated while idle resumes *its own* durable subscription and recovers exactly what it missed.
- [`examples/cluster`](examples/cluster) — three silos over the real WebSocket transport. Calls from
  any silo route to one shared activation (directory CAS); when the hosting silo dies the grain
  reactivates on a survivor.
- [`examples/bank`](examples/bank) — reducer grains in the functional style. The account is shown two
  ways: a multi-method `defineGrain` closure with `useReducerState`, and the zero-boilerplate
  `defineReducerGrain` whose whole surface is `dispatch(action)` + `query()` with cross-grain work
  returned as Elm-style effects. Both fold a pure reducer into immutable state, persisted as a
  snapshot that survives a silo restart.
- [`examples/thermostat`](examples/thermostat) — the Orleans README example: `@persistentState`, a
  durable self-check reminder, and a telemetry stream consumed by an aggregator.

One example deploys to a real cluster rather than running in-process:

- [`examples/k8s-silo`](examples/k8s-silo) — a silo on Kubernetes: a `StatefulSet` with membership
  from the headless Service's EndpointSlices, durable state in an in-cluster Redis, and an
  HTTP-over-grain API. Its opt-in end-to-end test (`K8S_E2E=1`) deploys it and asserts the cluster
  forms, calls route to one activation across pods, a killed pod's grain reactivates on a survivor,
  and a rolling update keeps state.

Work proceeds test-first in vertical slices that map to the
[`EPICS.md`](EPICS.md) status board; [`todo.md`](todo.md) tracks outstanding items.
