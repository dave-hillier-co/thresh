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
Per-activation state lives in closure variables; durable state and other facets come from hooks on
the setup context; the returned object is the grain's message surface.

```ts
// Interface — the contract callers see. A compile-time view; no method table.
interface IThermostat extends GrainWithStringKey {
  onUpdate(status: ThermostatStatus): Promise<Command[]>;
}
const IThermostat = defineGrainInterface<IThermostat>("IThermostat");

// Implementation — a factory closure that runs once per activation.
const ThermostatGrain = defineGrain<IThermostat>("Thermostat", (ctx) => {
  const status = usePersistentState<ThermostatStatus>(ctx, "status");

  const onUpdate = async (next: ThermostatStatus): Promise<Command[]> => {
    status.value = next;
    await status.write();
    return [];
  };

  return { onUpdate };
});

// Caller — from a web frontend or another grain.
const thermostat = client.getGrain<IThermostat>(IThermostat, deviceId);
const commands = await thermostat.onUpdate(update);
```

`client.getGrain` returns a lightweight proxy; the grain is activated on first call, wherever the
cluster decides to place it. The caller does not know — or need to know — which pod that is. The
class + decorator form this functional API is built on is still supported as an interop surface.

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

### Backing services

The Redis, Postgres and Kafka suites probe their service when the file loads and
skip themselves when it is unreachable, so `pnpm test` passes without any of
them — it just covers less. Bringing them up is what turns those suites on:

```sh
./scripts/ensure-services.sh          # start Redis, Postgres and Kafka in Docker
eval "$(./scripts/ensure-services.sh --env)"   # point the suites at them
pnpm test
./scripts/ensure-services.sh --down   # stop them again
```

With all three up, exactly one unit test file should skip; without them, twelve
do. They listen on **non-default ports** (6390, 5433, 9192) so they cannot
collide with — or silently borrow — another project's services on the same
machine. Borrowing is the dangerous case: a foreign Postgres on 5432 answers the
probe, fails to be usable, and the suite skips itself, so the run stays green
while testing nothing.

### CI

[`.github/workflows/ci.yml`](.github/workflows/ci.yml) runs typecheck and lint,
then the unit and parity suites with the backing services up, on a **self-hosted
runner**. It fails the build if more than one unit file skips, so a service that
stops starting shows up as a red build rather than as quietly reduced coverage.

To install the runner on the Mac mini (idempotent; needs `gh` logged in with
admin on the repo, and the mini reachable over SSH):

```sh
./scripts/setup-ci-runner.sh
```

Parity's skips are unrelated to CI: they are deliberate `orleansTest.excluded`
backlog entries, and that count is the parity metric itself.

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
