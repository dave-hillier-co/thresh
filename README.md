# ts-virtual-actors

A TypeScript implementation of the **virtual actor model** popularised by
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

```ts
// Interface — the contract callers see.
interface IThermostat extends GrainWithStringKey {
  onUpdate(status: ThermostatStatus): Promise<Command[]>;
}

// Implementation — runs inside the cluster.
@grain()
class ThermostatGrain extends Grain implements IThermostat {
  @persistentState("status")
  private status!: PersistentState<ThermostatStatus>;

  async onUpdate(status: ThermostatStatus): Promise<Command[]> {
    this.status.value = status;
    await this.status.write();
    return [];
  }
}

// Caller — from a web frontend or another grain.
const thermostat = client.getGrain<IThermostat>(IThermostat, deviceId);
const commands = await thermostat.onUpdate(update);
```

`client.getGrain` returns a lightweight proxy; the grain is activated on first call, wherever the
cluster decides to place it. The caller does not know — or need to know — which pod that is.

## Documentation

Read these in order for a full picture of the design and its build order.

- [01 — Overview and goals](docs/01-overview-and-goals.md)
- [02 — The actor model](docs/02-actor-model.md)
- [03 — Runtime and silo](docs/03-runtime-and-silo.md)
- [04 — Messaging and serialization](docs/04-messaging-and-serialization.md)
- [05 — Clustering and membership on Kubernetes](docs/05-clustering-membership-k8s.md)
- [06 — Grain directory and placement](docs/06-grain-directory-and-placement.md)
- [07 — Persistence](docs/07-persistence.md)
- [08 — Timers and reminders](docs/08-timers-and-reminders.md)
- [09 — Event streams](docs/09-event-streams.md)
- [10 — Kubernetes hosting](docs/10-kubernetes-hosting.md)
- [11 — Public API and examples](docs/11-public-api-and-examples.md)
- [12 — Project structure and tooling](docs/12-project-structure-and-tooling.md)
- [13 — Roadmap and phases](docs/13-roadmap-and-phases.md)
- [Architecture decision records](docs/adr/)

## Developing

A [pnpm](https://pnpm.io) workspace of `@tsva/*` packages (see
[project structure](docs/12-project-structure-and-tooling.md)). Requires Node 22+ and pnpm.

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
pnpm --filter @tsva/example-greeter start     # core actor model: activation, turns, idle reset
pnpm --filter @tsva/example-chat start         # stream fan-out to many members + durable resume
pnpm --filter @tsva/example-cluster start      # 3 silos over WebSocket: cross-silo routing + failover
pnpm --filter @tsva/example-thermostat start   # durable state + a reminder + a telemetry stream
```

- [`examples/greeter`](examples/greeter) — the smallest grain: `onActivate` runs before the first
  call, concurrent calls are serialized turns, and volatile state resets when the grain reactivates
  after going idle.
- [`examples/chat`](examples/chat) — a room fans each message out to every member; a member that
  deactivated while idle resumes *its own* durable subscription and recovers exactly what it missed.
- [`examples/cluster`](examples/cluster) — three silos over the real WebSocket transport. Calls from
  any silo route to one shared activation (directory CAS); when the hosting silo dies the grain
  reactivates on a survivor.
- [`examples/thermostat`](examples/thermostat) — the Orleans README example: `@persistentState`, a
  durable self-check reminder, and a telemetry stream consumed by an aggregator.

Work proceeds test-first in vertical slices that map to the
[roadmap](docs/13-roadmap-and-phases.md); [`todo.md`](todo.md) tracks outstanding items.
