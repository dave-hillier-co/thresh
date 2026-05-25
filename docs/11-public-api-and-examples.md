# 11 — Public API and examples

This document collects the developer-facing surface in one place and shows complete, worked
examples. Every symbol used here is defined in the deep-dive documents:
[02 actor model](02-actor-model.md), [07 persistence](07-persistence.md),
[08 timers/reminders](08-timers-and-reminders.md), [09 streams](09-event-streams.md).

## API surface

### Declaring grains

```ts
// Base class and lifecycle (02).
abstract class Grain {
  protected readonly context: GrainContext;
  protected get id(): GrainId;
  protected get runtime(): GrainRuntime;
  protected getGrain<T>(def: GrainInterface<T>, key: GrainKey): T;
  onActivate(reason: ActivationReason): Promise<void>;
  onDeactivate(reason: DeactivationReason): Promise<void>;
}

// Decorators.
@grain(options?: GrainOptions)        // register an implementation
@reentrant()                          // class-level full reentrancy
@persistentState(name, opts?)         // inject a PersistentState<T> facet (07)
@reducerState(name, { initial, reduce })  // inject a ReducerState<S, E> facet (ADR 0006)
@serializable(opts?)                  // register a wire/state type (04)

interface GrainOptions {
  placement?: "random" | "preferLocal" | "activationCount";
  stateless?: boolean;                // stateless-worker grain (06)
  collectionAgeSeconds?: number;      // idle deactivation threshold
}
```

### Key kinds (02)

```ts
interface GrainWithStringKey  { /* key: string */ }
interface GrainWithIntegerKey { /* key: bigint */ }
interface GrainWithGuidKey    { /* key: Guid   */ }
```

### Defining an interface (02)

A compile-time view — the TypeScript type plus any non-default per-method options; no method table
(calls dispatch by name, see [ADR 0009](adr/0009-message-dispatch-substrate.md)):

```ts
const ICounter = defineGrainInterface<ICounter>("ICounter", {
  options: { get: { readOnly: true } },
});
```

### Runtime services available to a grain (03)

```ts
interface GrainRuntime {
  getGrain<T>(def: GrainInterface<T>, key: GrainKey): T;
  getStorage<TState>(name: string): PersistentState<TState>;
  registerTimer(cb: () => Promise<void>, due: Duration, period?: Duration): GrainTimer;
  registerReminder(name: string, due: Duration, period: Duration): Promise<void>;
  unregisterReminder(name: string): Promise<void>;
  getStreamProvider(name?: string): StreamProvider;
  deactivateOnIdle(): void;
  delayDeactivation(by: Duration): void;
}
```

### Hosting a silo

```ts
const silo = createSilo({ clusterId: process.env.CLUSTER_ID! })
  // membership
  .useKubernetesMembership()                       // watch endpoints (05); or .useStaticMembership([...]) for local
  // transport + serialization (04)
  .useWebSocketTransport({ port: 11111 })
  .useMessagePackSerialization()
  // durable backends — Redis by default (07, 08, 09)
  .addRedisStorage("default", { url: process.env.REDIS_URL! })
  .useRedisReminders({ url: process.env.REDIS_URL! })
  .addRedisStreams("default", { url: process.env.REDIS_URL!, partitions: 16 })
  // health endpoints for probes (10)
  .useHealthEndpoints({ port: 8080 })
  // register grain implementations
  .registerGrains([ThermostatGrain, AggregatorGrain]);

await silo.start();   // joins membership, begins accepting calls
```

### What is implemented today

The snippets above are the target surface. The shipped surface differs in a few concrete ways:

- `createSilo({ clusterId, local })` takes the silo's own `SiloAddress`; `build()` returns a
  `SiloHost` whose `start()` brings the silo online (flipping readiness) and `stop()` drains it.
- Grains are registered with the interfaces they serve —
  `registerGrain(ThermostatGrain, { interfaces: [IThermostat, IThermostatControl] })` — because
  TypeScript interfaces are erased at runtime and cannot be reflected.
- Both in-memory and Redis providers ship. In-memory (dev/tests): `useMemoryStorage()` /
  `addStorage(name, p)`, `useReminders(table?)`, `useMemoryStreams()`. Durable Redis:
  `addRedisStorage(name, { url, keyPrefix? })`, `useRedisReminders({ url, keyPrefix? })`, and
  `addRedisStreams(name, { url, keyPrefix? })` — each connects its client when the silo starts and
  disconnects when it stops (via host `onStart`/`onStop` hooks). The Postgres providers and stream
  partitioning (the `partitions` option above) are future work behind the same builder shape.
- Persistent state is declared with `@persistentState(name, { defaultValue })` and injected before
  `onActivate`; the `getStorage` accessor on `GrainRuntime` is not implemented (the decorator is the
  supported path). `registerTimer`, `registerReminder` / `unregisterReminder` and
  `getStreamProvider` (which delivers each `onNext` as a turn) are all wired.
- Transport is `useInProcessTransport(network)` or `useWebSocketTransport()`; membership is
  `useStaticMembership([...])`, `useKubernetesMembership(watch)`, or `useMembership(service)` to
  share one view across several in-process silos.
- `createSilo` also accepts `collectionAgeSeconds` / `collectionIntervalSeconds` (idle collection runs
  in the hosted path), `reminderRefreshSeconds` (how often a silo re-reads its reminder ranges), and
  `random` (deterministic placement for tests).
- Reducer grains ship in **snapshot mode**: `@reducerState(name, { initial, reduce })` injects a
  `ReducerState<S, E>` facet whose folded state is persisted via `GrainStorage`; see
  [ADR 0006](adr/0006-reducer-grains.md). The append-only event-log mode is future work.
- The external client (`@tsva/client`) is implemented for in-process and WebSocket transports
  (`createClient({ clusterId, local, transport, gateway })`), routing `getGrain` calls through a
  gateway silo; see "External client" below.

### External client

A client (`@tsva/client`) is not a silo — it hosts no grains — but it uses the same `getGrain` proxy
mechanism, forwarding every call to a **gateway** silo that routes it to the grain's activation and
replies. The client is itself reachable (it listens) so responses return over a connection to it.

```ts
const client = createClient({
  clusterId: process.env.CLUSTER_ID!,
  local: clientAddress,                         // the client's own reachable address
  transport: new WebSocketTransport(clusterId), // or an in-process transport for tests
  gateway: gatewaySiloAddress,                  // the gateway silo (10)
}).registerGrain(ThermostatGrain, { interfaces: [IThermostat] });

await client.connect();

const thermostat = client.getGrain<IThermostat>(IThermostat, deviceId);
await thermostat.onUpdate(update);
```

As with a silo, grains are registered with the interfaces they serve so the client can address the
same activation a silo-side caller would (TypeScript interfaces are erased — see "What is implemented
today" above). The shipped client takes the gateway's `SiloAddress` and a transport; a higher-level
gateway-discovery shape (`gateway: { url }`) is future work.

## Runnable examples

Most examples under [`examples/`](../examples) run end-to-end over in-memory providers and the
in-process transport, and double as acceptance tests in the suite. Start each with
`pnpm --filter <name> start`. One — `@tsva/example-k8s-silo` — deploys to a real Kubernetes cluster
instead (see [10](10-kubernetes-hosting.md)).

- **`@tsva/example-greeter`** — the smallest grain. Demonstrates the core actor guarantees with no
  providers: `onActivate` before the first call, serialized turns, and volatile state resetting when
  an idle grain is collected and reactivated.
- **`@tsva/example-chat`** — stream fan-out. A room publishes to one stream; many member grains each
  receive every message as a turn on their own activation. A member that deactivates while idle
  resumes its own durable subscription on reactivation, recovering exactly the messages it missed
  (see consumer-scoped subscriptions in [09](09-event-streams.md)).
- **`@tsva/example-cluster`** — three silos in one process over the **real WebSocket transport**,
  sharing one membership view. Records issued from any silo route to a single leaderboard activation
  (directory compare-and-set); killing the hosting silo and dropping it from the view reactivates the
  grain on a surviving silo on the next call (see [06](06-grain-directory-and-placement.md)). Uses
  `createSilo(...).useMembership(shared).useWebSocketTransport()`.
- **`@tsva/example-bank`** — reducer grains ([ADR 0006](adr/0006-reducer-grains.md)): account
  commands validate, then `raise` past-tense events that a pure reducer folds into immutable state.
  Snapshot mode persists the folded state via `GrainStorage`, surviving a silo restart; the events
  are transient.
- **`@tsva/example-thermostat`** — the full Orleans README example, below.
- **`@tsva/example-k8s-silo`** — a silo on **Kubernetes**: membership from the headless Service's
  EndpointSlices, WebSocket transport over per-pod IPs, durable state in an in-cluster Redis, health
  probes, and a small HTTP API over a counter grain. Its opt-in end-to-end test (`K8S_E2E=1`) builds
  the image, deploys the `StatefulSet`, and asserts the Phase-3 exit criteria — cluster formation,
  single-activation routing across pods, pod-kill reactivation on a survivor, and rolling-update
  state survival. See [10](10-kubernetes-hosting.md).

## Worked example: IoT thermostat (the Orleans README example, in TypeScript)

### Interfaces

```ts
interface IThermostat extends GrainWithStringKey {
  onUpdate(status: ThermostatStatus): Promise<Command[]>;
}

interface IThermostatControl extends GrainWithStringKey {
  getStatus(): Promise<ThermostatStatus>;
  updateConfiguration(config: ThermostatConfiguration): Promise<void>;
}

const IThermostat = defineGrainInterface<IThermostat>("IThermostat");
const IThermostatControl = defineGrainInterface<IThermostatControl>("IThermostatControl", {
  options: { getStatus: { readOnly: true } },
});
```

### Implementation (persistent state + a stream + a reminder)

```ts
@serializable()
class ThermostatState {
  status: ThermostatStatus = ThermostatStatus.unknown();
  pendingCommands: Command[] = [];
  config: ThermostatConfiguration = ThermostatConfiguration.default();
}

@grain()
class ThermostatGrain extends Grain
  implements IThermostat, IThermostatControl, Remindable {

  @persistentState("thermostat")
  private state!: PersistentState<ThermostatState>;

  async onActivate(): Promise<void> {
    // a durable daily self-check that fires even if idle/restarted (08)
    await this.runtime.registerReminder("self-check", { hours: 24 }, { hours: 24 });
  }

  // IThermostat — called by the device frontend
  async onUpdate(status: ThermostatStatus): Promise<Command[]> {
    this.state.value.status = status;
    const commands = this.state.value.pendingCommands;
    this.state.value.pendingCommands = [];
    await this.state.write();

    // publish telemetry for downstream aggregation (09)
    const stream = this.runtime.getStreamProvider().getStream<ThermostatStatus>("telemetry", this.id.key);
    await stream.publish(status);

    return commands;
  }

  // IThermostatControl — called by control systems
  async getStatus(): Promise<ThermostatStatus> {
    return this.state.value.status;   // read-only, served from memory
  }

  async updateConfiguration(config: ThermostatConfiguration): Promise<void> {
    this.state.value.config = config;
    this.state.value.pendingCommands.push(Command.configUpdate(config));
    await this.state.write();
  }

  // Remindable (08)
  async receiveReminder(name: string, _tick: TickStatus): Promise<void> {
    if (name === "self-check") this.state.value.pendingCommands.push(Command.selfTest());
  }
}
```

Two interfaces on one implementation class, exactly as in the Orleans README. The grain is
single-threaded (no locks around `state`), durably persisted via Redis, publishes to a stream, and
schedules durable work via a reminder.

### Consuming the telemetry stream

```ts
@grain()
class FleetAggregatorGrain extends Grain implements IFleetAggregator {
  async onActivate(): Promise<void> {
    const stream = this.runtime.getStreamProvider().getStream<ThermostatStatus>("telemetry", this.id.key);
    const existing = await stream.getSubscriptions();
    if (existing.length > 0) await existing[0].resume(this.handler());
    else await stream.subscribe(this.handler());
  }

  private handler(): StreamHandler<ThermostatStatus> {
    return { onNext: async (status) => { /* update rolling aggregates */ } };
  }
}
```

### Calling from a web frontend

```ts
// in an HTTP handler
app.post("/devices/:id/update", async (req, res) => {
  const thermostat = client.getGrain<IThermostat>(IThermostat, req.params.id);
  const commands = await thermostat.onUpdate(req.body);
  res.json(commands);
});
```

## Error handling at call sites

```ts
try {
  await thermostat.onUpdate(update);
} catch (e) {
  if (e instanceof RejectionError && e.kind === "siloDraining") {
    // transient: the runtime will re-resolve on retry
  } else if (e instanceof InconsistentStateError) {
    // optimistic-concurrency conflict (07): re-read and retry
  } else {
    // application error thrown by the grain method (04)
  }
}
```

The error taxonomy (`GrainCallError`, `RejectionError`, `InconsistentStateError`,
`GrainCallTimeoutError`) is defined in [04 — Messaging](04-messaging-and-serialization.md) and
[07 — Persistence](07-persistence.md).
