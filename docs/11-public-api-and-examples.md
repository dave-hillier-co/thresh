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

### Defining an interface's method table (02)

```ts
const ICounter = defineGrainInterface<ICounter>("ICounter", {
  methods: ["increment", "decrement", "get"],
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
- Builder providers shipped so far are **in-memory**: `useMemoryStorage()` / `addStorage(name, p)`,
  `useReminders(table?)`, and (with the stream grain-wiring) `useMemoryStreams()`. The
  `addRedisStorage` / `useRedisReminders` / `addRedisStreams` methods are future work behind the same
  builder shape.
- Persistent state is declared with `@persistentState(name, { defaultValue })` and injected before
  `onActivate`; the `getStorage` accessor on `GrainRuntime` is not implemented (the decorator is the
  supported path). `registerTimer`, `registerReminder` / `unregisterReminder` are wired;
  `getStreamProvider` lands with the stream grain-wiring.
- Transport is `useInProcessTransport(network)` or `useWebSocketTransport()`; membership is
  `useStaticMembership([...])` or `useKubernetesMembership(watch)`.

### External client

```ts
const client = await createClient({
  clusterId: process.env.CLUSTER_ID!,
  gateway: { url: process.env.GATEWAY_URL! },   // the gateway Service (10)
}).connect();

const thermostat = client.getGrain<IThermostat>(IThermostat, deviceId);
await thermostat.onUpdate(update);
```

A client uses the same `getGrain` and the same proxy mechanism as a grain; it simply routes through
a gateway silo rather than placing calls locally.

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

const IThermostat = defineGrainInterface<IThermostat>("IThermostat", {
  methods: ["onUpdate"],
});
const IThermostatControl = defineGrainInterface<IThermostatControl>("IThermostatControl", {
  methods: ["getStatus", "updateConfiguration"],
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
