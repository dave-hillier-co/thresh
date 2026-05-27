# 11 — Public API and examples

The developer-facing surface in one place, with worked examples. Symbols are defined in the deep-dive
docs: [02 actor model](02-actor-model.md), [07 persistence](07-persistence.md),
[08 timers/reminders](08-timers-and-reminders.md), [09 streams](09-event-streams.md).

> Orleans references: `Orleans.Core.Abstractions/Core/Grain.cs`, `.../IGrainFactory.cs`,
> `.../IGrainRuntime.cs`, `Orleans.Sdk`, `Orleans.Server`.

## Declaring grains

Grains are authored functionally by default ([ADR 0009](adr/0009-functional-grains.md)): a factory
closure runs once per activation and returns the grain's methods plus optional lifecycle hooks. Facet
hooks read the `ctx`.

```ts
const CounterGrain = defineGrain<ICounter>(
  "Counter",
  (ctx) => {
    const state = usePersistentState<CounterState>(ctx, "counter", { defaultValue: () => ({ n: 0 }) });
    return {
      increment: async () => { state.value.n++; await state.write(); return state.value.n; },
      get: async () => state.value.n,
    };
  },
  { reentrant: false }, // GrainOptions: placement, stateless, collectionAgeSeconds, …
);
```

Hooks: `usePersistentState(ctx, name, { defaultValue })` (07), `useReducerState(ctx, name, { initial,
reduce })` (folds events into a persisted snapshot; [ADR 0006](adr/0006-reducer-grains.md)).
`defineReducerGrain(name, { initial, reduce })` is the zero-boilerplate single-dispatch form whose
whole surface is `dispatch(action)` + `query()`, with cross-grain work returned as effects
([ADR 0010](adr/0010-message-dispatch-reducer-grains.md)).

`defineGrain` is a shell over a `Grain` base class with field decorators (`@grain`, `@reentrant`,
`@persistentState`, `@reducerState`, `@serializable`, `@implicitStreamSubscription`,
`@implicitChannelSubscription`); that class form stays supported for interop and subclassing (02).

Key kinds are marker interfaces: `GrainWithStringKey` / `GrainWithIntegerKey` / `GrainWithGuidKey` (02).

## Defining an interface

A compile-time view — the TypeScript type plus any non-default per-method options. Calls dispatch by
name; there is no method table ([ADR 0011](adr/0011-message-dispatch-substrate.md)).

```ts
const ICounter = defineGrainInterface<ICounter>("ICounter", { options: { get: { readOnly: true } } });
```

References are obtained as `getGrain(ICounter, key)` rather than Orleans'
`GetGrain<ICounter>(key)`: TypeScript interfaces are erased at runtime, so the token carries the
identity and per-method options the runtime needs to route and rehydrate. The `<ICounter>` parameter
still gives full compile-time type-safety on the proxy — a TypeScript-idiom adaptation, not a loss of
typing. (For the same reason a grain is registered with the interfaces it serves:
`registerGrain(CounterGrain, { interfaces: [ICounter] })`.)

## Runtime services available to a grain

`GrainRuntime` (reached as `this.runtime` in a class grain, or `ctx.runtime` in a factory):
`getGrain`, `registerTimer`, `registerReminder` / `unregisterReminder`, `getStreamProvider`,
`getBroadcastChannelProvider`, `deactivateOnIdle`, `delayDeactivation`, `migrateOnIdle`. Persistent
state is acquired through the `usePersistentState` hook / `@persistentState` decorator (injected before
`onActivate`), not a runtime accessor.

## Hosting a silo

`createSilo({ clusterId, local })` returns a builder; `build()` yields a `SiloHost` whose `start()`
brings the silo online (flipping readiness) and `stop()` drains it.

```ts
const silo = createSilo({ clusterId: process.env.CLUSTER_ID!, local: siloAddress })
  .useKubernetesMembership(watch)              // or .useStaticMembership([...]) / .useMembership(shared)
  .useWebSocketTransport()                     // or .useInProcessTransport(network)
  .addRedisStorage("default", { url })         // or useMemoryStorage() / addPostgresStorage(name, { connectionString })
  .useRedisReminders({ url })                  // or useReminders() / usePostgresReminders({ connectionString })
  .addRedisStreams("default", { url })         // or useMemoryStreams()
  .useBroadcastChannels()                      // optional: in-cluster pub/sub
  .registerGrain(ThermostatGrain, { interfaces: [IThermostat, IThermostatControl] })
  .build();

await silo.start();
```

Durable backends are Redis by default; in-memory variants back dev/tests, and Postgres grain storage /
reminder table also ship (each connects and creates its backing table on `start()`, disconnects on
`stop()`). `createSilo` also accepts `collectionAgeSeconds` / `collectionIntervalSeconds`,
`reminderRefreshSeconds`, and `random` (deterministic placement for tests); `useTracing()` /
`useMetrics()` / `useLogging()` wire observability ([ADR 0013](adr/0013-observability.md)), and
`useVersioning(...)` enables version-aware placement ([ADR 0014](adr/0014-grain-interface-versioning.md)).

## External client

A client (`@tsva/client`) hosts no grains but uses the same `getGrain` proxy, forwarding each call to a
**gateway** silo that routes it and replies (the client listens so responses return to it). It
discovers gateways through a `GatewayListProvider` (static / membership / URL) and fails over when one
is unreachable.

```ts
const client = createClient({
  clusterId, local: clientAddress,
  transport: new WebSocketTransport(clusterId),
  gateways: membershipGatewayProvider(membership), // or staticGatewayProvider([...]) / urlGatewayProvider([...])
}).registerGrain(ThermostatGrain, { interfaces: [IThermostat] });
await client.connect();
await client.getGrain(IThermostat, deviceId).onUpdate(update);
```

## Runnable examples

Examples under [`examples/`](../examples) run end-to-end over in-memory providers and the in-process
transport, double as acceptance tests, and start with `pnpm --filter <name> start`. Grains are
authored functionally (`defineGrain`); `@tsva/example-thermostat` keeps one `@grain()` class on
purpose as the living interop example its functional aggregator consumes from.

- **greeter** — the smallest grain: `onActivate` before the first call, serialized turns, volatile
  state resetting on idle reactivation.
- **chat** — stream fan-out; a member that deactivates resumes its own durable subscription, recovering
  exactly the messages it missed ([09](09-event-streams.md)).
- **cluster** — three silos in one process over real WebSocket transport; records route to a single
  activation, and killing the host silo reactivates the grain on a survivor ([06](06-grain-directory-and-placement.md)).
- **bank** — reducer grains two ways: a `useReducerState` closure and a `defineReducerGrain` whose
  transfer credit leg is an Elm-style effect ([ADR 0010](adr/0010-message-dispatch-reducer-grains.md)).
- **broadcast** — broadcast-channel pub/sub fan-out to implicit subscribers ([ADR 0015](adr/0015-broadcast-channels.md)).
- **migration** — live activation move carrying even unflushed state to another silo.
- **thermostat** — the Orleans README example (below).
- **k8s-silo** — a silo on **Kubernetes** with an opt-in (`K8S_E2E=1`) end-to-end test asserting the
  Phase-3 criteria ([10](10-kubernetes-hosting.md)).

## Worked example: IoT thermostat

Two interfaces served by one grain, persistent state, a stream, and a durable reminder.

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

const ThermostatGrain = defineGrain<IThermostat & IThermostatControl>("Thermostat", (ctx) => {
  const state = usePersistentState<ThermostatState>(ctx, "thermostat", {
    defaultValue: () => new ThermostatState(),
  });
  return {
    onUpdate: async (status) => {
      state.value.status = status;
      const commands = state.value.pendingCommands;
      state.value.pendingCommands = [];
      await state.write();
      await ctx.runtime.getStreamProvider().getStream<ThermostatStatus>("telemetry", ctx.id.key).publish(status);
      return commands;
    },
    getStatus: async () => state.value.status, // read-only, served from memory
    updateConfiguration: async (config) => {
      state.value.config = config;
      state.value.pendingCommands.push(Command.configUpdate(config));
      await state.write();
    },
    onActivate: async () => {
      await ctx.runtime.registerReminder("self-check", { hours: 24 }, { hours: 24 });
    },
    receiveReminder: async (name) => {
      if (name === "self-check") state.value.pendingCommands.push(Command.selfTest());
    },
  };
});
```

The grain is single-threaded (no locks around `state`), durably persisted, publishes telemetry, and
schedules durable work via a reminder. A consumer subscribes to the `telemetry` stream in its
`onActivate` (resuming an existing subscription via `getSubscriptions`/`resume`, else `subscribe`).

## Error handling at call sites

```ts
try {
  await thermostat.onUpdate(update);
} catch (e) {
  if (e instanceof RejectionError && e.kind === "siloDraining") {
    // transient: the runtime re-resolves on retry
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
