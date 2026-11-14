# Testing

Use `TestCluster.start()` from `@thresh/testing/test-cluster` for integration tests. It creates
several in-process silos on one in-process network, sharing one set of memory backends (grain
state, journals, reminders, durable jobs, transactional state), so state outlives any one silo the
way a durable backend's would. Routing, placement, activation and the grain directory are the
production code paths.

The default harness is not production in three places, and each has an option that brings it
closer:

- **Membership.** By default every silo watches one shared static membership object: a view
  change reaches every silo in the same instant with the same version, and a stopped or killed
  silo leaves every view at once. Production silos each follow their own Kubernetes watch, so their
  views change one at a time and can differ. Pass `membership: "kubernetes"` to run that path.
- **Serialization.** A call between silos round-trips the value codec. In `TestCluster` so does a
  call to a grain on the same silo, so a test never sees shared objects or a preserved error class
  that a cross-silo call would lose. Pass `wireSemanticsForLocalCalls: false` to opt out. A silo
  outside the harness does not copy local calls unless the option is set.
- **Failure.** Nothing fails unless the test makes it fail: use `killSilo`, `cluster.faults` and the
  fault-injecting storage decorators below.

Use `FakeTimeProvider` (`time`) to drive collection, timeouts, timers, reminders and durable jobs
without sleeps. Unit-test pure reducers separately. Run fast suites with `pnpm test` and the Orleans
behavioural ports with `pnpm test:parity`.

## Stopping and killing silos

`stopSilo` is a graceful shutdown: the silo drains, runs its deactivation hooks and leaves
membership. `killSilo` is a crash, the stand-in for a killed pod (Orleans `KillSiloAsync`). The
silo's transport is gone, so peers dialling it are refused and nothing it sends arrives. Every timer
it armed is cancelled (grain timers, reminder ticks, durable-job polls, idle collection), its
pulling-stream agents stop, and it sees no further membership change, so it runs no further
ownership hooks. No shutdown code runs: no drain, no deactivation hook, no release of what it held.
One gap remains: JavaScript that is already running cannot be preempted, so a turn in progress at
the kill finishes its own code (its outgoing calls fail). `restartSilo` kills a silo and starts a
fresh one with a new identity.

## Kubernetes membership

```ts
const cluster = await TestCluster.start({ membership: "kubernetes", grains: [Counter] });
const [a, b, c] = cluster.silos;

cluster.kubernetes.hold(a); // a's watch lags
await cluster.kubernetes.markNotReady(c);
cluster.viewOf(a).appliedVersion < cluster.viewOf(b).appliedVersion; // two views at once
await cluster.kubernetes.release(a); // a catches up
```

Each silo runs `useKubernetesMembership` over its own watch of one fake EndpointSlice API server
(`cluster.kubernetes.api`). Silos join the way pods do: the pod is published not ready, the silo
starts, then the pod is marked ready. The controls are `setReady`, `markNotReady`, `removePod`,
`depart` (not ready, then removed), `hold` and `release` (one silo's watch lags, then catches up),
and `relist` (the API server ends that silo's watch, which then re-lists). Each control resolves
once every silo has applied the view it holds. `viewOf(silo)` returns that silo's membership view
and the view version its node has applied. `siloMetadata` travels as pod labels.

`stopSilo` marks the pod not ready, drains the silo, then removes the pod. `killSilo` leaves the
pod listed and ready, as after a real crash, until the test calls `kubernetes.depart` (or
`markNotReady` and `removePod`). Until then the survivors still route to the dead silo, and those
calls fail.

The static membership stays the default because the existing suites were written against its
instant, cluster-wide views.

## Network faults

Every silo's transport sits behind a `FaultInjectingTransport`
(`@thresh/messaging/fault-injecting-transport`). All of them share `cluster.faults`, a
`NetworkFaults`. Name silos by `SiloAddress`, pod name or endpoint:

```ts
cluster.faults.drop({ from: a.address, to: b.address }); // the caller times out
cluster.faults.delay({ to: "test-silo-2" }, 200); // on the cluster's clock
cluster.faults.failConnects({ from: a.address, to: b.address }); // RejectionError "unknownTarget"
const cut = cluster.faults.partition([a.address], [b.address, c.address]);
cut.heal();
cluster.faults.schedule({ from: a.address, to: b.address }, ["deliver", "drop", { delayMs: 50 }]);
```

Faults are applied where a message is sent, to requests and replies alike. Each fault holds until
healed, or for the given `count` of messages. A schedule decides the next messages on a link one at
a time, then the link goes back to the standing rules. Nothing is random. Connections are pooled, so
a refused connect only bites a link that has not connected yet; arm one before the cluster starts by
passing `faults` in the options. Links not named by any fault are untouched. A partition also cuts
grain-directory traffic, so an uncached call can fail even between two silos on the same side.
`useTransport` installs the decorator on a silo built by hand.

## Storage faults

`FaultInjectingGrainStorage` and `FaultInjectingJournalStorage`
(`@thresh/testing/fault-injecting-storage`) wrap any store. You can:

- `fail` chosen operations, raising `InjectedStorageFault` or an error you supply;
- `delay` them until a promise resolves, or for a time on a given clock;
- `lose` a grain-state write or clear (it reports success and stores nothing);
- `halt` the store, after which every operation hangs.

Narrow a fault with `when: ({ grainId, name }) => ...`, limit it with `count`, and remove it with
`heal()` or `healAll()`. Everything else passes through. The journal decorator carries the wrapped
store's `backingKey`. Install them from `configureSilo`, for example
`builder.useMemoryStorage(new FaultInjectingGrainStorage(cluster.storage))`.

## Remote semantics for local calls

`wireSemanticsForLocalCalls` (also a `SiloConfig` field) passes the arguments, the result and
a thrown error of every grain call through the codec round trip a message makes. A same-silo
callee then gets copies, not the caller's objects. A class instance with no registered surrogate
is flattened or refused, as on the wire. An error class the codec cannot rebuild arrives as
`UnavailableExceptionFallbackException`, as a remote caller would see it. The option is on by
default in `TestCluster` and off by default in a silo; a test that pins local-only behaviour, such as
a cyclic argument, passes `false`.
