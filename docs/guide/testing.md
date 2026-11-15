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
  that a cross-silo call would lose (`localCallSemantics: "wire"`). Pass
  `localCallSemantics: "reference"` to opt out. A silo outside the harness does not copy local
  calls unless it is set to `"copy"` or `"wire"`.
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
(`cluster.kubernetes.api`). Silos join the way pods do: the pod is listed not ready before the
silo starts (so the silo finds itself listed and start does not wait), the silo starts, and the
kubelet then marks the pod ready once the silo's own readiness passes. `siloMetadata` travels as
pod labels. Each control resolves once every live silo has applied the view its own membership
holds; `viewOf(silo)` returns that silo's membership view and the view version its node applied.

The controls on `cluster.kubernetes`:

- `setReady(silo, ready)` flips a pod's readiness condition. `markNotReady`, `removePod` and
  `depart` (not ready, then removed) take a pod away in the two steps Kubernetes does.
- `kubelet(silo)` is the readiness probe: it waits until the silo's own readiness passes (it
  throws after `timeoutMs`, default 10s), then marks the pod ready. `startAdditionalSilo` and
  `startAdditionalSilos` call it for you.
- `terminate(silo)` is a pod being deleted: its endpoint turns not ready and terminating, so every
  silo sees it `draining`. It stays listed until `removePod`.
- `hold(silo)` queues that silo's watch events so its view falls behind, and `release(silo)`
  delivers them in order and resumes live delivery. `relist(silo)` ends the watch as the API
  server does on a timeout; the silo re-lists and watches again.
- `settle()` lets watch reconnects, view reconciliation and directory traffic run.

`startAdditionalSilo({ holdReadiness: true })` starts a silo and leaves its pod not ready: it
stays a joining member, outside the ring while any peer is active, until the test flips it with
`setReady` or `kubelet`. `startAdditionalSilos(count, options)` starts several silos at once, as a
Deployment rolls out pods together: every pod is listed not ready before any silo starts, the
silos start concurrently, and the kubelet marks each ready as its own readiness passes. On a
cluster with no ready silo that is a cold start, and the ring is the joining silos until the first
is ready.

`stopSilo` marks the pod not ready, drains the silo, then removes the pod. `killSilo` leaves the
pod listed and ready, as after a real crash, until the test calls `kubernetes.depart` (or
`markNotReady` and `removePod`). Until its pod turns not ready the dead silo is still in the ring
and owns its share of the directory, so calls that need it are refused, as rejections the caller
may resend.

`restartInPlace(silo, options)` is a container that restarts in place: the silo's process dies as
in `killSilo` (a silo already killed, whose pod is still listed, may be restarted too), its pod
turns not ready, and a new silo process starts with the same `SiloAddress` — pod name, uid and
endpoint — while the pod stays listed throughout. The new process is a joining member until the
kubelet marks the pod ready once its own readiness passes, or, with `{ holdReadiness: true }`, until
the test flips it. It returns the new silo's handle; the old handle is no longer live. Survivors
may still hold directory entries and cached routes that name the address for activations of the
process before it.

```ts
const restarted = await cluster.restartInPlace(c, { holdReadiness: true });
restarted.address.equals(c.address); // true: the same pod
await cluster.kubernetes.kubelet(restarted); // readiness passes, it rejoins the ring
```

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
await cluster.faults.sever({ from: a.address, to: b.address }); // calls pending on it: "siloUnavailable"
```

Faults are applied where a message is sent, to requests and replies alike; a reply travels back down
the connection its request came in on, so a fault from `b` to `a` reaches `b`'s replies to `a`'s
calls. `sever` closes the connections already open on a link at the accepting end, as a reset socket
would: the dialler sees each one lost and fails exactly the calls pending on it. Each fault holds until
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

`localCallSemantics` (also a `SiloConfig` field) says what a same-silo call does to its arguments,
its result and a thrown error. `TestCluster` defaults to `"wire"`: each passes through the
serializer's round trip a message makes. A same-silo callee then gets copies, not the caller's
objects. A class instance with no registered surrogate is flattened or refused, as on the wire. An
error class the codec cannot rebuild arrives as `UnavailableExceptionFallbackException`, as a
remote caller would see it. `"copy"` gives the same result through the deep copier, faster, and
shares values marked `immutable` (see [Grains and calls](grains.md)); pass it to test grain code
that relies on that mark. A silo outside the harness defaults to `"reference"`, which copies
nothing; a test that pins local-only behaviour, such as a cyclic argument, passes `"reference"`.
