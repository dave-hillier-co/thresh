# todo

Outstanding work only. See [`EPICS.md`](EPICS.md) for the status board and
[`docs/deviations.md`](docs/deviations.md) for how the design differs from Orleans. Test-first,
vertical slices (see [`CLAUDE.md`](CLAUDE.md)).

## Parity gaps

- [ ] **Cancellation tokens & per-call deadlines end-to-end** — thread a `CancellationToken`
      through the dispatcher, turn scheduler, persistence/storage providers and the deactivation
      path so hung methods, slow downstreams and shutdown can be interrupted. Prerequisite for the
      `onDeactivate` timeout, TM cancel and stream redelivery backoff.
- [ ] **Scheduler back-pressure, stuck-turn detection & `onDeactivate` timeout** — bounded
      per-activation queue with soft/hard limits (`MaxEnqueuedRequestsSoftLimit`/`HardLimit`),
      `MaxRequestProcessingTime` stuck-turn detection, and an enforced `DeactivationTimeout` that
      force-invalidates the activation so one bad grain cannot OOM or block silo shutdown.
- [ ] **Runtime instrumentation breadth** — wire the OTel meters already exposed by
      `@tsva/observability` into the catalog, directory, persistence, messaging, reminders, streams
      and durable-jobs runtimes, and add `exception.type` / `exception.message` /
      `exception.stacktrace` attributes to spans.
- [ ] **Versioned, registry-based serializer** — replace the 6-tag value codec with a versioned
      serializer that supports surrogate types, polymorphism resolution, `Map`/`Set`,
      circular-reference handling and per-field version tags so rolling upgrades and event-sourced
      payloads are safe.
- [ ] **Stream `IStreamFailureHandler` wiring & producer registration** — forward the configurable
      failure handler from `RedisPullingStreamProvider` to the agent, and persist poison events to
      a durable failure store (Orleans `AzureTableStorageStreamFailureHandler` equivalent). Add
      explicit producer registration.
- [ ] **Transaction TM confirmation-worker keepalive** — pair with the lock-acquisition deadline so
      the TM periodically pings remote participants to resolve in-doubt prepared records after a TM
      crash, rather than relying on one-shot recovery at next activation.
- [ ] **Grain observers / typed client callbacks** — `CreateObjectReference<T>()` surface for
      server-to-client push, including W3C `traceparent` propagation back to the client.
- [ ] **`IGrainExtension` mechanism** — minimal extension surface so management/system-target style
      APIs, cancellation tokens and per-grain push surfaces can be expressed.
- [ ] **`StatelessWorker` placement enforcement** — honor the option in the catalog/dispatcher so
      multiple activations per key can be created up to `maxLocalWorkers`, with placement-local
      delivery.
- [ ] **`@readOnly` runtime check** — at least a dev-mode mutation guard that detects state writes
      from a `@readOnly` call and surfaces the violation.
- [ ] **Directory handoff ACK & cleanup** — ACK-delete loop on snapshot handoff so retained
      snapshots are expired when a successor crashes pre-pull; add retry-with-backoff on recovery
      pulls and gate concurrent registers on a `recoveryMembershipVersion`.

## Parity follow-ups (from in-flight slices)

- [ ] **Reminders — surface `getReminder`/`getReminders` through `GrainRuntime`** so grains can
      introspect their own reminders (Orleans `IReminderRegistry.GetReminder` parity); currently
      exposed only at the registry level.
- [ ] **Reminders — plumb `ReminderOptions` (including `minimumPeriod`) through the silo builder**
      so hosts can configure the minimum without constructing `LocalReminderService` by hand.
- [ ] **Directory — apply silo-liveness gate to `register`** (Orleans `RegisterCore` treats an
      existing entry whose silo is dead as overwritable).
- [ ] **Client — per-attempt deadline** so the cumulative `callTimeoutMs` accounts for time spent
      in gateway-failover backoff.
- [ ] **Durable jobs — in-grain `RunId` dedup** on the delivery path
      (`cluster-node.deliverDurableJob`) mirroring Orleans
      `DurableJobReceiverExtension._runningJobs`, so concurrent re-deliveries of an in-flight RunId
      can be coalesced in the receiving grain in addition to the executor-side post-completion
      dedup.

## Beyond parity

- [ ] **Browser state replication & browser-hosted grains** — read-only live read-views first;
      implementation pending.

## Deferred

- [ ] Additional stream backings behind the existing interfaces (Redis is the default). Postgres
      grain-storage and reminder providers already ship.
