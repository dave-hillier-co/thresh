# ADR 0018 — Durable jobs (sharded, durable, at-least-once scheduled execution)

- Status: Proposed — design only, no implementation. Slice plan with exit criteria below.
- Context docs: [07 — Persistence](../07-persistence.md),
  [06 — Grain directory and placement](../06-grain-directory-and-placement.md),
  [13 — Roadmap](../13-roadmap-and-phases.md);
  [ADR 0005](0005-redis-default-providers.md) (default durable backend),
  [ADR 0009](0009-functional-grains.md) (functional authoring),
  [ADR 0015](0015-broadcast-channels.md) (the symbol-keyed handler idiom),
  [ADR 0016](0016-activation-rebalancer.md) (elected/membership-driven background work).

> Orleans references (read to ground this ADR): `Orleans.DurableJobs/DurableJob.cs`,
> `IDurableJobHandler.cs` (`IJobRunContext`, `IDurableJobHandler.ExecuteJobAsync`),
> `DurableJobRunResult.cs` (`Completed` / `PollAfter` / `Failed`), `ScheduleJobRequest.cs`,
> `ILocalDurableJobManager.cs`, `IDurableJobReceiverExtension.cs` (the per-grain receiver, `RunId`
> task tracking, `[AlwaysInterleave]`), `JobShard.cs` (`IJobShard` + the abstract
> `PersistAddJobAsync` / `PersistRemoveJobAsync` / `PersistRetryJobAsync` hooks),
> `InMemoryJobQueue.cs` (due-time priority queue), `JobShardManager.cs`
> (`AssignJobShardsAsync`, dead-silo adoption, `AdoptedCount` / poison shards),
> `ShardExecutor.cs` (concurrency limiter, concurrency slow-start, overload backoff, the poll loop,
> retry), `LocalDurableJobManager.cs` (the `SystemTarget`, shard-key bucketing, the periodic check,
> shard-claim ramp-up), `Hosting/DurableJobsOptions.cs` + `DurableJobsExtensions.cs`. There is **no**
> `Orleans.DurableJobs.Abstractions` project — the whole feature lives in one assembly — and it has
> **no dependency on `Orleans.Journaling`**.

## Context

Reminders ([roadmap phase 5](../13-roadmap-and-phases.md), `@tsva/reminders`) already give the port
durable, single-fire-and-recurring callbacks: a grain registers a reminder, the durable
`ReminderTable` keeps it, and the silo that owns the grain's hash range on the ring re-reads the
table and fires it — surviving deactivation and the owning silo's death. That covers "wake grain
*G* every *p*". It does **not** scale to "schedule a few million one-shot callbacks for arbitrary
future times, run them with bounded per-silo concurrency, retry the failures, and don't melt a silo
that just rejoined after an outage." Reminder ownership is keyed by *grain hash*, so a burst of
jobs all targeting one grain (or all due at one instant) has nowhere to spread, and there is no
retry, concurrency, or back-pressure model around the fire.

Orleans 10 adds `Orleans.DurableJobs` for exactly this. **It is important to be precise about what it
is, because the name misleads:** despite "jobs" suggesting a Temporal / Durable Task Framework-style
*workflow* engine, the Orleans implementation is **not** an orchestration/deterministic-replay
engine. There are no activities, no sub-orchestrations, no replay of a workflow function, no
durable awaits inside a workflow body. The unit of work is a **single scheduled invocation of a
target grain at a due time**, made durable, sharded for scale, retried on failure, and rebalanced
across silos on membership change. It is best understood as **"reminders at scale, one-shot, with
retries, concurrency control, and crash-failover of the work itself."** This ADR ports that, and
the headline scope boundary (below) is to *not* grow it into a workflow engine.

### How Orleans' engine actually works (the model we port)

- **`DurableJob`** — `{ Id, Name, DueTime, TargetGrainId, ShardId, Metadata }`. A job names a target
  grain and a due time; the target grain is expected to implement `IDurableJobHandler` (one method,
  `ExecuteJobAsync(IJobRunContext, CancellationToken)`).
- **Shards are time buckets, owned by one silo.** A job's shard key is
  `floor(DueTime / ShardDuration)` (default `ShardDuration = 1h`); the shard owns every job whose
  due time falls in `[start, end)`. A shard is owned and processed by **exactly one silo at a time**
  — that is the single-writer guarantee, at *shard* granularity rather than *grain* granularity.
  Sharding by *time* (not by grain key) is what lets a burst all targeting one grain spread across
  silos and lets the system hold cheap, append-only buckets for the far future.
- **`IJobShard` is the durable seam.** Its abstract `PersistAddJobAsync` / `PersistRemoveJobAsync`
  / `PersistRetryJobAsync` hooks are the only contact with storage; an in-memory queue
  (`InMemoryJobQueue`, a due-time priority queue polled each second) is the runtime working set. The
  shipped `InMemoryJobShard` makes the persist hooks no-ops — explicitly dev/test only.
- **`LocalDurableJobManager`** (a per-silo `SystemTarget`, started at the `Active` lifecycle stage)
  is the orchestrator. `ScheduleJobAsync` buckets the request, gets-or-creates the writable shard,
  persists the add, and enqueues. A periodic check (every 10 min, or immediately on a membership
  change) reconciles owned shards from the store and **activates** any shard whose start time is
  within `ShardActivationBufferPeriod` (default 5 min ahead).
- **`ShardExecutor`** runs an activated shard: waits until start time, consumes due jobs, and for
  each one takes a slot from a **concurrency limiter** (`MaxConcurrentJobsPerSilo`, default
  `10_000 × procs`), with a **concurrency slow-start** (begin at `procCount`, double every 10 s up
  to the max — avoids cold-cache starvation right after start) and an **overload backoff** (pause
  consumption while `IOverloadDetector.IsOverloaded`). It calls the target via a per-grain
  **receiver extension**.
- **`DurableJobRunResult` drives the per-job outcome:** `Completed` → remove from the shard;
  `PollAfter(delay)` → an inline poll loop (the executor sleeps `delay` and re-asks, holding the
  concurrency slot — this is how a long-running handler is supervised without blocking the grain's
  turn); `Failed(exception)` → the retry policy.
- **Retries** are a pure policy `ShouldRetry(IJobRunContext, Exception) → DateTimeOffset?`: a new due
  time to re-enqueue at, or `null` to drop. Default: up to 5 attempts, exponential backoff `2^n` s.
  `IJobRunContext.DequeueCount` is the attempt counter.
- **Failover and self-protection.** `JobShardManager.AssignJobShardsAsync` claims shards this silo
  already owns, **orphaned** shards (an owner that drained set its owner to null), and shards from
  **dead** silos (via the cluster membership snapshot). A shard adopted from a *dead* owner
  increments an `AdoptedCount`; past `MaxAdoptedCount` (default 3) the shard is **poisoned** and
  never reassigned — so one job that crashes its host can't bounce around killing silos. A freshly
  started silo claims orphaned shards under a **ramp-up budget** (linear from `ShardClaimInitialBudget`
  2 to `ShardClaimMaxBudget` 20 over `ShardClaimRampUpDuration` 5 min, then unlimited; overload ⇒ 0)
  so disaster recovery doesn't immediately re-overwhelm the recovering node.

### Delivery semantics

**At-least-once.** A job is durable in the shard store and removed only after it completes; a silo
that crashes mid-job loses the in-memory slot, another silo adopts the shard, and the job re-runs.
**Handlers must be idempotent.** There is no exactly-once and no cross-job ordering guarantee beyond
"a shard's jobs come due roughly in due-time order." Single ownership per shard means no concurrent
double-processing in steady state (only a crash/adoption window can re-run).

## Decision

Port `Orleans.DurableJobs` faithfully as a new package, **`@tsva/durable-jobs`**, layered on the
existing runtime exactly the way `@tsva/reminders` is — and **not** as a workflow engine. Keep
Orleans' shard/ownership/retry/back-pressure model; express the authoring surface functional-first
([ADR 0009](0009-functional-grains.md)); reuse the port's clock, membership, ring-leadership,
dispatcher and Redis-default ([ADR 0005](0005-redis-default-providers.md)) seams rather than adding
parallel machinery.

### Package layout — `@tsva/durable-jobs`

Structured as the mirror of `@tsva/reminders` (per-silo service + durable table + memory/Redis
backings):

- **Types** — `DurableJob`, `ScheduleJobRequest`, `JobRunContext`, `DurableJobRunResult`
  (`completed` / `pollAfter(delay)` / `failed(error)`), `DurableJobsOptions`.
- `local-durable-job-manager.ts` — the per-silo manager (analogue of `local-reminder-service.ts`):
  bucket-and-schedule, the periodic shard check, membership-driven reconcile, claim-budget ramp-up.
  Driven by the **injectable clock** so it is deterministically testable, exactly like the reminder
  service.
- `job-shard.ts` — the `JobShard` base with the three abstract `persistAdd/Remove/Retry` hooks;
  `in-memory-job-queue.ts` — the due-time priority queue (second-granularity buckets, cancel/retry).
- `shard-executor.ts` — concurrency limiter, concurrency slow-start, overload backoff, the poll loop,
  the retry application.
- `job-shard-store.ts` — the durable shard+job table **contract** (the `ReminderTable` analogue),
  with `memory-job-shard-store.ts` (dev/test) and `redis-job-shard-store.ts` (the default,
  [ADR 0005](0005-redis-default-providers.md)).

### How it sits on the existing port

- **Grains (the target / receiver).** Orleans auto-registers an `IDurableJobReceiverExtension` on
  every grain so any grain implementing `IDurableJobHandler` is targetable without per-grain wiring,
  and the extension tracks the running task by `RunId` so a poll returns status without re-running or
  blocking the grain's turn (`[AlwaysInterleave]`). This port has no grain-extension mechanism but it
  has the **symbol-keyed handler idiom** the equivalent features already use —
  `STREAM_SUBSCRIPTION_OBSERVER`, `BROADCAST_CHANNEL_OBSERVER`, `INCOMING_CALL_FILTER`. So a target
  grain opts in by exposing a `DURABLE_JOB_HANDLER` handler; the executor dispatches the job to it
  over the existing dispatcher (the same path `receiveReminder` takes in `cluster-node.ts`). A small
  per-activation receiver shim holds the `RunId → running task` map and answers polls, reproducing
  the extension's behaviour on top of the dispatcher.
- **Reminders.** Durable jobs reuse the *shape* of `LocalReminderService` — per-silo, backed by a
  durable table, reconciled on membership change, clock-driven — and the same ring-leadership /
  membership-snapshot seams. They deliberately **do not** reuse reminders' *hash-range* ownership:
  the unit of ownership is the **time-bucketed shard**, claimed from the store with explicit
  owner/adopt/poison bookkeeping, because jobs are bucketed by due time, not by grain key. Reminders
  and durable jobs are complementary, not redundant: reminders for recurring per-grain wakeups,
  durable jobs for high-volume one-shot scheduled work with retries and back-pressure.
- **Persistence.** The shard store is its **own** durable table (Redis by default, like the
  `ReminderTable`), not `GrainStorage` — jobs are queue entries keyed by shard, not grain state, so
  there is no etag-per-job model and nothing to read-on-activate. It reuses the existing
  serializer/value-codec ([04](../04-messaging-and-serialization.md)) for job metadata, and the
  directory/ring + membership snapshot ([06](../06-grain-directory-and-placement.md)) for shard
  claim and failover.
- **Journaling (ADR 0017 — durable journaling, not yet written).** No journaling ADR exists yet
  (the ADR series ends at 0016; durable journaling is the still-unwritten next slot, see
  [docs/13](../13-roadmap-and-phases.md) and [EPICS](../../EPICS.md)). **Durable jobs do not need it,
  and do not wait on it:** Orleans' own `Orleans.DurableJobs` has no dependency on
  `Orleans.Journaling` — durability there is the `IJobShard` persist hooks, which this port maps onto
  the shard store. If a journaling ADR later lands, the shard store *could* optionally journal its
  mutations instead of doing discrete add/remove/retry writes, but that is an implementation choice
  off this feature's critical path, not a prerequisite.

### Public authoring surface (functional-first)

Scheduling mirrors `registerReminder` on the grain runtime; handling mirrors the symbol-observer
hooks; hosting mirrors `useReminders` / `useRedisReminders`.

- **Schedule / cancel**, from any grain:

  ```ts
  const job = await ctx.runtime.scheduleJob({
    target,                      // GrainId of the handler grain
    name: "send-reminder-email",
    dueTime: addHours(now, 24),
    metadata: { userId },
  });
  await ctx.runtime.cancelJob(job); // best-effort; true if removed before it ran
  ```

- **Handle**, in the target grain's factory (functional-first; the class form exposes the same
  handler under the `DURABLE_JOB_HANDLER` symbol):

  ```ts
  export const Mailer = defineGrain("Mailer", (ctx) => {
    useDurableJobHandler(ctx, async (job) => {
      await send(job.metadata.userId);
      // resolve ⇒ Completed; throw ⇒ Failed (retry policy decides);
      // return pollAfter(seconds(5)) ⇒ supervised long-running work
    });
    return { /* ... */ };
  });
  ```

- **Hosting**: `createSilo().useDurableJobs(store?)`, `.useRedisDurableJobs({ url })`,
  `.useMemoryDurableJobs()` (dev/test), with a `DurableJobsOptions` mirroring Orleans'
  (`shardDuration`, `shardActivationBufferPeriod`, `maxConcurrentJobsPerSilo`, the slow-start knobs,
  `shouldRetry`, `maxAdoptedCount`, and the shard-claim ramp-up budget).

## Slice plan (TDD, vertical, testable)

Each slice is demonstrable and tested before the next, following the workflow in
[CLAUDE.md](../../CLAUDE.md) and the roadmap's exit-criteria discipline.

- **Slice 1 — the pure model (no cluster, fake clock).** Shard-key bucketing
  (`floor(dueTime/shardDuration)`), the in-memory due-time queue (priority by due time,
  second-granularity buckets, cancel, retry-later preserving `dequeueCount`), the default retry
  policy, and the claim-budget computation — all as pure functions.
  - *Exit:* unit tests show jobs surface in due-time order; a cancelled job never surfaces; a
    retried job re-surfaces at its new due time with `dequeueCount` incremented; the default policy
    drops after 5 attempts with `2^n` s backoff; the claim budget interpolates and clamps as
    specified. Fully deterministic on the injectable clock.
- **Slice 2 — single-silo end-to-end on the memory store.** Manager + executor + the
  `DURABLE_JOB_HANDLER` receiver, wired through the dispatcher.
  - *Exit:* a scheduled job fires the target grain's handler **as a turn** at its due time; resolve
    removes it; throwing retries with backoff up to N then drops; `cancelJob` before due removes it;
    a handler returning `pollAfter` is re-polled and only removed on completion; the per-silo
    concurrency limit and overload backoff are honoured.
- **Slice 3 — durable store + restart.** The Redis `JobShardStore` implementing the persist hooks.
  - *Exit:* a job scheduled before a silo restart is re-read from the store and fired after restart
    (at-least-once); a job that ran but whose silo died before removal re-runs after adoption; the
    memory store is asserted dev/test-only (no persistence across a fresh store).
- **Slice 4 — multi-silo ownership & failover (kind e2e).** Shard claim from the store, dead-silo
  adoption via the membership snapshot, poison protection, and the claim ramp-up; mirrors the
  existing reminder-failover cluster test.
  - *Exit:* killing a silo causes another to adopt and drain its shards with no lost jobs beyond
    expected at-least-once redelivery; a shard adopted past `maxAdoptedCount` is poisoned and never
    reassigned; a freshly started silo respects the ramp-up budget; graceful drain releases shards
    (owner→null, adopted count reset) for another silo to pick up.

## Scope boundaries and divergences

- **Not a workflow / orchestration engine.** No activities, sub-orchestrations, deterministic
  replay, or durable awaits inside a job body — a job is one scheduled grain invocation. This is the
  defining boundary and it matches Orleans' actual `Orleans.DurableJobs`, not the Temporal/DTFx
  engine the name evokes. A future workflow engine, if ever wanted, is a separate ADR built *on top*
  of durable jobs, not this one.
- **At-least-once, idempotent handlers required.** No exactly-once; no cross-job ordering beyond
  due-time bucketing. Documented as a hard contract on the authoring surface.
- **Time-bucketed shard ownership, not reminders' hash-range ownership** — a deliberate divergence,
  required because jobs are bucketed by due time rather than grain key.
- **Overload signal.** Orleans throttles on `IOverloadDetector`. The port has health/drain plumbing
  but no per-silo load gossip (the same gap [ADR 0016](0016-activation-rebalancer.md) calls out). The
  executor will expose an overload hook; the initial backing is a simple local signal (e.g.
  concurrency-slot saturation / health state), with cluster-aware load deferred to the same future
  work as the rebalancer's load reporting.
- **Memory store is dev/test only** (no-op persistence), exactly as in Orleans.
- **Cancellation** is cooperative via shard takeover (matching Orleans); a `CancellationToken`
  threaded into handler bodies is not part of the first cut. A `cancelJob` race after a job has
  started returns `false`.
- **Deferred:** a query/index surface beyond the returned `DurableJob` handle (e.g. "list jobs for
  target *G*", per-job status lookup), and pluggable non-Redis shard stores ([ADR 0005](0005-redis-default-providers.md)
  keeps Postgres et al. as deferred additional providers, not parity gaps).
