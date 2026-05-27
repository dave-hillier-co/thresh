# ADR 0018 — Durable jobs (sharded, durable, at-least-once scheduled execution)

- Status: Proposed — design only. Slice plan below.
- Context docs: [07 — Persistence](../07-persistence.md),
  [06 — Directory and placement](../06-grain-directory-and-placement.md),
  [13 — Roadmap](../13-roadmap-and-phases.md); [ADR 0005](0005-redis-default-providers.md),
  [ADR 0009](0009-functional-grains.md), [ADR 0016](0016-activation-rebalancer.md).

> Orleans references: `Orleans.DurableJobs/{DurableJob,IDurableJobHandler,DurableJobRunResult,ScheduleJobRequest,ILocalDurableJobManager,IDurableJobReceiverExtension,JobShard,InMemoryJobQueue,JobShardManager,ShardExecutor,LocalDurableJobManager}.cs`,
> `Hosting/{DurableJobsOptions,DurableJobsExtensions}.cs`. The whole feature lives in one assembly with
> **no `Orleans.Journaling` dependency**.

## Context

Reminders (`@tsva/reminders`) give durable per-grain wakeups keyed by grain hash — but they don't
scale to "schedule millions of one-shot callbacks for arbitrary future times, run them with bounded
per-silo concurrency, retry failures, and don't melt a silo that just rejoined," and have no retry,
concurrency, or back-pressure model. Orleans 10's `Orleans.DurableJobs` covers this.

**Despite the name it is not a workflow engine** — no activities, sub-orchestrations, deterministic
replay, or durable awaits. The unit of work is a **single scheduled invocation of a target grain at a
due time**, made durable, sharded for scale, retried, and rebalanced on membership change — i.e.
"reminders at scale, one-shot, with retries, concurrency control, and crash-failover of the work."

**Orleans' model:** a `DurableJob` `{ Id, Name, DueTime, TargetGrainId, ShardId, Metadata }` names a
target grain (implementing `IDurableJobHandler.ExecuteJobAsync`). Jobs are bucketed into **time
shards** (`shardKey = floor(DueTime / ShardDuration)`, default 1h), each owned by **one silo at a
time** — single-writer at *shard* granularity, and sharding by *time* (not grain key) lets a burst at
one grain spread across silos. `IJobShard`'s `PersistAdd/Remove/Retry` hooks are the only storage
contact; a due-time priority queue is the working set. A per-silo `LocalDurableJobManager`
(`SystemTarget`) buckets-and-schedules and periodically activates shards near their start; a
`ShardExecutor` consumes due jobs under a concurrency limiter (with slow-start and overload backoff)
and dispatches to the target. `DurableJobRunResult` is `Completed` (remove) / `PollAfter(delay)`
(supervised re-poll holding the slot) / `Failed` (retry policy: default 5 attempts, `2^n`s backoff).
Failover: a silo claims its own, orphaned, and dead-silo shards; a shard adopted from a dead owner
past `MaxAdoptedCount` (3) is **poisoned**; a fresh silo claims under a ramp-up budget. **At-least-once**
— handlers must be idempotent.

## Decision

Port it faithfully as **`@tsva/durable-jobs`**, layered on the runtime the way `@tsva/reminders` is
(per-silo service + durable table + memory/Redis backings), authored functional-first, reusing the
port's clock, membership, ring-leadership, dispatcher and Redis-default seams — **not** as a workflow
engine.

- **Package** — `DurableJob` / `ScheduleJobRequest` / `JobRunContext` / `DurableJobRunResult` types;
  `local-durable-job-manager.ts` (clock-driven, membership-reconciled, claim-budget ramp-up);
  `job-shard.ts` + `in-memory-job-queue.ts`; `shard-executor.ts` (concurrency limiter, slow-start,
  overload backoff, poll loop, retry); `job-shard-store.ts` contract with memory (dev/test) and Redis
  (default) backings.
- **Target grain** — opts in via a `DURABLE_JOB_HANDLER` symbol-keyed handler (the idiom
  `STREAM_SUBSCRIPTION_OBSERVER` / `BROADCAST_CHANNEL_OBSERVER` already use); the executor dispatches
  over the existing dispatcher (the `receiveReminder` path), with a per-activation receiver shim
  tracking `RunId → running task` for poll status.
- **Ownership is the time-bucketed shard** claimed from the store with owner/adopt/poison bookkeeping
  — deliberately **not** reminders' hash-range ownership, since jobs bucket by due time. Reminders and
  durable jobs are complementary.
- **Storage** is the shard store's own durable table (Redis default), not `GrainStorage` (jobs are
  queue entries, not grain state). **No journaling dependency** — Orleans' doesn't have one either;
  the persist hooks are the durability seam.

### Authoring surface

```ts
// schedule / cancel, from any grain
const job = await ctx.runtime.scheduleJob({ target, name: "send-email", dueTime, metadata });
await ctx.runtime.cancelJob(job); // best-effort

// handle, in the target grain (functional; class form exposes DURABLE_JOB_HANDLER)
useDurableJobHandler(ctx, async (job) => {
  await send(job.metadata.userId);
  // resolve ⇒ Completed; throw ⇒ Failed (retry policy); return pollAfter(seconds(5)) ⇒ supervised
});

// hosting
createSilo().useRedisDurableJobs({ url }); // or .useMemoryDurableJobs() (dev/test)
```

`DurableJobsOptions` mirrors Orleans (`shardDuration`, `shardActivationBufferPeriod`,
`maxConcurrentJobsPerSilo`, slow-start knobs, `shouldRetry`, `maxAdoptedCount`, claim ramp-up budget).

## Slice plan (TDD, vertical)

1. **Pure model (no cluster, fake clock)** — shard-key bucketing, the due-time queue (cancel,
   retry-later preserving `dequeueCount`), the default retry policy, the claim-budget computation.
2. **Single-silo e2e on the memory store** — manager + executor + `DURABLE_JOB_HANDLER` receiver: a
   job fires as a turn at due time; resolve removes, throw retries then drops, `cancelJob` removes,
   `pollAfter` re-polls; concurrency limit + overload backoff honoured.
3. **Durable store + restart** — Redis `JobShardStore`: a job scheduled before restart fires after;
   a job whose silo died re-runs after adoption.
4. **Multi-silo ownership & failover (kind e2e)** — shard claim, dead-silo adoption, poison protection,
   ramp-up; killing a silo adopts its shards with no lost jobs; graceful drain releases shards.

## Scope boundaries

- **Not a workflow engine** — one scheduled invocation per job; a workflow engine, if ever wanted, is
  a separate ADR on top.
- **At-least-once, idempotent handlers required**; no exactly-once, no cross-job ordering beyond
  due-time bucketing.
- **Time-bucketed shard ownership**, not reminders' hash-range ownership.
- **Overload signal** starts as a local hook (concurrency saturation / health); cluster-aware load is
  deferred to the same work as [ADR 0016](0016-activation-rebalancer.md)'s load reporting.
- **Deferred**: a job query/index surface, and non-Redis shard stores.
