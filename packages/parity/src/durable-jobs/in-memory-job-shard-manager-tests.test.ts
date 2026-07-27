// Ported from dotnet/orleans test/Orleans.DurableJobs.Tests/DurableJobs/InMemoryJobShardManagerTests.cs @ v10.1.0 (MIT).
//
// Upstream drives `JobShardManagerTestsRunner`
// (test/Orleans.DurableJobs.Tests/DurableJobs/JobShardManagerTestsRunner.cs) against an
// explicit `JobShardManager` / shard object model:
//   - `manager.CreateShardAsync(minDate, maxDate, metadata, ct)` mints a *new* shard with a
//     fresh id (even for an identical date range — see `ShardRegistrationRetry_IdCollisions`)
//     and its own arbitrary string metadata dictionary, independent of any job's metadata;
//   - `manager.AssignJobShardsAsync(horizon, maxNewClaims, ct)` returns every shard this silo
//     owns or newly claims (a slow-start ramp-up budget on newly-claimed *orphaned* shards);
//   - a returned shard is itself an object exposing `TryScheduleJobAsync`,
//     `ConsumeDurableJobsAsync()` (an async-enumerable pull loop), `RemoveJobAsync`,
//     `RetryJobLaterAsync`, `MarkAsCompleteAsync`;
//   - `manager.UnregisterShardAsync(shard, ct)` releases a shard (dropped only if it has no
//     jobs remaining).
//
// This framework's durable-jobs subsystem (`@thresh/durable-jobs`) has no equivalent surface:
// sharding is implicit time-bucketing (`shardKeyFor(dueTime, shardDuration)` — one deterministic
// shard per time bucket, not a freshly minted object per `CreateShardAsync` call), there is no
// per-shard custom metadata dictionary (only per-*job* metadata), and shard ownership/claiming/
// consumption live inside `LocalDurableJobManager` + `ShardExecutor`, driving a target grain's
// `DURABLE_JOB_HANDLER` directly rather than exposing a shard object with its own
// consume/retry/mark-complete methods for a test to drive standalone. `JobShardStore` (the
// pluggable persistence contact) has `claimShard`/`releaseShard`/`listShards`, which cover the
// ownership/adoption/poison mechanics `AssignJobShardsAsync` exercises, but not the rest of the
// object model these tests are written against — porting them 1:1 would test a different API,
// not this one. See `packages/durable-jobs/src/job-shard-store.ts` and
// `packages/durable-jobs/src/local-durable-job-manager.ts`.
import { it } from "vitest";
import { orleansTest } from "@thresh/testing/orleans-test";

const NAMESPACE = "Tester.DurableJobs.InMemoryJobShardManagerTests";

orleansTest.excluded(
  "this framework's durable-jobs subsystem deliberately uses implicit time-bucketing (shardKeyFor(dueTime, shardDuration)) rather than Orleans' explicit JobShardManager/shard-object model (CreateShardAsync/AssignJobShardsAsync returning a shard object with TryScheduleJobAsync/ConsumeDurableJobsAsync/RetryJobLaterAsync/MarkAsCompleteAsync); porting these 1:1 would test a different API, not this one",
  `${NAMESPACE}.InMemoryJobShardManager_ShardCreationAndAssignment`,
);
orleansTest.excluded(
  "this framework's durable-jobs subsystem deliberately uses implicit time-bucketing (shardKeyFor(dueTime, shardDuration)) rather than Orleans' explicit JobShardManager/shard-object model (CreateShardAsync/AssignJobShardsAsync returning a shard object with TryScheduleJobAsync/ConsumeDurableJobsAsync/RetryJobLaterAsync/MarkAsCompleteAsync); porting these 1:1 would test a different API, not this one",
  `${NAMESPACE}.InMemoryJobShardManager_ReadFrozenShard`,
);
orleansTest.excluded(
  "this framework's durable-jobs subsystem deliberately uses implicit time-bucketing (shardKeyFor(dueTime, shardDuration)) rather than Orleans' explicit JobShardManager/shard-object model (CreateShardAsync/AssignJobShardsAsync returning a shard object with TryScheduleJobAsync/ConsumeDurableJobsAsync/RetryJobLaterAsync/MarkAsCompleteAsync); porting these 1:1 would test a different API, not this one",
  `${NAMESPACE}.InMemoryJobShardManager_LiveShard`,
);
orleansTest.excluded(
  "this framework's durable-jobs subsystem deliberately uses implicit time-bucketing (shardKeyFor(dueTime, shardDuration)) rather than Orleans' explicit JobShardManager/shard-object model (CreateShardAsync/AssignJobShardsAsync returning a shard object with TryScheduleJobAsync/ConsumeDurableJobsAsync/RetryJobLaterAsync/MarkAsCompleteAsync); porting these 1:1 would test a different API, not this one",
  `${NAMESPACE}.InMemoryJobShardManager_JobMetadata`,
);
orleansTest.excluded(
  "this framework's durable-jobs subsystem deliberately uses implicit time-bucketing (shardKeyFor(dueTime, shardDuration)) rather than Orleans' explicit JobShardManager/shard-object model (CreateShardAsync/AssignJobShardsAsync returning a shard object with TryScheduleJobAsync/ConsumeDurableJobsAsync/RetryJobLaterAsync/MarkAsCompleteAsync); porting these 1:1 would test a different API, not this one",
  `${NAMESPACE}.InMemoryJobShardManager_ConcurrentShardAssignment_OwnershipConflicts`,
);
orleansTest.excluded(
  "this framework's durable-jobs subsystem deliberately uses implicit time-bucketing (shardKeyFor(dueTime, shardDuration)) rather than Orleans' explicit JobShardManager/shard-object model (CreateShardAsync/AssignJobShardsAsync returning a shard object with TryScheduleJobAsync/ConsumeDurableJobsAsync/RetryJobLaterAsync/MarkAsCompleteAsync); porting these 1:1 would test a different API, not this one",
  `${NAMESPACE}.InMemoryJobShardManager_ShardMetadataMerge`,
);
orleansTest.excluded(
  "this framework's durable-jobs subsystem deliberately uses implicit time-bucketing (shardKeyFor(dueTime, shardDuration)) rather than Orleans' explicit JobShardManager/shard-object model (CreateShardAsync/AssignJobShardsAsync returning a shard object with TryScheduleJobAsync/ConsumeDurableJobsAsync/RetryJobLaterAsync/MarkAsCompleteAsync); porting these 1:1 would test a different API, not this one",
  `${NAMESPACE}.InMemoryJobShardManager_StopProcessingShard`,
);
orleansTest.excluded(
  "this framework's durable-jobs subsystem deliberately uses implicit time-bucketing (shardKeyFor(dueTime, shardDuration)) rather than Orleans' explicit JobShardManager/shard-object model (CreateShardAsync/AssignJobShardsAsync returning a shard object with TryScheduleJobAsync/ConsumeDurableJobsAsync/RetryJobLaterAsync/MarkAsCompleteAsync); porting these 1:1 would test a different API, not this one",
  `${NAMESPACE}.InMemoryJobShardManager_RetryJobLater`,
);
orleansTest.excluded(
  "this framework's durable-jobs subsystem deliberately uses implicit time-bucketing (shardKeyFor(dueTime, shardDuration)) rather than Orleans' explicit JobShardManager/shard-object model (CreateShardAsync/AssignJobShardsAsync returning a shard object with TryScheduleJobAsync/ConsumeDurableJobsAsync/RetryJobLaterAsync/MarkAsCompleteAsync); porting these 1:1 would test a different API, not this one",
  `${NAMESPACE}.InMemoryJobShardManager_JobCancellation`,
);
orleansTest.excluded(
  "this framework's durable-jobs subsystem deliberately uses implicit time-bucketing (shardKeyFor(dueTime, shardDuration)) rather than Orleans' explicit JobShardManager/shard-object model (CreateShardAsync/AssignJobShardsAsync returning a shard object with TryScheduleJobAsync/ConsumeDurableJobsAsync/RetryJobLaterAsync/MarkAsCompleteAsync); porting these 1:1 would test a different API, not this one",
  `${NAMESPACE}.InMemoryJobShardManager_ShardRegistrationRetry_IdCollisions`,
);
orleansTest.excluded(
  "this framework's durable-jobs subsystem deliberately uses implicit time-bucketing (shardKeyFor(dueTime, shardDuration)) rather than Orleans' explicit JobShardManager/shard-object model (CreateShardAsync/AssignJobShardsAsync returning a shard object with TryScheduleJobAsync/ConsumeDurableJobsAsync/RetryJobLaterAsync/MarkAsCompleteAsync); porting these 1:1 would test a different API, not this one",
  `${NAMESPACE}.InMemoryJobShardManager_UnregisterShard_WithJobsRemaining`,
);
orleansTest.excluded(
  "this framework's durable-jobs subsystem deliberately uses implicit time-bucketing (shardKeyFor(dueTime, shardDuration)) rather than Orleans' explicit JobShardManager/shard-object model (CreateShardAsync/AssignJobShardsAsync returning a shard object with TryScheduleJobAsync/ConsumeDurableJobsAsync/RetryJobLaterAsync/MarkAsCompleteAsync); porting these 1:1 would test a different API, not this one",
  `${NAMESPACE}.InMemoryJobShardManager_SlowStart_LimitsOrphanedShardClaims`,
);
orleansTest.excluded(
  "this framework's durable-jobs subsystem deliberately uses implicit time-bucketing (shardKeyFor(dueTime, shardDuration)) rather than Orleans' explicit JobShardManager/shard-object model (CreateShardAsync/AssignJobShardsAsync returning a shard object with TryScheduleJobAsync/ConsumeDurableJobsAsync/RetryJobLaterAsync/MarkAsCompleteAsync); porting these 1:1 would test a different API, not this one",
  `${NAMESPACE}.InMemoryJobShardManager_SlowStart_ZeroBudgetClaimsNothing`,
);
orleansTest.excluded(
  "this framework's durable-jobs subsystem deliberately uses implicit time-bucketing (shardKeyFor(dueTime, shardDuration)) rather than Orleans' explicit JobShardManager/shard-object model (CreateShardAsync/AssignJobShardsAsync returning a shard object with TryScheduleJobAsync/ConsumeDurableJobsAsync/RetryJobLaterAsync/MarkAsCompleteAsync); porting these 1:1 would test a different API, not this one",
  `${NAMESPACE}.InMemoryJobShardManager_SlowStart_UnlimitedBudgetClaimsAll`,
);
orleansTest.excluded(
  "this framework's durable-jobs subsystem deliberately uses implicit time-bucketing (shardKeyFor(dueTime, shardDuration)) rather than Orleans' explicit JobShardManager/shard-object model (CreateShardAsync/AssignJobShardsAsync returning a shard object with TryScheduleJobAsync/ConsumeDurableJobsAsync/RetryJobLaterAsync/MarkAsCompleteAsync); porting these 1:1 would test a different API, not this one",
  `${NAMESPACE}.InMemoryJobShardManager_SlowStart_BudgetExhaustion_DoesNotInflateAdoptedCount`,
);

// vitest requires at least one runtime test per file; every upstream Fact is
// orleansTest.excluded above.
it.skip("(all tests in this file are orleansTest.excluded — see above)", () => undefined);
