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
// This framework's durable-jobs subsystem (`@tsva/durable-jobs`) has no equivalent surface:
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
import { orleansTest } from "@tsva/testing/orleans-test";

const NAMESPACE = "Tester.DurableJobs.InMemoryJobShardManagerTests";

orleansTest.gap(
  "GAP-JOB-SHARD-MANAGER-API",
  `${NAMESPACE}.InMemoryJobShardManager_ShardCreationAndAssignment`,
);
orleansTest.gap(
  "GAP-JOB-SHARD-MANAGER-API",
  `${NAMESPACE}.InMemoryJobShardManager_ReadFrozenShard`,
);
orleansTest.gap("GAP-JOB-SHARD-MANAGER-API", `${NAMESPACE}.InMemoryJobShardManager_LiveShard`);
orleansTest.gap("GAP-JOB-SHARD-MANAGER-API", `${NAMESPACE}.InMemoryJobShardManager_JobMetadata`);
orleansTest.gap(
  "GAP-JOB-SHARD-MANAGER-API",
  `${NAMESPACE}.InMemoryJobShardManager_ConcurrentShardAssignment_OwnershipConflicts`,
);
orleansTest.gap(
  "GAP-JOB-SHARD-MANAGER-API",
  `${NAMESPACE}.InMemoryJobShardManager_ShardMetadataMerge`,
);
orleansTest.gap(
  "GAP-JOB-SHARD-MANAGER-API",
  `${NAMESPACE}.InMemoryJobShardManager_StopProcessingShard`,
);
orleansTest.gap("GAP-JOB-SHARD-MANAGER-API", `${NAMESPACE}.InMemoryJobShardManager_RetryJobLater`);
orleansTest.gap(
  "GAP-JOB-SHARD-MANAGER-API",
  `${NAMESPACE}.InMemoryJobShardManager_JobCancellation`,
);
orleansTest.gap(
  "GAP-JOB-SHARD-MANAGER-API",
  `${NAMESPACE}.InMemoryJobShardManager_ShardRegistrationRetry_IdCollisions`,
);
orleansTest.gap(
  "GAP-JOB-SHARD-MANAGER-API",
  `${NAMESPACE}.InMemoryJobShardManager_UnregisterShard_WithJobsRemaining`,
);
orleansTest.gap(
  "GAP-JOB-SHARD-MANAGER-API",
  `${NAMESPACE}.InMemoryJobShardManager_SlowStart_LimitsOrphanedShardClaims`,
);
orleansTest.gap(
  "GAP-JOB-SHARD-MANAGER-API",
  `${NAMESPACE}.InMemoryJobShardManager_SlowStart_ZeroBudgetClaimsNothing`,
);
orleansTest.gap(
  "GAP-JOB-SHARD-MANAGER-API",
  `${NAMESPACE}.InMemoryJobShardManager_SlowStart_UnlimitedBudgetClaimsAll`,
);
orleansTest.gap(
  "GAP-JOB-SHARD-MANAGER-API",
  `${NAMESPACE}.InMemoryJobShardManager_SlowStart_BudgetExhaustion_DoesNotInflateAdoptedCount`,
);
