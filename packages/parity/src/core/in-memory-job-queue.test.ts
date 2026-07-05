// Ported from dotnet/orleans test/Orleans.Core.Tests/DurableJobs/InMemoryJobQueueTests.cs @ v10.1.0 (MIT).
import { describe, expect } from "vitest";
import { orleansTest } from "@tsva/testing/orleans-test";
import { InMemoryJobQueue } from "@tsva/durable-jobs/job-model";

// This framework's InMemoryJobQueue (job-model.ts) is a pure, pull-based due-time
// queue: the caller passes "now" to dequeueDue(now) rather than the queue driving
// an IAsyncEnumerable that itself waits on the clock. Waiting for due time and
// polling loops are the ShardExecutor's job (via TimerHandle/TimeProvider), not
// the queue's — so tests of GetAsyncEnumerator's own waiting/completion/RunId
// generation are excluded below; the ordering/cancel/retry behaviour they also
// cover is ported using dequeueDue/cancel/retryLater directly.

describe("NonSilo.Tests.DurableJobs.InMemoryJobQueueTests", () => {
  const NS = "NonSilo.Tests.DurableJobs.InMemoryJobQueueTests";

  orleansTest(`${NS}.Enqueue_AddsJobToQueue`, () => {
    const queue = new InMemoryJobQueue();
    queue.add({ id: "job1", dueMs: Date.now() + 1000, dequeueCount: 0, payload: "job1" });

    expect(queue.size).toBe(1);
  });

  orleansTest(`${NS}.Enqueue_MultipleJobs_IncreasesCount`, () => {
    const queue = new InMemoryJobQueue();
    queue.add({ id: "job1", dueMs: Date.now() + 1000, dequeueCount: 0, payload: "job1" });
    queue.add({ id: "job2", dueMs: Date.now() + 2000, dequeueCount: 0, payload: "job2" });
    queue.add({ id: "job3", dueMs: Date.now() + 3000, dequeueCount: 0, payload: "job3" });

    expect(queue.size).toBe(3);
  });

  orleansTest.excluded(
    "MarkAsComplete is a completion sentinel for the .NET async-enumerable queue " +
      "(signals GetAsyncEnumerator to stop waiting for more jobs). This framework's queue " +
      "has no such lifecycle state — it is a plain synchronous structure with no " +
      "'accepting more enqueues' flag to enforce.",
    `${NS}.Enqueue_AfterMarkAsComplete_ThrowsInvalidOperationException`,
  );

  orleansTest(`${NS}.GetAsyncEnumerator_ReturnsJobsInDueTimeOrder`, () => {
    const queue = new InMemoryJobQueue();
    const now = Date.now();
    queue.add({ id: "job1", dueMs: now - 100, dequeueCount: 0, payload: "job1" });
    queue.add({ id: "job2", dueMs: now - 50, dequeueCount: 0, payload: "job2" });

    const results = queue.dequeueDue(now);

    expect(results).toHaveLength(2);
    expect(results[0]?.id).toBe("job1");
    expect(results[1]?.id).toBe("job2");
  });

  orleansTest.excluded(
    "The .NET queue increments DequeueCount itself as jobs are dequeued from its " +
      "IAsyncEnumerable. In this framework that responsibility belongs to the " +
      "ShardExecutor (see job-model.ts's dequeueDue doc comment) — the queue's " +
      "dequeueDue deliberately never touches dequeueCount, so this exact behaviour has " +
      "moved to a different component and has no queue-level analog to port.",
    `${NS}.GetAsyncEnumerator_IncrementsDequeueCount`,
  );

  orleansTest.excluded(
    "Same architectural difference as GetAsyncEnumerator_IncrementsDequeueCount: " +
      "dequeueCount bookkeeping on dequeue is the ShardExecutor's responsibility, not " +
      "the queue's, in this framework.",
    `${NS}.GetAsyncEnumerator_WithInitialDequeueCount_IncrementsCorrectly`,
  );

  orleansTest.excluded(
    "The .NET queue's GetAsyncEnumerator internally awaits real time until a job's " +
      "due time. This framework's queue is clock-free (dequeueDue takes an explicit " +
      "'now' from the caller); waiting for due time is the ShardExecutor's poll-timer " +
      "responsibility (see shard-executor.test.ts), not the queue's.",
    `${NS}.GetAsyncEnumerator_WaitsForDueTime`,
  );

  orleansTest.excluded(
    "Depends on the MarkAsComplete completion sentinel, which has no analog here (see " +
      "Enqueue_AfterMarkAsComplete_ThrowsInvalidOperationException).",
    `${NS}.GetAsyncEnumerator_CompletesWhenQueueIsMarkedComplete`,
  );

  orleansTest(`${NS}.CancelJob_RemovesJobFromQueue`, () => {
    const queue = new InMemoryJobQueue();
    queue.add({ id: "job1", dueMs: Date.now() + 5000, dequeueCount: 0, payload: "job1" });

    const removed = queue.cancel("job1");

    expect(removed).toBe(true);
    expect(queue.size).toBe(0);
  });

  orleansTest(`${NS}.CancelJob_PreventsJobFromBeingDequeued`, () => {
    const queue = new InMemoryJobQueue();
    const now = Date.now();
    queue.add({ id: "job1", dueMs: now - 100, dequeueCount: 0, payload: "job1" });
    queue.add({ id: "job2", dueMs: now - 50, dequeueCount: 0, payload: "job2" });
    queue.cancel("job1");

    const results = queue.dequeueDue(now);

    expect(results).toHaveLength(1);
    expect(results[0]?.id).toBe("job2");
  });

  orleansTest(`${NS}.CancelJob_NonExistentJob_DoesNotThrow`, () => {
    const queue = new InMemoryJobQueue();

    const removed = queue.cancel("non-existent-job");

    expect(removed).toBe(false);
    expect(queue.size).toBe(0);
  });

  orleansTest(`${NS}.RetryJobLater_MovesJobToNewDueTime`, () => {
    const queue = new InMemoryJobQueue();
    const now = Date.now();
    queue.add({ id: "job1", dueMs: now + 1000, dequeueCount: 0, payload: "job1" });
    const [job] = queue.dequeueDue(now + 1000);

    queue.retryLater(job!, { seconds: 10 }, now);

    expect(queue.size).toBe(1);
  });

  orleansTest(`${NS}.RetryJobLater_PreservesDequeueCount`, () => {
    const queue = new InMemoryJobQueue();
    const now = Date.now();
    queue.add({ id: "job1", dueMs: now - 100, dequeueCount: 5, payload: "job1" });
    const [job] = queue.dequeueDue(now);

    queue.retryLater({ ...job!, dequeueCount: 5 }, { ms: 50 }, now);

    const requeued = queue.get("job1");
    expect(requeued?.dequeueCount).toBe(5);
    expect(requeued?.id).toBe("job1");
  });

  orleansTest(`${NS}.RetryJobLater_NonExistentJob_DoesNotThrow`, () => {
    const queue = new InMemoryJobQueue();
    const now = Date.now();
    const job = { id: "job1", dueMs: now + 1000, dequeueCount: 1, payload: "job1" };

    queue.retryLater(job, { seconds: 10 }, now);

    expect(queue.size).toBe(0);
  });

  orleansTest(`${NS}.GetAsyncEnumerator_RespectsEmptyBuckets`, () => {
    const queue = new InMemoryJobQueue();
    const dueMs = Date.now() - 100;
    queue.add({ id: "job1", dueMs, dequeueCount: 0, payload: "job1" });
    queue.add({ id: "job2", dueMs, dequeueCount: 0, payload: "job2" });
    queue.cancel("job1");
    queue.cancel("job2");

    const results = queue.dequeueDue(dueMs);

    expect(results).toHaveLength(0);
  });

  orleansTest(`${NS}.GetAsyncEnumerator_HandlesMultipleDueTimes`, () => {
    const queue = new InMemoryJobQueue();
    const now = Date.now();
    queue.add({ id: "job1", dueMs: now - 5000, dequeueCount: 0, payload: "job1" });
    queue.add({ id: "job2", dueMs: now - 3000, dequeueCount: 0, payload: "job2" });
    queue.add({ id: "job3", dueMs: now - 1000, dequeueCount: 0, payload: "job3" });

    const results = queue.dequeueDue(now).map((j) => j.id);

    expect(results).toEqual(["job1", "job2", "job3"]);
  });

  orleansTest.excluded(
    "RunId generation on dequeue is the .NET queue's responsibility; in this framework " +
      "a fresh RunId is assigned by the ShardExecutor when it claims a job for execution " +
      "(see shard-executor.ts), not by the queue — QueuedJob has no runId field.",
    `${NS}.GetAsyncEnumerator_GeneratesUniqueRunIds`,
  );

  orleansTest.excluded(
    "CancellationToken-based enumeration cancellation is .NET-specific; this framework's " +
      "queue exposes only a synchronous dequeueDue/add/cancel API with no async " +
      "enumeration to cancel.",
    `${NS}.GetAsyncEnumerator_CancellationToken_StopsEnumeration`,
  );
});
