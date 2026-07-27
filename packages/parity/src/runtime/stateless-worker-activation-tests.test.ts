// Ported from dotnet/orleans test/Orleans.Runtime.Tests/StatelessWorkerActivationTests.cs @ v10.1.0 (MIT).
import { afterAll, beforeAll, describe, expect } from "vitest";
import { IManagementGrain } from "@thresh/core/management-grain";
import { orleansTest } from "@thresh/testing/orleans-test";
import { TestCluster } from "@thresh/testing/test-cluster";
import {
  IStatelessWorkerGrain,
  IStatelessWorkerScalingGrain,
  StatelessWorkerGrain,
  StatelessWorkerScalingGrain,
} from "@thresh/parity/grains/impl/stateless-worker-grain";

/** Upstream's `Until(condition, maxTimeout)` polling helper. */
async function until(condition: () => Promise<boolean>, maxTimeoutMs = 40_000): Promise<void> {
  let remaining = maxTimeoutMs;
  while (!(await condition()) && (remaining -= 10) > 0) {
    await new Promise((r) => setTimeout(r, 10));
  }
  expect(remaining).toBeGreaterThan(0);
}

describe("UnitTests.General.StatelessWorkerActivationTests", () => {
  let cluster: TestCluster;

  beforeAll(async () => {
    cluster = await TestCluster.start({
      initialSilos: 1,
      grains: [
        { ctor: StatelessWorkerScalingGrain, interfaces: [IStatelessWorkerScalingGrain] },
        { ctor: StatelessWorkerGrain, interfaces: [IStatelessWorkerGrain] },
      ],
    });
  });

  afterAll(async () => {
    await cluster.dispose();
  });

  orleansTest(
    "UnitTests.General.StatelessWorkerActivationTests.SingleWorkerInvocationUnderLoad",
    async () => {
      const workerGrain = cluster.getGrain(IStatelessWorkerScalingGrain, 0n);
      const managementGrain = cluster.getGrain(IManagementGrain, 0n);

      for (let i = 0; i < 100; i += 1) {
        await workerGrain.getActivationCount();
        await until(
          async () => 1 === (await managementGrain.getGrainActivationCount(workerGrain)),
          5_000,
        );
      }
    },
  );

  orleansTest(
    "UnitTests.General.StatelessWorkerActivationTests.MultipleWorkerInvocationUnderLoad",
    async () => {
      const MaxLocalWorkers = 4;
      const waiters: Array<Promise<void>> = [];
      const worker = cluster.getGrain(IStatelessWorkerScalingGrain, 1n);

      // Initially, only one activation exists.
      let activationCount = await worker.getActivationCount();
      expect(activationCount).toBe(1);

      // First blocking call triggers creation of a second activation.
      waiters.push(worker.wait());
      await until(async () => 2 === (await worker.getActivationCount()));
      activationCount = await worker.getActivationCount();
      expect(activationCount).toBe(2);

      waiters.push(worker.wait());
      await until(async () => 3 === (await worker.getActivationCount()));
      activationCount = await worker.getActivationCount();
      expect(activationCount).toBe(3);

      waiters.push(worker.wait());
      await until(async () => 4 === (await worker.getActivationCount()));
      activationCount = await worker.getActivationCount();
      expect(activationCount).toBe(4);

      let waitingCount = await worker.getWaitingCount();
      expect(waitingCount).toBe(3);

      for (let i = 0; i < MaxLocalWorkers; i += 1) waiters.push(worker.wait());

      await until(async () => MaxLocalWorkers === (await worker.getActivationCount()));
      await until(async () => MaxLocalWorkers === (await worker.getWaitingCount()));
      activationCount = await worker.getActivationCount();
      expect(activationCount).toBe(MaxLocalWorkers);
      waitingCount = await worker.getWaitingCount();
      expect(waitingCount).toBe(MaxLocalWorkers);

      // Release all the waiting workers to clean up.
      for (let i = 0; i < waiters.length; i += 1) await worker.release();

      // Ensure all blocking calls complete properly.
      await Promise.all(waiters);
    },
  );

  orleansTest(
    "UnitTests.General.StatelessWorkerActivationTests.CatalogCleanupOnDeactivation",
    async () => {
      const workerGrain = cluster.getGrain(IStatelessWorkerGrain, 0n);
      const mgmt = cluster.getGrain(IManagementGrain, 0n);

      let numActivations = await mgmt.getGrainActivationCount(workerGrain);
      expect(numActivations).toBe(0);

      // Activate the grain with a dummy call.
      await workerGrain.dummyCall();

      numActivations = await mgmt.getGrainActivationCount(workerGrain);
      expect(numActivations).toBe(1);

      // Force immediate activation collection to trigger deactivation.
      await mgmt.forceActivationCollection({ ms: 0 });

      // The activation count for the stateless worker grain should become 0 again.
      await until(async () => 0 === (await mgmt.getGrainActivationCount(workerGrain)), 5_000);
    },
  );
});
