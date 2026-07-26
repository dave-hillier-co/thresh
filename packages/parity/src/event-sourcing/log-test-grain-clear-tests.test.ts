// Ported from dotnet/orleans test/Orleans.EventSourcing.Tests/EventSourcingTests/LogTestGrainClearTests.cs @ v10.1.0 (MIT).
// Upstream parametrizes over three storage-provider configurations (default,
// shared, custom primary-cluster) that all exercise the same
// log-consistency-provider `Clear()` protocol; this framework has one
// journal-storage substrate, so a single `LogTestGrain` covers the same
// protocol behaviour the theory checks.
import { afterAll, beforeAll, describe, expect } from "vitest";
import { orleansTest } from "@thresh/testing/orleans-test";
import { TestCluster } from "@thresh/testing/test-cluster";
import { ILogTestGrain, LogTestGrain } from "@thresh/parity/grains/impl/log-test-grain";
import { randomIntegerKey } from "@thresh/parity/support/keys";

async function runConcurrentOperationsAroundClear(grain: ILogTestGrain): Promise<unknown[]> {
  const exceptions: unknown[] = [];

  async function run(operation: () => Promise<unknown>): Promise<void> {
    try {
      await operation();
    } catch (error) {
      exceptions.push(error);
    }
  }

  await Promise.all([
    run(() => grain.setALocal(1)),
    run(() => grain.setAGlobal(2)),
    run(() => grain.incrementAGlobal()),
    run(() => grain.clear()),
    run(() => grain.setAGlobal(3)),
    run(() => grain.getAGlobal()),
  ]);

  return exceptions;
}

describe("Tester.EventSourcingTests.LogTestGrainClearTests", () => {
  let cluster: TestCluster;

  beforeAll(async () => {
    cluster = await TestCluster.start({
      initialSilos: 2,
      grains: [{ ctor: LogTestGrain, interfaces: [ILogTestGrain] }],
    });
  });

  afterAll(async () => {
    await cluster.dispose();
  });

  orleansTest(
    "Tester.EventSourcingTests.LogTestGrainClearTests.ClearLog_ResetDropsTentativeAndAllowsFurtherWrites",
    async () => {
      const grain = cluster.getGrain(ILogTestGrain, randomIntegerKey());

      await grain.clear();
      await grain.setAGlobal(10);
      expect(await grain.getAGlobal()).toBe(10);
      expect(await grain.getConfirmedVersion()).toBe(1);

      await grain.setALocal(99);
      await grain.setBLocal(77);
      const tentativeBeforeClear = await grain.getBothLocal();
      expect(tentativeBeforeClear.a).toBe(99);
      expect(tentativeBeforeClear.b).toBe(77);

      await grain.clear();
      expect(await grain.getConfirmedVersion()).toBe(0);

      const confirmedAfterClear = await grain.getBothGlobal();
      expect(confirmedAfterClear.a).toBe(0);
      expect(confirmedAfterClear.b).toBe(0);

      const tentativeAfterClear = await grain.getBothLocal();
      expect(tentativeAfterClear.a).toBe(0);
      expect(tentativeAfterClear.b).toBe(0);

      await grain.setAGlobal(41);
      await grain.incrementAGlobal();
      expect(await grain.getAGlobal()).toBe(42);
      expect(await grain.getConfirmedVersion()).toBe(2);

      await grain.clear();
      const exceptions = await runConcurrentOperationsAroundClear(grain);
      expect(exceptions).toHaveLength(0);

      await grain.clear();
      await grain.setAGlobal(7);
      expect(await grain.getAGlobal()).toBe(7);
      expect(await grain.getConfirmedVersion()).toBe(1);
    },
  );
});
