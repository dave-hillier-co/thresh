// Ported from dotnet/orleans test/Orleans.EventSourcing.Tests/EventSourcingTests/CountersGrainTests.cs @ v10.1.0 (MIT).
import { afterAll, beforeAll, describe, expect } from "vitest";
import { orleansTest } from "@tsva/testing/orleans-test";
import { TestCluster } from "@tsva/testing/test-cluster";
import { CountersGrain, ICountersGrain } from "@tsva/parity/grains/impl/counters-grain";
import { concurrentIncrementsRunner } from "@tsva/parity/event-sourcing/counters-runner";

describe("Tester.EventSourcingTests.CountersGrainTests", () => {
  let cluster: TestCluster;

  beforeAll(async () => {
    cluster = await TestCluster.start({
      initialSilos: 2,
      grains: [{ ctor: CountersGrain, interfaces: [ICountersGrain] }],
    });
  });

  afterAll(async () => {
    await cluster.dispose();
  });

  orleansTest("Tester.EventSourcingTests.CountersGrainTests.Record", async () => {
    const grain = cluster.getGrain(ICountersGrain, 0n);

    const currentState = await grain.getTentativeState();
    expect(currentState).not.toBeNull();
    expect(Object.keys(currentState)).toHaveLength(0);

    await grain.add("Alice", 1, false);
    await grain.add("Alice", 1, false);
    await grain.add("Alice", 1, false);

    // all three updates should be visible in the tentative count (even if not confirmed yet)
    expect(await grain.getTentativeCount("Alice")).toBe(3);

    // reset all counters to zero, and wait for confirmation
    await grain.reset(true);

    expect(Object.keys(await grain.getTentativeState())).toHaveLength(0);
  });

  orleansTest("Tester.EventSourcingTests.CountersGrainTests.ConcurrentIncrements", async () => {
    const grain = cluster.getGrain(ICountersGrain, 0n);
    await concurrentIncrementsRunner(grain, 50, false);
  });
});
