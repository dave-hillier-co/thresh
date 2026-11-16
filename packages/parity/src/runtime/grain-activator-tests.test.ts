// Ported from dotnet/orleans test/Orleans.Runtime.Tests/GrainActivatorTests.cs @ v10.1.0 (MIT).
import { afterAll, beforeAll, describe, expect } from "vitest";
import { orleansTest } from "@thresh/testing/orleans-test";
import { TestCluster } from "@thresh/testing/test-cluster";
import { FakeTimeProvider } from "@thresh/core/test-support/fake-time-provider";
import type { Grain } from "@thresh/core/grain";
import type { GrainClass } from "@thresh/core/grain-class";
import type { GrainId } from "@thresh/core/grain-id";
import type { GrainActivator } from "@thresh/runtime/catalog";
import {
  ExplicitlyRegisteredSimpleDiGrain,
  ISimpleDiGrain,
} from "@thresh/parity/grains/impl/simple-di-grain";
import { randomIntegerKey } from "@thresh/parity/support/keys";

const HARDCODED_VALUE = "Hardcoded Test Value";

/**
 * Mirrors upstream's `HardcodedGrainActivator`: bypasses `new ctor()` entirely,
 * constructing `ExplicitlyRegisteredSimpleDiGrain` with a hardcoded string and
 * the current released-instance count, and counts disposals so a test can
 * observe `DisposeInstance` firing on deactivation.
 */
class HardcodedGrainActivator implements GrainActivator {
  private released = 0;

  createInstance(_ctor: GrainClass, _id: GrainId): Grain {
    return new ExplicitlyRegisteredSimpleDiGrain(HARDCODED_VALUE, BigInt(this.released));
  }

  disposeInstance(): void {
    this.released += 1;
  }
}

// `deactivateOnIdle()` starts the deactivation as soon as the asking turn
// finishes; let its pipeline (hook, then disposal) run.
async function settleDeactivation(): Promise<void> {
  await new Promise((resolve) => setTimeout(resolve, 0));
}

describe("UnitTests.General.GrainActivatorTests", () => {
  const testClass = "UnitTests.General.GrainActivatorTests";
  let cluster: TestCluster;
  let time: FakeTimeProvider;

  beforeAll(async () => {
    time = new FakeTimeProvider();
    cluster = await TestCluster.start({
      initialSilos: 1,
      time,
      grains: [{ ctor: ExplicitlyRegisteredSimpleDiGrain, interfaces: [ISimpleDiGrain] }],
      configureSilo: (builder) => {
        builder.useGrainActivator(new HardcodedGrainActivator());
      },
    });
  });

  afterAll(async () => {
    await cluster.dispose();
  });

  orleansTest(`${testClass}.CanUseCustomGrainActivatorToCreateGrains`, async () => {
    const grain = cluster.getGrain(ISimpleDiGrain, randomIntegerKey());

    const actual = await grain.getStringValue();

    expect(actual).toBe(HARDCODED_VALUE);
  });

  orleansTest(`${testClass}.CanUseCustomGrainActivatorToReleaseGrains`, async () => {
    const grain1 = cluster.getGrain(ISimpleDiGrain, randomIntegerKey());
    const initialReleasedInstances = await grain1.getLongValue();

    const grain2 = cluster.getGrain(ISimpleDiGrain, randomIntegerKey());
    const secondReleasedInstances = await grain2.getLongValue();

    expect(secondReleasedInstances).toBe(initialReleasedInstances);

    await grain1.doDeactivate();
    await settleDeactivation();

    const grain3 = cluster.getGrain(ISimpleDiGrain, randomIntegerKey());
    const finalReleasedInstances = await grain3.getLongValue();

    expect(finalReleasedInstances).toBe(initialReleasedInstances + 1n);
  });
});
