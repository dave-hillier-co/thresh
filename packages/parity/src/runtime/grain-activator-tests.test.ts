// Ported from dotnet/orleans test/Orleans.Runtime.Tests/GrainActivatorTests.cs @ v10.1.0 (MIT).
import { afterAll, beforeAll, describe, expect } from "vitest";
import { orleansTest } from "@tsva/testing/orleans-test";
import { TestCluster } from "@tsva/testing/test-cluster";
import { FakeTimeProvider } from "@tsva/core/test-support/fake-time-provider";
import type { Grain } from "@tsva/core/grain";
import type { GrainId } from "@tsva/core/grain-id";
import type { GrainActivator } from "@tsva/runtime/catalog";
import {
  ExplicitlyRegisteredSimpleDiGrain,
  ISimpleDiGrain,
} from "@tsva/parity/grains/impl/simple-di-grain";
import { randomIntegerKey } from "@tsva/parity/support/keys";

const HARDCODED_VALUE = "Hardcoded Test Value";

/**
 * Mirrors upstream's `HardcodedGrainActivator`: bypasses `new ctor()` entirely,
 * constructing `ExplicitlyRegisteredSimpleDiGrain` with a hardcoded string and
 * the current released-instance count, and counts disposals so a test can
 * observe `DisposeInstance` firing on deactivation.
 */
class HardcodedGrainActivator implements GrainActivator {
  private released = 0;

  createInstance(_ctor: new () => Grain, _id: GrainId): Grain {
    return new ExplicitlyRegisteredSimpleDiGrain(HARDCODED_VALUE, BigInt(this.released));
  }

  disposeInstance(): void {
    this.released += 1;
  }
}

// Fires the collection sweep (default interval 60s). `deactivateOnIdle()`
// short-circuits `isStale()` regardless of age (see `ActivationData.isStale`),
// so this only needs to get a sweep to run at all, not age the activation.
async function forceSweep(time: FakeTimeProvider): Promise<void> {
  time.advance(65_000);
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
    await forceSweep(time);

    const grain3 = cluster.getGrain(ISimpleDiGrain, randomIntegerKey());
    const finalReleasedInstances = await grain3.getLongValue();

    expect(finalReleasedInstances).toBe(initialReleasedInstances + 1n);
  });
});
