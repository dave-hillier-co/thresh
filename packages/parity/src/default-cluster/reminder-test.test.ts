// Ported from dotnet/orleans test/Orleans.DefaultCluster.Tests/ReminderTest.cs @ v10.1.0 (MIT).
import { afterAll, beforeAll, describe, expect } from "vitest";
import { orleansTest } from "@tsva/testing/orleans-test";
import { TestCluster } from "@tsva/testing/test-cluster";
import { randomIntegerKey } from "@tsva/parity/support/keys";
import {
  IReminderTestGrain,
  ReminderTestGrain,
} from "@tsva/parity/grains/impl/reminder-test-grain";

describe("DefaultCluster.Tests.ReminderTest", () => {
  let cluster: TestCluster;

  beforeAll(async () => {
    cluster = await TestCluster.start({
      grains: [{ ctor: ReminderTestGrain, interfaces: [IReminderTestGrain] }],
    });
  });

  afterAll(async () => {
    await cluster.dispose();
  });

  orleansTest("DefaultCluster.Tests.ReminderTest.SimpleGrainGetGrain", async () => {
    const grain = cluster.getGrain(IReminderTestGrain, randomIntegerKey());
    const notExists = await grain.isReminderExists("not exists");
    expect(notExists).toBe(false);

    await grain.addReminder("dummy");
    expect(await grain.isReminderExists("dummy")).toBe(true);

    await grain.removeReminder("dummy");
    expect(await grain.isReminderExists("dummy")).toBe(false);
  });
});
