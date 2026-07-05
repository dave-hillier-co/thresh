// Ported from dotnet/orleans test/Orleans.Runtime.Tests/MinimalReminderTests.cs @ v10.1.0 (MIT).
import { afterAll, beforeAll, describe, expect } from "vitest";
import { orleansTest } from "@tsva/testing/orleans-test";
import { TestCluster } from "@tsva/testing/test-cluster";
import type { Duration } from "@tsva/core/duration";
import { FakeTimeProvider } from "@tsva/core/test-support/fake-time-provider";
import { randomGuidKey } from "@tsva/parity/support/keys";
import {
  IReminderTestGrain2,
  ReminderTestGrain2,
} from "@tsva/parity/grains/impl/reminder-test-grain2";

describe("UnitTests.CatalogTests.MinimalReminderTests", () => {
  let cluster: TestCluster;
  let time: FakeTimeProvider;

  beforeAll(async () => {
    time = new FakeTimeProvider();
    cluster = await TestCluster.start({
      time,
      grains: [{ ctor: ReminderTestGrain2, interfaces: [IReminderTestGrain2] }],
    });
  });

  afterAll(async () => {
    await cluster.dispose();
  });

  // Upstream registers the reminder with a 100ms period; this framework enforces a
  // 1-minute minimum reminder period (Orleans' own default `MinimumReminderPeriod`,
  // hard-coded by `SiloBuilder.useReminders` — unrelated to GAP-GET-REMINDERS), so the
  // period here is widened to the minimum. The behavior under test — starting a
  // reminder, then calling `GetReminderObject` (this framework's `getReminder`) to
  // fetch it back before stopping it — is unaffected by the period's exact value.
  orleansTest("UnitTests.CatalogTests.MinimalReminderTests.MinimalReminderInterval", async () => {
    const reminderName = "minimal_reminder";
    const period: Duration = { seconds: 60 };

    const reminderGrain = cluster.getGrain(IReminderTestGrain2, randomGuidKey());
    const started = await reminderGrain.startReminder(reminderName, period);
    expect(started.name).toBe(reminderName);

    const r = await reminderGrain.getReminderObject(reminderName);
    expect(r).toBeDefined();
    await reminderGrain.stopReminder(r!);
  });
});
