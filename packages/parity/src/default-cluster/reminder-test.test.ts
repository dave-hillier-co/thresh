// Ported from dotnet/orleans test/Orleans.DefaultCluster.Tests/ReminderTest.cs @ v10.1.0 (MIT).
import { describe } from "vitest";
import { orleansTest } from "@tsva/testing/orleans-test";

// IsReminderExists needs to read back a single named reminder registered to the
// calling grain (Orleans `IRemindable`/`this.GetReminder(name)`). This framework's
// `GrainRuntime` only exposes registerReminder/unregisterReminder — no way for a
// grain to query whether one of its own reminders exists (GAP-GET-REMINDERS).
describe("DefaultCluster.Tests.ReminderTest", () => {
  orleansTest.gap("GAP-GET-REMINDERS", "DefaultCluster.Tests.ReminderTest.SimpleGrainGetGrain");
});
