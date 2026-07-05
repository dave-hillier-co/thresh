// Ported from dotnet/orleans test/Orleans.Runtime.Tests/MinimalReminderTests.cs @ v10.1.0 (MIT).
import { describe } from "vitest";
import { orleansTest } from "@tsva/testing/orleans-test";

describe("UnitTests.CatalogTests.MinimalReminderTests", () => {
  // Starts a reminder and then calls `GetReminderObject` (Orleans'
  // `IGrainReminder GetReminder(string reminderName)`) to fetch the
  // `IGrainReminder` handle back out before stopping it. This framework's
  // `GrainRuntime` only exposes `registerReminder`/`unregisterReminder` — there
  // is no `getReminder`/`getReminders` surface to look one up after the fact
  // (GAP-GET-REMINDERS).
  orleansTest.gap(
    "GAP-GET-REMINDERS",
    "UnitTests.CatalogTests.MinimalReminderTests.MinimalReminderInterval",
  );
});
