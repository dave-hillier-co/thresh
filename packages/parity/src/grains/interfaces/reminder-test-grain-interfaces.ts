// Ported from dotnet/orleans test/Grains/TestGrainInterfaces/IReminderTestGrain.cs
// and IReminderTestGrain2.cs @ v10.1.0 (MIT).
import type { Duration } from "@thresh/core/duration";
import { defineGrainInterface } from "@thresh/core/grain-interface";
import type { GrainReminder } from "@thresh/core/reminder";
import type { GrainWithGuidKey, GrainWithIntegerKey } from "@thresh/core/key-kinds";

export interface IReminderTestGrain extends GrainWithIntegerKey {
  isReminderExists(reminderName: string): Promise<boolean>;
  addReminder(reminderName: string): Promise<void>;
  removeReminder(reminderName: string): Promise<void>;
}

export const IReminderTestGrain = defineGrainInterface<IReminderTestGrain>(
  "UnitTests.GrainInterfaces.IReminderTestGrain",
);

// Upstream's IReminderTestGrain2 is much larger (persistent state, counters, file-backed
// logging) — only the subset needed to port MinimalReminderInterval (start a reminder,
// fetch it back by name, stop it) is declared here.
export interface IReminderTestGrain2 extends GrainWithGuidKey {
  startReminder(reminderName: string, period: Duration): Promise<GrainReminder>;
  getReminderObject(reminderName: string): Promise<GrainReminder | undefined>;
  stopReminder(reminder: GrainReminder): Promise<void>;
}

export const IReminderTestGrain2 = defineGrainInterface<IReminderTestGrain2>(
  "UnitTests.GrainInterfaces.IReminderTestGrain2",
);
