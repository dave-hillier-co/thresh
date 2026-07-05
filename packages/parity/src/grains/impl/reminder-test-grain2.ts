// Ported from dotnet/orleans test/Grains/TestInternalGrains/ReminderTestGrain2.cs @ v10.1.0
// (MIT) — only the subset needed for MinimalReminderInterval (start/get/stop a reminder by
// name); see the interface file for what upstream members were left out.
import type { Duration } from "@tsva/core/duration";
import { grain } from "@tsva/core/decorators";
import { Grain } from "@tsva/core/grain";
import type { GrainReminder, Remindable, TickStatus } from "@tsva/core/reminder";
import { IReminderTestGrain2 } from "@tsva/parity/grains/interfaces/reminder-test-grain-interfaces";

export { IReminderTestGrain2 };

@grain({ name: "UnitTests.Grains.ReminderTestGrain2" })
export class ReminderTestGrain2 extends Grain implements IReminderTestGrain2, Remindable {
  async startReminder(reminderName: string, period: Duration): Promise<GrainReminder> {
    await this.runtime.registerReminder(reminderName, period, period);
    const reminder = await this.runtime.getReminder(reminderName);
    if (reminder === undefined) throw new Error("reminder was not registered");
    return reminder;
  }

  getReminderObject(reminderName: string): Promise<GrainReminder | undefined> {
    return this.runtime.getReminder(reminderName);
  }

  async stopReminder(reminder: GrainReminder): Promise<void> {
    await this.runtime.unregisterReminder(reminder.name);
  }

  async receiveReminder(_reminderName: string, _status: TickStatus): Promise<void> {
    // No-op: the ported test only reads the reminder back and stops it, it never
    // waits for a tick to be delivered.
  }
}
