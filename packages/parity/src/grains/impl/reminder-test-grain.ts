// Ported from dotnet/orleans test/Grains/TestGrains/ReminderTestGrain.cs @ v10.1.0 (MIT).
import { grain } from "@tsva/core/decorators";
import { Grain } from "@tsva/core/grain";
import type { Remindable, TickStatus } from "@tsva/core/reminder";
import { IReminderTestGrain } from "@tsva/parity/grains/interfaces/reminder-test-grain-interfaces";

export { IReminderTestGrain };

@grain({ name: "UnitTests.Grains.ReminderTestGrain" })
export class ReminderTestGrain extends Grain implements IReminderTestGrain, Remindable {
  async isReminderExists(reminderName: string): Promise<boolean> {
    const reminder = await this.runtime.getReminder(reminderName);
    return reminder !== undefined;
  }

  addReminder(reminderName: string): Promise<void> {
    return this.runtime.registerReminder(reminderName, { days: 1 }, { days: 1 });
  }

  async removeReminder(reminderName: string): Promise<void> {
    const reminder = await this.runtime.getReminder(reminderName);
    if (reminder === undefined) throw new Error("Reminder not found");
    await this.runtime.unregisterReminder(reminderName);
  }

  receiveReminder(_reminderName: string, _status: TickStatus): Promise<void> {
    throw new Error("not supported");
  }
}
