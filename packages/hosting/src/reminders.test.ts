import { describe, expect, it } from "vitest";
import { grain } from "@tsva/core/decorators";
import { Grain } from "@tsva/core/grain";
import { defineGrainInterface } from "@tsva/core/grain-interface";
import type { GrainWithStringKey } from "@tsva/core/key-kinds";
import type { Remindable, TickStatus } from "@tsva/core/reminder";
import { SiloAddress } from "@tsva/core/silo-address";
import { FakeTimeProvider } from "@tsva/core/test-support/fake-time-provider";
import { InProcessNetwork } from "@tsva/messaging/in-process-transport";
import { MemoryReminderTable } from "@tsva/reminders/memory-reminder-table";
import { createSilo } from "@tsva/hosting/silo-builder";

// receiveReminder counts are kept in a module sink so they're observable across
// (re)activations — the grain instance itself may be a fresh activation.
const checks = new Map<string, number>();

interface IBilling extends GrainWithStringKey {
  scheduleSelfCheck(): Promise<void>;
  checkCount(): Promise<number>;
}
const IBilling = defineGrainInterface<IBilling>("IBilling.reminders");

@grain()
class BillingGrain extends Grain implements IBilling, Remindable {
  async scheduleSelfCheck(): Promise<void> {
    await this.runtime.registerReminder("self-check", { seconds: 60 }, { seconds: 60 });
  }
  async checkCount(): Promise<number> {
    return checks.get(String(this.id.key)) ?? 0;
  }
  async receiveReminder(name: string, _status: TickStatus): Promise<void> {
    if (name === "self-check") {
      const key = String(this.id.key);
      checks.set(key, (checks.get(key) ?? 0) + 1);
    }
  }
}

const local = new SiloAddress("silo-0", "uid-0", "silo-0:11111");

describe("reminders end-to-end", () => {
  it("fires a registered reminder, delivering receiveReminder as a turn", async () => {
    checks.clear();
    const time = new FakeTimeProvider();
    const silo = createSilo({ clusterId: "c1", local, time })
      .useStaticMembership([local])
      .useInProcessTransport(new InProcessNetwork())
      .useReminders(new MemoryReminderTable())
      .registerGrain(BillingGrain, { interfaces: [IBilling] })
      .build();
    await silo.start();
    try {
      await silo.getGrain(IBilling, "acct").scheduleSelfCheck();
      time.advance(180_000); // three 60s periods
      await new Promise((r) => setTimeout(r, 0));
      expect(await silo.getGrain(IBilling, "acct").checkCount()).toBe(3);
    } finally {
      await silo.stop();
    }
  });

  it("throws if a grain registers a reminder on a silo without reminders configured", async () => {
    const silo = createSilo({ clusterId: "c1", local })
      .useStaticMembership([local])
      .useInProcessTransport(new InProcessNetwork())
      .registerGrain(BillingGrain, { interfaces: [IBilling] })
      .build();
    await silo.start();
    try {
      await expect(silo.getGrain(IBilling, "acct").scheduleSelfCheck()).rejects.toThrow(
        /reminders/,
      );
    } finally {
      await silo.stop();
    }
  });
});
