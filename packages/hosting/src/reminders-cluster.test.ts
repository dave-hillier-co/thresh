import { describe, expect, it } from "vitest";
import { grain } from "@thresh/core/decorators";
import { Grain } from "@thresh/core/grain";
import { GrainId } from "@thresh/core/grain-id";
import { defineGrainInterface } from "@thresh/core/grain-interface";
import type { GrainWithStringKey } from "@thresh/core/key-kinds";
import type { Remindable, TickStatus } from "@thresh/core/reminder";
import { SiloAddress } from "@thresh/core/silo-address";
import { FakeTimeProvider } from "@thresh/core/test-support/fake-time-provider";
import { ConsistentHashRing } from "@thresh/directory/consistent-hash-ring";
import { InProcessNetwork } from "@thresh/messaging/in-process-transport";
import { MemoryReminderTable } from "@thresh/reminders/memory-reminder-table";
import { StaticMembershipService } from "@thresh/runtime/static-membership";
import { createSilo } from "@thresh/hosting/silo-builder";
import type { SiloHost } from "@thresh/hosting/silo-host";

interface IBeacon extends GrainWithStringKey {
  begin(): Promise<void>;
}
const IBeacon = defineGrainInterface<IBeacon>("IBeacon.reminders");

// Total ticks delivered anywhere in the cluster (a second activation would show up here).
let totalTicks = 0;

@grain()
class BeaconGrain extends Grain implements IBeacon, Remindable {
  async begin(): Promise<void> {
    await this.runtime.registerReminder("tick", { ms: 5000 }, { ms: 1_000_000 });
  }
  async receiveReminder(_name: string, _status: TickStatus): Promise<void> {
    totalTicks++;
  }
}

const flush = () => new Promise((r) => setTimeout(r, 0));
const addrs = [0, 1, 2].map((n) => new SiloAddress(`silo-${n}`, `uid-${n}`, `silo-${n}:11111`));

const inRanges = (h: number, ranges: ReadonlyArray<readonly [number, number]>) =>
  ranges.some(([b, e]) => (b <= e ? h >= b && h < e : h >= b || h < e));

/** A beacon key whose reminder hash is owned by a silo other than placement (silo-0). */
function keyOwnedBy(index: number): string {
  const ring = new ConsistentHashRing(addrs);
  const ranges = ring.rangesFor(addrs[index]!);
  for (let i = 0; ; i++) {
    const key = `beacon-${i}`;
    if (inRanges(new GrainId("Beacon", key).getUniformHashCode(), ranges)) return key;
  }
}

describe("reminders across a cluster", () => {
  it("fires a reminder once and delivers it to the grain's single activation", async () => {
    totalTicks = 0;
    const time = new FakeTimeProvider();
    const net = new InProcessNetwork();
    const table = new MemoryReminderTable();
    const membership = new StaticMembershipService(addrs[0]!, addrs);

    const silos: SiloHost[] = addrs.map((local) =>
      createSilo({ clusterId: "c", local, time, random: () => 0, reminderRefreshSeconds: 1 })
        .useMembership(membership)
        .useInProcessTransport(net)
        .useReminders(table)
        .registerGrain(BeaconGrain, { interfaces: [IBeacon] })
        .build(),
    );
    for (const s of silos) await s.start();

    try {
      // Placement (random -> first candidate) puts the activation on silo-0, but
      // the reminder hash is owned by silo-1 — so firing must route across silos.
      const key = keyOwnedBy(1);
      const grainId = new GrainId("Beacon", key);
      await silos[1]!.getGrain(IBeacon, key).begin();
      expect(silos[0]!.isActive(grainId)).toBe(true);

      // silo-1 discovers the reminder via its periodic table refresh, then fires it.
      time.advance(1000);
      await flush();
      time.advance(5000);
      await flush();
      await flush();

      expect(totalTicks).toBe(1);
      // Exactly one activation cluster-wide: delivery did not spawn one on the owner.
      expect(silos.filter((s) => s.isActive(grainId))).toHaveLength(1);
      expect(silos[0]!.isActive(grainId)).toBe(true);
    } finally {
      await Promise.all(silos.map((s) => s.stop()));
    }
  });
});
