import { randomUUID } from "node:crypto";
import { afterAll, describe, expect, it } from "vitest";
import { Pool } from "pg";
import { grain } from "@thresh/core/decorators";
import { Grain } from "@thresh/core/grain";
import { defineGrainInterface } from "@thresh/core/grain-interface";
import { stableHash32 } from "@thresh/core/hash";
import type { GrainKey } from "@thresh/core/key-kinds";
import { SiloAddress } from "@thresh/core/silo-address";
import type { StreamHandler } from "@thresh/core/stream";
import { ConsistentHashRing } from "@thresh/directory/consistent-hash-ring";
import { InProcessNetwork } from "@thresh/messaging/in-process-transport";
import { StaticMembershipService } from "@thresh/runtime/static-membership";
import { queueRingHash } from "@thresh/streams/queue-ownership";
import { createSilo } from "@thresh/hosting/silo-builder";
import type { SiloHost } from "@thresh/hosting/silo-host";

const PG_URL = process.env.PG_URL ?? "postgres://localhost:5432/postgres";

async function reachable(connectionString: string): Promise<Pool | undefined> {
  const probe = new Pool({ connectionString });
  probe.on("error", () => {});
  try {
    await probe.query("SELECT 1");
    return probe;
  } catch {
    await probe.end().catch(() => undefined);
    return undefined;
  }
}

async function waitFor(predicate: () => boolean, timeoutMs = 4000): Promise<void> {
  const start = Date.now();
  while (!predicate()) {
    if (Date.now() - start > timeoutMs) throw new Error("waitFor: timed out");
    await new Promise((r) => setTimeout(r, 15));
  }
}

const admin = await reachable(PG_URL);
const tablePrefix = `thresh_test_pgcl_${randomUUID().replace(/-/g, "")}`;
const QUEUE_COUNT = 8; // PostgresPullingStreamProvider default

// Cross-activation sinks (a reactivated consumer keeps accumulating here).
const received = new Map<string, string[]>();
const joinedRoom = new Map<string, string>();

const addrs = [0, 1].map((n) => new SiloAddress(`silo-${n}`, `uid-${n}`, `silo-${n}:11111`));
const inRanges = (h: number, ranges: ReadonlyArray<readonly [number, number]>) =>
  ranges.some(([b, e]) => (b <= e ? h >= b && h < e : h >= b || h < e));

/** A room whose physical queue the ring assigns to silo `index`. */
function roomOwnedBy(index: number): string {
  const ranges = new ConsistentHashRing(addrs).rangesFor(addrs[index]!);
  for (let i = 0; ; i++) {
    const room = `room-${i}`;
    const queue = stableHash32(`chat/${room}`) % QUEUE_COUNT;
    if (inRanges(queueRingHash("default", queue), ranges)) return room;
  }
}

afterAll(async () => {
  if (admin === undefined) return;
  for (const suffix of ["events", "cursors", "subscriptions", "failures"]) {
    await admin.query(`DROP TABLE IF EXISTS ${tablePrefix}_${suffix}`);
  }
  await admin.end();
});

interface IChatRoom extends GrainKey<string> {
  say(text: string): Promise<void>;
}
const IChatRoom = defineGrainInterface<IChatRoom>("IChatRoom.pg-cluster");

interface IChatUser extends GrainKey<string> {
  join(room: string): Promise<void>;
  count(): Promise<number>;
}
const IChatUser = defineGrainInterface<IChatUser>("IChatUser.pg-cluster", {
  options: { count: { readOnly: true } },
});

@grain()
class ChatRoomGrain extends Grain implements IChatRoom {
  async say(text: string): Promise<void> {
    await this.runtime.getStreamProvider().getStream<string>("chat", this.id.key).publish(text);
  }
}

@grain()
class ChatUserGrain extends Grain implements IChatUser {
  override async onActivate(): Promise<void> {
    // Re-bind the subscription if this is a fresh activation after reactivation.
    const room = joinedRoom.get(String(this.id.key));
    if (room === undefined) return;
    const stream = this.runtime.getStreamProvider().getStream<string>("chat", room);
    const existing = await stream.getSubscriptions();
    if (existing.length > 0) await existing[0]!.resume(this.handler(room));
  }

  async join(room: string): Promise<void> {
    joinedRoom.set(String(this.id.key), room);
    received.set(String(this.id.key), received.get(String(this.id.key)) ?? []);
    await this.runtime
      .getStreamProvider()
      .getStream<string>("chat", room)
      .subscribe(this.handler(room));
  }

  async count(): Promise<number> {
    return received.get(String(this.id.key))?.length ?? 0;
  }

  private handler(_room: string): StreamHandler<string> {
    const key = String(this.id.key);
    return { onNext: async (text) => void received.get(key)!.push(text) };
  }
}

function buildSilo(local: SiloAddress, membership: StaticMembershipService, net: InProcessNetwork) {
  return createSilo({ clusterId: "c", local, random: () => 0 })
    .useMembership(membership)
    .useInProcessTransport(net)
    .addPostgresStreams("default", { connectionString: PG_URL, tablePrefix })
    .registerGrain(ChatRoomGrain, { interfaces: [IChatRoom] })
    .registerGrain(ChatUserGrain, { interfaces: [IChatUser] })
    .build();
}

describe.skipIf(admin === undefined)("Postgres streams across a cluster", () => {
  it("hands a queue to a surviving silo, which resumes from the committed cursor", async () => {
    received.clear();
    joinedRoom.clear();
    const net = new InProcessNetwork();
    const membership = new StaticMembershipService(addrs[0]!, addrs);
    const silos: SiloHost[] = addrs.map((local) => buildSilo(local, membership, net));
    for (const s of silos) await s.start();

    // The room's queue is owned by silo-1; the consumer activates on silo-0
    // (random -> first candidate), so delivery crosses silos and killing silo-1
    // exercises a genuine queue handoff without disturbing the consumer.
    const room = roomOwnedBy(1);

    try {
      await silos[0]!.getGrain(IChatUser, "alice").join(room);
      await silos[0]!.getGrain(IChatRoom, room).say("m1");
      await silos[0]!.getGrain(IChatRoom, room).say("m2");
      await waitFor(() => (received.get("alice")?.length ?? 0) === 2);

      // silo-1 (the queue owner) dies and leaves the view.
      await silos[1]!.stop();
      membership.removeSilo(addrs[1]!);

      // More events arrive; silo-0 takes the queue over and resumes from cursor 2.
      await silos[0]!.getGrain(IChatRoom, room).say("m3");
      await silos[0]!.getGrain(IChatRoom, room).say("m4");
      await waitFor(() => (received.get("alice")?.length ?? 0) === 4);

      // No gaps, no duplicate redelivery of the pre-handoff events.
      expect(received.get("alice")).toEqual(["m1", "m2", "m3", "m4"]);
    } finally {
      await Promise.all(silos.map((s) => s.stop().catch(() => undefined)));
    }
  }, 30_000);
});

// Issue #64: `addPostgresStreams` must thread the silo's `serviceId` into the
// events/cursors/subscriptions tables `PostgresPullingStreamProvider` builds.
describe.skipIf(admin === undefined)("Postgres streams service identity (issue #64)", () => {
  it("threads the silo's serviceId into the events table's rows", async () => {
    const svcTablePrefix = `thresh_test_pgcl_svc_${randomUUID().replace(/-/g, "")}`;
    const address = new SiloAddress("silo-svc", "uid-svc", "silo-svc:11111");
    const silo = createSilo({ clusterId: "c", serviceId: "svc-a", local: address })
      .useStaticMembership([address])
      .useInProcessTransport(new InProcessNetwork())
      .addPostgresStreams("default", { connectionString: PG_URL, tablePrefix: svcTablePrefix })
      .registerGrain(ChatRoomGrain, { interfaces: [IChatRoom] })
      .build();
    await silo.start();
    try {
      await silo.getGrain(IChatRoom, "room-svc").say("hi");
      const res = await admin!.query(
        `SELECT service_id FROM ${svcTablePrefix}_events WHERE service_id = $1`,
        ["svc-a"],
      );
      expect(res.rowCount).toBeGreaterThan(0);
    } finally {
      await silo.stop();
      for (const suffix of ["events", "cursors", "subscriptions", "failures"]) {
        await admin!.query(`DROP TABLE IF EXISTS ${svcTablePrefix}_${suffix}`);
      }
    }
  }, 10_000);
});
