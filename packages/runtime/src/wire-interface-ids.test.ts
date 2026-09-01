import { describe, expect, it } from "vitest";
import {
  BroadcastChannelPublisherInterface,
  BroadcastConsumerInterface,
} from "@thresh/core/broadcast-channel";
import { DurableJobConsumerInterface } from "@thresh/core/durable-job";
import { stableHash32 } from "@thresh/core/hash";
import { IManagementGrain } from "@thresh/core/management-grain";
import { RemindableInterface } from "@thresh/core/reminder";
import { StreamConsumerInterface } from "@thresh/core/stream";
import { TransactionResourceInterface } from "@thresh/core/transaction-resource";
import { ICancellationSourcesExtension } from "./cancellation-extension";
import { IGrainManagementExtension } from "./grain-management-extension";

/**
 * An interface id is `stableHash32(name)` and travels on the wire, so an
 * interface *name* is a wire format. These are the system interfaces both ends
 * of a call resolve by id without a shared static type — a rename would be
 * invisible at compile time and would strand in-flight calls, older silos and
 * persisted grain references against an id nothing answers to.
 *
 * The literals below are therefore the format, not a restatement of the
 * implementation: changing one is a breaking protocol change, and this test
 * exists to make that decision explicit rather than incidental.
 */
describe("wire-visible system interface ids", () => {
  const pinned: ReadonlyArray<readonly [string, number, { name: string; id: number }]> = [
    ["system.Remindable", 3_071_457_241, RemindableInterface],
    ["system.StreamConsumer", 1_621_429_668, StreamConsumerInterface],
    ["system.BroadcastConsumer", 3_578_580_190, BroadcastConsumerInterface],
    ["system.BroadcastChannelPublisher", 2_419_568_341, BroadcastChannelPublisherInterface],
    ["system.DurableJobConsumer", 552_335_097, DurableJobConsumerInterface],
    ["system.TransactionResource", 605_437_587, TransactionResourceInterface],
    ["Orleans.Runtime.IManagementGrain", 421_952_311, IManagementGrain],
    ["$cancellation", 2_202_107_751, ICancellationSourcesExtension],
    ["$management", 3_491_455_527, IGrainManagementExtension],
  ];

  it.each(pinned)("keeps %s pinned to id %d", (name, id, iface) => {
    expect(iface.name).toBe(name);
    expect(iface.id).toBe(id);
    expect(stableHash32(name)).toBe(id);
  });

  it("gives every system interface a distinct id", () => {
    const ids = new Set(pinned.map(([, id]) => id));
    expect(ids.size).toBe(pinned.length);
  });
});
