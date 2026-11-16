// W6: whether a request may be resent is decided by one fact — it was refused before it ran, or
// it never left — never by the class or kind of the error that came back. A grain method that
// itself throws a "stale" RejectionError (say, by letting a nested call's rejection escape) has
// run, and resending it would run it again.
import { describe, expect, it } from "vitest";
import { grain } from "@thresh/core/decorators";
import { RejectionError } from "@thresh/core/errors";
import { Grain } from "@thresh/core/grain";
import { defineGrainInterface } from "@thresh/core/grain-interface";
import type { GrainKey } from "@thresh/core/key-kinds";
import { SiloAddress } from "@thresh/core/silo-address";
import { InProcessNetwork, InProcessTransport } from "@thresh/messaging/in-process-transport";
import { ClusterNode } from "@thresh/runtime/cluster-node";
import { StaticMembershipService } from "@thresh/runtime/static-membership";

interface ICounted extends GrainKey<string> {
  run(): Promise<number>;
  throwStale(): Promise<void>;
}
const ICounted = defineGrainInterface<ICounted>("ICounted.resend-safety");

let runs = 0;
let staleThrows = 0;
let onDeactivateGate: Promise<void> = Promise.resolve();
let deactivationStarted: () => void = () => undefined;

@grain()
class CountedGrain extends Grain implements ICounted {
  async run(): Promise<number> {
    runs += 1;
    return runs;
  }

  async throwStale(): Promise<void> {
    staleThrows += 1;
    // What a method does when it lets a nested call's routing rejection escape.
    throw new RejectionError("a nested call found no activation", "noActivation");
  }

  override async onDeactivate(): Promise<void> {
    deactivationStarted();
    await onDeactivateGate;
  }
}

const silo = (n: number) => new SiloAddress(`silo-${n}`, `uid-${n}`, `silo-${n}:2222${n}`);

async function cluster(): Promise<{ nodes: ClusterNode[]; stop: () => Promise<void> }> {
  const network = new InProcessNetwork();
  const addresses = [silo(0), silo(1)];
  const nodes = addresses.map(
    (local) =>
      new ClusterNode({
        local,
        clusterId: "c1",
        membership: new StaticMembershipService(local, addresses),
        transport: new InProcessTransport(network, "c1"),
        random: () => 0, // every grain is placed on silo-0
      }),
  );
  for (const node of nodes) node.registerGrain(CountedGrain, { interfaces: [ICounted] });
  await Promise.all(nodes.map((n) => n.start()));
  return { nodes, stop: () => Promise.all(nodes.map((n) => n.stop())).then(() => undefined) };
}

function deferred(): { promise: Promise<void>; resolve: () => void } {
  let resolve!: () => void;
  const promise = new Promise<void>((r) => (resolve = r));
  return { promise, resolve };
}

describe("resending a remote call (W6)", () => {
  it("does not resend a call whose own method threw a stale-kind rejection: the method runs once", async () => {
    staleThrows = 0;
    const { nodes, stop } = await cluster();
    try {
      // Activate on silo-0 and warm silo-1's location cache, so the call below crosses the wire.
      await nodes[0]!.getGrain(ICounted, "g").run();
      await nodes[1]!.getGrain(ICounted, "g").run();

      const error = await nodes[1]!
        .getGrain(ICounted, "g")
        .throwStale()
        .catch((e: unknown) => e);

      expect(error).toBeInstanceOf(RejectionError);
      expect((error as RejectionError).kind).toBe("noActivation");
      expect(staleThrows).toBe(1);
    } finally {
      await stop();
    }
  });

  it("resends a call refused at admission (held by a migrating activation): it runs once", async () => {
    runs = 0;
    const gate = deferred();
    onDeactivateGate = gate.promise;
    const started = deferred();
    deactivationStarted = started.resolve;
    const { nodes, stop } = await cluster();
    try {
      await nodes[0]!.getGrain(ICounted, "h").run();
      await nodes[1]!.getGrain(ICounted, "h").run();
      expect(runs).toBe(2);

      // Move it to silo-1; its onDeactivate holds the move open.
      const migrating = nodes[0]!.migrateRandomActivations(silo(1), 1);
      await started.promise;

      // silo-1 still routes to silo-0, whose activation holds the call and then refuses it.
      const held = nodes[1]!.getGrain(ICounted, "h").run();
      await new Promise((r) => setTimeout(r, 10));
      expect(runs).toBe(2);
      gate.resolve();
      expect(await migrating).toBe(1);

      await expect(held).resolves.toBe(3);
      expect(runs).toBe(3);
    } finally {
      onDeactivateGate = Promise.resolve();
      deactivationStarted = () => undefined;
      await stop();
    }
  });
});
