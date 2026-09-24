// Regression coverage for GH #91 on the distributed path: a REMOTE caller's
// call reaching an activation mid-migration must be HELD and rerouted to the
// target once the move settles (Orleans `RerouteAllQueuedMessages`,
// forwarding to `ForwardingAddress`), not fail with "activation migrated" —
// see `ActivationData.invoke`'s hold-and-reroute branch and
// `DistributedDispatcher.lookupAndInvoke`'s single retry against a fresh
// directory lookup.
import { describe, expect, it } from "vitest";
import { grain } from "@thresh/core/decorators";
import { Grain } from "@thresh/core/grain";
import { defineGrainInterface } from "@thresh/core/grain-interface";
import type { GrainKey } from "@thresh/core/key-kinds";
import { SiloAddress } from "@thresh/core/silo-address";
import { InProcessNetwork, InProcessTransport } from "@thresh/messaging/in-process-transport";
import { ClusterNode } from "@thresh/runtime/cluster-node";
import { StaticMembershipService } from "@thresh/runtime/static-membership";

interface IWorker extends GrainKey<string> {
  ping(): Promise<string>;
}
const IWorker = defineGrainInterface<IWorker>("IWorker.migration-reroute");

/** A deferred promise `onDeactivate` awaits, so the test can hold the source
 * activation's `deactivate()` (called right after the target accepts the
 * migrated state) open for as long as it needs before letting it finish. */
function deferred(): { promise: Promise<void>; resolve: () => void } {
  let resolve!: () => void;
  const promise = new Promise<void>((r) => (resolve = r));
  return { promise, resolve };
}

let onDeactivateGate: Promise<void>;

@grain()
class SlowMigrateGrain extends Grain implements IWorker {
  async ping(): Promise<string> {
    return "ok";
  }

  override async onDeactivate(): Promise<void> {
    await onDeactivateGate;
  }
}

const silo = (n: number) => new SiloAddress(`silo-${n}`, `uid-${n}`, `silo-${n}:1111${n}`);

function buildCluster(): { nodes: ClusterNode[]; addresses: SiloAddress[] } {
  const network = new InProcessNetwork();
  const addresses = [silo(0), silo(1)];
  const nodes = addresses.map(
    (local) =>
      new ClusterNode({
        local,
        clusterId: "c1",
        membership: new StaticMembershipService(local, addresses),
        transport: new InProcessTransport(network, "c1"),
        random: () => 0, // place every grain on silo-0 (first ring candidate)
      }),
  );
  for (const node of nodes) node.registerGrain(SlowMigrateGrain, { interfaces: [IWorker] });
  return { nodes, addresses };
}

describe("a remote caller reaching an activation mid-migration is held and rerouted (GH #91)", () => {
  it("serves the remote call from the new host once the migration settles, instead of failing it", async () => {
    const gate = deferred();
    onDeactivateGate = gate.promise;
    const { nodes, addresses } = buildCluster();
    await Promise.all(nodes.map((n) => n.start()));
    try {
      // Activate on silo-0 and warm silo-1's cache/directory view of it, so
      // the later remote call resolves straight to silo-0 (the pre-migration
      // owner) rather than placing fresh.
      await nodes[0]!.getGrain(IWorker, "g").ping();
      await nodes[1]!.getGrain(IWorker, "g").ping();
      expect(nodes[0]!.activationCount()).toBe(1);
      expect(nodes[1]!.activationCount()).toBe(0);

      // Kick off the migration (dehydrates, hands off, and — once accepted —
      // calls `deactivate()`, whose `onDeactivate` is gated open) without
      // awaiting it yet.
      const migrating = nodes[0]!.migrateRandomActivations(addresses[1]!, 1);

      // A remote call from silo-1, issued while the migration is in flight,
      // must be held rather than rejected — regardless of whether it lands
      // before or after the hand-off completes.
      const remoteCall = nodes[1]!.getGrain(IWorker, "g").ping();
      let settled = false;
      void remoteCall.then(() => (settled = true));
      await new Promise((r) => setTimeout(r, 20));
      expect(settled).toBe(false); // held: the migration hasn't settled yet

      // Let the source's deactivate() (and thus the whole migration) finish.
      gate.resolve();
      const moved = await migrating;
      expect(moved).toBe(1);

      await expect(remoteCall).resolves.toBe("ok");
      expect(settled).toBe(true);
      expect(nodes[0]!.activationCount()).toBe(0);
      expect(nodes[1]!.activationCount()).toBe(1);
    } finally {
      await Promise.all(nodes.map((n) => n.stop()));
    }
  });
});
