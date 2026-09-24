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
const IWorker = defineGrainInterface<IWorker>("IWorker.migrate-fail");

@grain()
class WorkerGrain extends Grain implements IWorker {
  async ping(): Promise<string> {
    return "ok";
  }
}

const silo = (n: number) => new SiloAddress(`silo-${n}`, `uid-${n}`, `silo-${n}:1112${n}`);

/**
 * `silo-1` never registers `WorkerGrain`, so any migration attempt toward it
 * is rejected on arrival (`Catalog.activateMigrated` finds no registered
 * type) — the "target does not host the type" trigger from issue #93.
 */
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
        random: () => 0,
      }),
  );
  nodes[0]!.registerGrain(WorkerGrain, { interfaces: [IWorker] });
  return { nodes, addresses };
}

describe("a failed migration continues into deactivation (issue #93)", () => {
  it("does not leave the activation rejecting every call with 'activation migrated'", async () => {
    const { nodes, addresses } = buildCluster();
    await Promise.all(nodes.map((n) => n.start()));
    try {
      const worker = nodes[0]!.getGrain(IWorker, "w1");
      await worker.ping();
      expect(nodes[0]!.activationCount()).toBe(1);

      // silo-1 doesn't host WorkerGrain, so this migration is rejected.
      const moved = await nodes[0]!.migrateRandomActivations(addresses[1]!, 1);
      expect(moved).toBe(0);

      // The activation must have continued into deactivation rather than
      // being left live-but-dehydrated (which would reject every future
      // call with "activation migrated"). A fresh call must succeed and
      // must not throw that rejection.
      await expect(worker.ping()).resolves.toBe("ok");
    } finally {
      await Promise.all(nodes.map((n) => n.stop()));
    }
  });
});
