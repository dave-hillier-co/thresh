import { createServer } from "node:net";
import { once } from "node:events";
import { describe, expect, it } from "vitest";
import { grain } from "@thresh/core/decorators";
import { Grain } from "@thresh/core/grain";
import { GrainId } from "@thresh/core/grain-id";
import { defineGrainInterface } from "@thresh/core/grain-interface";
import type { GrainKey } from "@thresh/core/key-kinds";
import { SiloAddress } from "@thresh/core/silo-address";
import { ConsistentHashRing } from "@thresh/directory/consistent-hash-ring";
import type { LocationCache } from "@thresh/directory/location-cache";
import { ClusterNode } from "@thresh/runtime/cluster-node";
import { WebSocketTransport } from "@thresh/messaging/web-socket-transport";
import { StaticMembershipService } from "@thresh/runtime/static-membership";

interface ICounter extends GrainKey<string> {
  increment(by: number): Promise<number>;
}
const ICounter = defineGrainInterface<ICounter>("ICounter.unreachablePeer");

@grain()
class CounterGrain extends Grain implements ICounter {
  private count = 0;
  async increment(by: number): Promise<number> {
    this.count += by;
    return this.count;
  }
}

const CLUSTER = "c1";

/** An endpoint on this machine that nothing is listening on (the port was bound, then released). */
async function deadSilo(): Promise<SiloAddress> {
  const probe = createServer().listen(0, "127.0.0.1");
  await once(probe, "listening");
  const info = probe.address();
  const port = typeof info === "object" && info !== null ? info.port : 0;
  await new Promise<void>((resolve) => probe.close(() => resolve()));
  return new SiloAddress("silo-dead", "uid-dead", `127.0.0.1:${port}`);
}

describe("a call whose route points at a peer that is not listening", () => {
  // The transport reports the refused dial as the typed `ConnectFailure` (cause `refused`), not a
  // raw ECONNREFUSED: only that lets the caller read it as "no process there", remove the entry
  // and place the grain afresh (docs/design-membership-and-directory.md). A raw error would fail
  // the call.
  it("removes the entry rather than failing on a raw ECONNREFUSED, and answers from a fresh activation", async () => {
    const local = new SiloAddress("silo-0", "uid-0", "127.0.0.1:0");
    const dead = await deadSilo();
    const membership = new StaticMembershipService(local, [local, dead]);
    membership.setStatus(dead, "joining");
    const node = new ClusterNode({
      local,
      clusterId: CLUSTER,
      membership,
      transport: new WebSocketTransport(CLUSTER),
    });
    node.registerGrain(CounterGrain, { interfaces: [ICounter] });
    await node.start();
    try {
      // The directory entry still points at the peer that has gone (a silo
      // Kubernetes lists as not ready, or has just removed).
      // The dead silo is still a member (listed, just not ready); the key is one
      // this silo's own partition owns, so the lookup is answered locally.
      const ring = new ConsistentHashRing([local, dead]);
      let key = "k-0";
      for (let i = 0; !ring.ownerOf(new GrainId("Counter", key)).equals(local); i++) key = `k-${i}`;
      const grainId = new GrainId("Counter", key);
      await node.partition.register(node.appliedViewVersion, {
        grainId,
        silo: dead,
        activationId: "act-dead",
      });

      const answer = await node
        .getGrain(ICounter, key)
        .increment(1)
        .catch((e: unknown) => e);

      expect(answer).toBe(1);
      const entry = await node.partition.lookup(node.appliedViewVersion, grainId);
      expect(entry.kind === "ok" && entry.value?.silo.equals(local)).toBe(true);
      const cache = (node as unknown as { cache: LocationCache }).cache;
      expect(cache.get(grainId)?.silo.equals(dead)).not.toBe(true);
    } finally {
      await node.stop();
    }
  });
});
