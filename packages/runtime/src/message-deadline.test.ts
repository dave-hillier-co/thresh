import { describe, expect, it } from "vitest";
import { grain } from "@thresh/core/decorators";
import { isCancellationError } from "@thresh/core/errors";
import { Grain } from "@thresh/core/grain";
import { defineGrainInterface } from "@thresh/core/grain-interface";
import type { GrainKey } from "@thresh/core/key-kinds";
import type { MembershipService } from "@thresh/core/membership";
import { SiloAddress } from "@thresh/core/silo-address";
import { InProcessNetwork, InProcessTransport } from "@thresh/messaging/in-process-transport";
import { ClusterNode } from "@thresh/runtime/cluster-node";
import { invocationContext } from "@thresh/runtime/invocation-context";
import { StaticMembershipService } from "@thresh/runtime/static-membership";

interface ICounter extends GrainKey<string> {
  increment(): Promise<number>;
}
const ICounter = defineGrainInterface<ICounter>("ICounter.messageDeadline");

let count = 0;

@grain()
class CounterGrain extends Grain implements ICounter {
  async increment(): Promise<number> {
    count += 1;
    return count;
  }
}

const CLUSTER = "c1";
const silo = (n: number) => new SiloAddress(`silo-${n}`, `uid-${n}`, `silo-${n}:11111`);

class MembershipView implements MembershipService {
  constructor(
    private readonly shared: StaticMembershipService,
    private readonly local: SiloAddress,
  ) {}
  current() {
    return this.shared.current();
  }
  updates() {
    return this.shared.updates();
  }
  localSilo() {
    return this.local;
  }
}

// Issue #90: `InvocationRequest.deadline` never rode `ClusterNode.sendRemote`'s
// wire `Message` -- it stopped at the `RemoteInvoker` boundary -- so a remote
// turn had no deadline signal at all, and a call the caller had already given
// up on still ran to completion on the callee. Orleans carries this as
// `Message.TimeToLive` and drops an expired request before it runs
// (`ActivationData.ReceiveMessage`).
describe("call deadlines cross a silo forward (issue #90)", () => {
  it("drops an already-expired call on the remote silo instead of running it", async () => {
    count = 0;
    const network = new InProcessNetwork();
    const membership = new StaticMembershipService(silo(0), [silo(0), silo(1)]);
    const makeNode = (local: SiloAddress) => {
      const node = new ClusterNode({
        local,
        clusterId: CLUSTER,
        membership: new MembershipView(membership, local),
        transport: new InProcessTransport(network, CLUSTER),
        random: () => 0.99, // picks silo-1 (the remote candidate) for a fresh placement
      });
      node.registerGrain(CounterGrain, { interfaces: [ICounter] });
      return node;
    };
    const node0 = makeNode(silo(0));
    const node1 = makeNode(silo(1));
    await node0.start();
    await node1.start();

    try {
      const grainRef = node0.getGrain(ICounter, "g1");
      const err = await invocationContext
        .run(
          {
            senderId: undefined,
            ownerId: undefined,
            reentrancyId: "r1",
            deadline: Date.now() - 1_000, // already expired before it even leaves silo-0
          },
          () => grainRef.increment(),
        )
        .catch((e: unknown) => e);

      expect(isCancellationError(err)).toBe(true);
      expect(count).toBe(0); // the remote turn on silo-1 never ran
    } finally {
      await node0.stop();
      await node1.stop();
    }
  });
});
