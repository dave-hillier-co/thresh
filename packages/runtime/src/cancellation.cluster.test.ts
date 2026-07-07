import { describe, expect, it } from "vitest";
import { grain } from "@tsva/core/decorators";
import { GrainTaskCanceledError } from "@tsva/core/errors";
import { Grain } from "@tsva/core/grain";
import type { GrainCancellationToken } from "@tsva/core/grain-cancellation-token";
import { GrainCancellationTokenSource } from "@tsva/core/grain-cancellation-token";
import type { GrainId } from "@tsva/core/grain-id";
import { defineGrainInterface } from "@tsva/core/grain-interface";
import { castGrainReference } from "@tsva/core/grain-reference";
import type { GrainWithStringKey } from "@tsva/core/key-kinds";
import type { MembershipService } from "@tsva/core/membership";
import { SiloAddress } from "@tsva/core/silo-address";
import { InProcessNetwork, InProcessTransport } from "@tsva/messaging/in-process-transport";
import { ICancellationSourcesExtension } from "@tsva/runtime/cancellation-extension";
import { ClusterNode } from "@tsva/runtime/cluster-node";
import { StaticMembershipService } from "@tsva/runtime/static-membership";

/** A grain whose `longWait` honours a `GrainCancellationToken` (Orleans cooperative cancellation). */
interface ILongWaitGrain extends GrainWithStringKey {
  longWait(token: GrainCancellationToken, delayMs: number, callId: string): Promise<void>;
  wasCancelled(callId: string): Promise<boolean>;
}
const ILongWaitGrain = defineGrainInterface<ILongWaitGrain>("ILongWaitGrain.cancellation");

@grain()
class LongWaitGrain extends Grain implements ILongWaitGrain {
  private readonly cancelledCallIds = new Set<string>();

  async longWait(token: GrainCancellationToken, delayMs: number, callId: string): Promise<void> {
    await new Promise<void>((resolve, reject) => {
      if (token.isCancellationRequested) {
        this.cancelledCallIds.add(callId);
        reject(new GrainTaskCanceledError());
        return;
      }
      const timer = setTimeout(resolve, delayMs);
      token.signal.addEventListener(
        "abort",
        () => {
          clearTimeout(timer);
          this.cancelledCallIds.add(callId);
          reject(new GrainTaskCanceledError());
        },
        { once: true },
      );
    });
  }

  async wasCancelled(callId: string): Promise<boolean> {
    return this.cancelledCallIds.has(callId);
  }
}

const CLUSTER = "cancellation-cluster";
const silo = (n: number) => new SiloAddress(`silo-${n}`, `uid-${n}`, `silo-${n}:11111`);

/** Reports each node's own localSilo() while sharing one membership view (mirrors cluster.test.ts). */
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

/** Two silos, deterministic placement (random -> 0 always picks silo-0 as host). */
function buildCluster() {
  const network = new InProcessNetwork();
  const addresses = [silo(0), silo(1)];
  const membership = new StaticMembershipService(addresses[0]!, addresses);
  const makeNode = (local: SiloAddress) => {
    const node = new ClusterNode({
      local,
      clusterId: CLUSTER,
      membership: new MembershipView(membership, local),
      transport: new InProcessTransport(network, CLUSTER),
      random: () => 0,
    });
    node.registerGrain(LongWaitGrain, { interfaces: [ILongWaitGrain] });
    return node;
  };
  const nodes = addresses.map(makeNode);
  return {
    nodes,
    start: async () => {
      for (const n of nodes) await n.start();
    },
    stop: async () => {
      for (const n of nodes) await n.stop();
    },
  };
}

/**
 * Build a `GrainCancellationTokenSource` whose `canceller` reaches the
 * target's `ICancellationSourcesExtension` — the cross-silo half of
 * cancellation propagation — by rebuilding an ordinary reference to the same
 * grain id (`getGrain` with the target's own key) from `caller`, then casting
 * it to the extension interface (`castGrainReference`, the same mechanism
 * `GrainFactory.castTo` implements for `AsReference<T>`/`Cast`).
 */
function newSource(caller: ClusterNode): GrainCancellationTokenSource {
  return new GrainCancellationTokenSource(async (target: GrainId, tokenId: string) => {
    const ref = caller.getGrain(ILongWaitGrain, target.key as string);
    const ext = castGrainReference(ref, ICancellationSourcesExtension);
    await ext.cancelRemoteToken(tokenId);
  });
}

describe("cooperative grain cancellation", () => {
  it("completes normally when the token is never cancelled (cross-silo)", async () => {
    const cluster = buildCluster();
    await cluster.start();
    try {
      // random -> 0 places "LongWait/g1" on silo-0; call from silo-1.
      const source = newSource(cluster.nodes[1]!);
      await cluster.nodes[1]!.getGrain(ILongWaitGrain, "g1").longWait(source.token, 20, "call-1");
      expect(await cluster.nodes[1]!.getGrain(ILongWaitGrain, "g1").wasCancelled("call-1")).toBe(
        false,
      );
    } finally {
      await cluster.stop();
    }
  });

  it("rejects when cancelled mid-flight, propagated cross-silo via the cancellation extension", async () => {
    const cluster = buildCluster();
    await cluster.start();
    try {
      const source = newSource(cluster.nodes[1]!);
      const call = cluster.nodes[1]!
        .getGrain(ILongWaitGrain, "g2")
        .longWait(source.token, 10_000, "call-2");
      // Let the call actually reach and register on the hosting activation
      // before cancelling, so this exercises "cancel after the call arrived"
      // rather than the pre-cancelled/racing case covered below.
      await new Promise((resolve) => setTimeout(resolve, 20));
      await source.cancel();
      // The reject crosses the wire as a generic error (only `RejectionError`
      // preserves its subtype across silos — see `ClusterNode.serializeError`),
      // so the caller sees `GrainCallError` here, not `GrainTaskCanceledError`
      // itself; the grain-side state (`wasCancelled`) is what proves the
      // *callee* actually observed `GrainTaskCanceledError`.
      await expect(call).rejects.toThrow(/cancelled/i);
      expect(await cluster.nodes[1]!.getGrain(ILongWaitGrain, "g2").wasCancelled("call-2")).toBe(
        true,
      );
    } finally {
      await cluster.stop();
    }
  });

  it("rejects promptly when the token was already cancelled before the call was sent (cross-silo)", async () => {
    const cluster = buildCluster();
    await cluster.start();
    try {
      const source = newSource(cluster.nodes[1]!);
      // Cancel before the call is even made: the wire-arrived placeholder
      // carries `cancelled: true`, so the activation pre-aborts the bound
      // controller before the grain method ever runs.
      await source.cancel();
      const call = cluster.nodes[1]!
        .getGrain(ILongWaitGrain, "g3")
        .longWait(source.token, 10_000, "call-3");
      await expect(call).rejects.toThrow(/cancelled/i);
      expect(await cluster.nodes[1]!.getGrain(ILongWaitGrain, "g3").wasCancelled("call-3")).toBe(
        true,
      );
    } finally {
      await cluster.stop();
    }
  });

  it("propagates the exact GrainTaskCanceledError when cancelled in-process (same silo, no wire)", async () => {
    const cluster = buildCluster();
    await cluster.start();
    try {
      // Calling from silo-0 (the grain's own host) never crosses the wire —
      // the dispatcher delivers locally — so the thrown error's identity
      // survives, unlike the cross-silo cases above.
      const source = newSource(cluster.nodes[0]!);
      const call = cluster.nodes[0]!
        .getGrain(ILongWaitGrain, "g4")
        .longWait(source.token, 10_000, "call-4");
      await new Promise((resolve) => setTimeout(resolve, 20));
      await source.cancel();
      await expect(call).rejects.toBeInstanceOf(GrainTaskCanceledError);
      expect(await cluster.nodes[0]!.getGrain(ILongWaitGrain, "g4").wasCancelled("call-4")).toBe(
        true,
      );
    } finally {
      await cluster.stop();
    }
  });
});
