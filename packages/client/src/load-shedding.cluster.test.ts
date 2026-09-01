import { describe, expect, it } from "vitest";
import { grain } from "@thresh/core/decorators";
import { GatewayTooBusyException } from "@thresh/core/errors";
import { Grain } from "@thresh/core/grain";
import { defineGrainInterface } from "@thresh/core/grain-interface";
import type { GrainKey } from "@thresh/core/key-kinds";
import { SiloAddress } from "@thresh/core/silo-address";
import { InProcessNetwork, InProcessTransport } from "@thresh/messaging/in-process-transport";
import { ClusterNode } from "@thresh/runtime/cluster-node";
import { StaticMembershipService } from "@thresh/runtime/static-membership";
import { createClient } from "@thresh/client/client-node";

interface IPing extends GrainKey<string> {
  ping(): Promise<string>;
  /** Exercises `GrainRuntime`'s load-shedding test hooks from inside a turn (Orleans `IPlacementTestGrain`). */
  latchCpuUsageViaRuntime(value: number): Promise<void>;
  enableOverloadDetectionViaRuntime(enabled: boolean): Promise<void>;
}
const IPing = defineGrainInterface<IPing>("test.load-shedding.IPing");

@grain()
class PingGrain extends Grain implements IPing {
  async ping(): Promise<string> {
    return "pong";
  }

  async latchCpuUsageViaRuntime(value: number): Promise<void> {
    this.runtime.latchCpuUsage(value);
  }

  async enableOverloadDetectionViaRuntime(enabled: boolean): Promise<void> {
    this.runtime.enableOverloadDetection(enabled);
  }
}

const CLUSTER = "c1";

function buildSilo(
  local: SiloAddress,
  network: InProcessNetwork,
  membershipAddrs: SiloAddress[],
  loadShedding?: { loadSheddingEnabled?: boolean; cpuThreshold?: number },
) {
  const silo = new ClusterNode({
    local,
    clusterId: CLUSTER,
    membership: new StaticMembershipService(local, membershipAddrs),
    transport: new InProcessTransport(network, CLUSTER),
    ...(loadShedding !== undefined ? { loadShedding } : {}),
  });
  silo.registerGrain(PingGrain, { interfaces: [IPing] });
  return silo;
}

describe("gateway load shedding (Orleans OverloadDetector / LoadSheddingOptions)", () => {
  it("rejects a client call with GatewayTooBusyException while the gateway's latched CPU usage is above the threshold", async () => {
    const network = new InProcessNetwork();
    const siloAddr = new SiloAddress("silo-1", "uid-1", "silo-1:11111");
    const silo = buildSilo(siloAddr, network, [siloAddr], {
      loadSheddingEnabled: true,
      cpuThreshold: 98,
    });
    await silo.start();

    const client = createClient({
      clusterId: CLUSTER,
      transport: new InProcessTransport(network, CLUSTER),
      gateway: siloAddr,
    }).registerGrain(PingGrain, { interfaces: [IPing] });
    await client.connect();

    try {
      const ref = client.getGrain(IPing, "p1");

      // Below the threshold: calls go through as normal.
      await expect(ref.ping()).resolves.toBe("pong");

      // Latch above the threshold: the gateway sheds the next request.
      silo.siloTestHooks().latchCpuUsage(99);
      await expect(ref.ping()).rejects.toThrow(GatewayTooBusyException);

      // Unlatch: back to normal.
      silo.siloTestHooks().unlatchCpuUsage();
      await expect(ref.ping()).resolves.toBe("pong");
    } finally {
      await client.close();
      await silo.stop();
    }
  });

  it("never sheds when load shedding is not enabled, regardless of latched CPU usage", async () => {
    const network = new InProcessNetwork();
    const siloAddr = new SiloAddress("silo-1", "uid-1", "silo-1:11111");
    const silo = buildSilo(siloAddr, network, [siloAddr]); // loadShedding left at defaults (disabled)
    await silo.start();

    const client = createClient({
      clusterId: CLUSTER,
      transport: new InProcessTransport(network, CLUSTER),
      gateway: siloAddr,
    }).registerGrain(PingGrain, { interfaces: [IPing] });
    await client.connect();

    try {
      silo.siloTestHooks().latchCpuUsage(100);
      await expect(client.getGrain(IPing, "p2").ping()).resolves.toBe("pong");
    } finally {
      await client.close();
      await silo.stop();
    }
  });

  it("never sheds ordinary inter-silo traffic, only requests arriving directly from a client", async () => {
    const network = new InProcessNetwork();
    const silo1Addr = new SiloAddress("silo-1", "uid-1", "silo-1:11111");
    const silo2Addr = new SiloAddress("silo-2", "uid-2", "silo-2:11112");
    const addrs = [silo1Addr, silo2Addr];
    const loadShedding = { loadSheddingEnabled: true, cpuThreshold: 98 };
    const silo1 = buildSilo(silo1Addr, network, addrs, loadShedding);
    const silo2 = buildSilo(silo2Addr, network, addrs, loadShedding);
    await silo1.start();
    await silo2.start();

    try {
      silo1.siloTestHooks().latchCpuUsage(99);
      // A direct silo-to-silo call (this test's own process acting as silo2)
      // is not a client-originated gateway request, so it must not be shed.
      await expect(silo2.getGrain(IPing, "p3").ping()).resolves.toBe("pong");
    } finally {
      await silo1.stop();
      await silo2.stop();
    }
  });

  it("latches CPU usage and toggles overload detection via GrainRuntime (Orleans IPlacementTestGrain test hooks)", async () => {
    const network = new InProcessNetwork();
    const siloAddr = new SiloAddress("silo-1", "uid-1", "silo-1:11111");
    const silo = buildSilo(siloAddr, network, [siloAddr], {
      loadSheddingEnabled: true,
      cpuThreshold: 98,
    });
    await silo.start();

    const client = createClient({
      clusterId: CLUSTER,
      transport: new InProcessTransport(network, CLUSTER),
      gateway: siloAddr,
    }).registerGrain(PingGrain, { interfaces: [IPing] });
    await client.connect();

    try {
      const ref = client.getGrain(IPing, "p4");
      // Disable detection, then latch CPU high through the grain's own
      // `GrainRuntime` hooks — both calls must land while the silo is still
      // reachable (detection starts enabled but CPU starts at 0, so neither
      // call is itself shed).
      await ref.enableOverloadDetectionViaRuntime(false);
      await ref.latchCpuUsageViaRuntime(99);
      await expect(ref.ping()).resolves.toBe("pong");

      // Re-enabling directly through the silo's test hooks (not a grain call,
      // which would now be shed by the very state it's trying to change)
      // makes the latched high CPU bite again.
      silo.siloTestHooks().enableOverloadDetection(true);
      await expect(ref.ping()).rejects.toThrow(GatewayTooBusyException);
    } finally {
      await client.close();
      await silo.stop();
    }
  });
});
