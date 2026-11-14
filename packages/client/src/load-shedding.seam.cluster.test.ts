import { describe, expect, it } from "vitest";
import { grain } from "@thresh/core/decorators";
import { GatewayTooBusyException } from "@thresh/core/errors";
import { Grain } from "@thresh/core/grain";
import { GrainId } from "@thresh/core/grain-id";
import { defineGrainInterface } from "@thresh/core/grain-interface";
import type { GrainKey } from "@thresh/core/key-kinds";
import { SiloAddress } from "@thresh/core/silo-address";
import { createClient } from "@thresh/client/client-node";
import { InProcessNetwork, InProcessTransport } from "@thresh/messaging/in-process-transport";
import { ClusterNode } from "@thresh/runtime/cluster-node";
import {
  NodeEnvironmentStatisticsProvider,
  type EnvironmentStatistics,
  type EnvironmentStatisticsProvider,
} from "@thresh/runtime/load-shedding";
import { StaticMembershipService } from "@thresh/runtime/static-membership";

interface IPing extends GrainKey<string> {
  ping(): Promise<string>;
  latch(value: number): Promise<void>;
}
const IPing = defineGrainInterface<IPing>("test.load-shedding-seam.IPing");

@grain({ placement: "activationCount" })
class PingGrain extends Grain implements IPing {
  async ping(): Promise<string> {
    return "pong";
  }
  async latch(value: number): Promise<void> {
    await this.runtime.latchCpuUsage(value);
  }
}

const GIB = 1024 ** 3;

class FixedStatistics implements EnvironmentStatisticsProvider {
  constructor(private cpuUsagePercentage: number) {}
  getEnvironmentStatistics(): EnvironmentStatistics {
    return {
      cpuUsagePercentage: this.cpuUsagePercentage,
      memoryUsageBytes: 1 * GIB,
      availableMemoryBytes: 3 * GIB,
      maximumAvailableMemoryBytes: 4 * GIB,
    };
  }
}

const CLUSTER = "c-seam";
const addr = (n: number) => new SiloAddress(`silo-${n}`, `uid-${n}`, `silo-${n}:1111${n}`);

function buildNodes(
  providers: Array<EnvironmentStatisticsProvider | undefined>,
  extra: { testHooks?: boolean } = {},
) {
  const network = new InProcessNetwork();
  const addresses = providers.map((_, i) => addr(i));
  const nodes = addresses.map((local, i) => {
    const provider = providers[i];
    const node = new ClusterNode({
      local,
      clusterId: CLUSTER,
      membership: new StaticMembershipService(local, addresses),
      transport: new InProcessTransport(network, CLUSTER),
      loadShedding: { loadSheddingEnabled: true, cpuThreshold: 90 },
      random: () => 0,
      ...(provider !== undefined ? { environmentStatistics: provider } : {}),
      ...(extra.testHooks === true ? { testHooks: true } : {}),
    });
    node.registerGrain(PingGrain, { interfaces: [IPing] });
    return node;
  });
  return { network, addresses, nodes };
}

describe("NodeEnvironmentStatisticsProvider", () => {
  it("reports a CPU reading within 0..100 and consistent memory figures", async () => {
    const provider = new NodeEnvironmentStatisticsProvider();
    const first = provider.getEnvironmentStatistics();
    const burnUntil = Date.now() + 20;
    while (Date.now() < burnUntil); // keep the event loop busy for a reading window
    const second = provider.getEnvironmentStatistics();
    for (const s of [first, second]) {
      expect(s.cpuUsagePercentage).toBeGreaterThanOrEqual(0);
      expect(s.cpuUsagePercentage).toBeLessThanOrEqual(100);
      expect(s.memoryUsageBytes).toBeGreaterThan(0);
      expect(s.maximumAvailableMemoryBytes).toBeGreaterThan(0);
      expect(s.availableMemoryBytes).toBeGreaterThan(0);
      expect(s.memoryUsageBytes).toBeLessThanOrEqual(s.maximumAvailableMemoryBytes);
      expect(s.availableMemoryBytes).toBeLessThanOrEqual(s.maximumAvailableMemoryBytes);
    }
  });
});

describe("environment statistics seam", () => {
  it("sheds gateway traffic and is excluded from peer placement with no test latch", async () => {
    const { network, nodes, addresses } = buildNodes([new FixedStatistics(99), new FixedStatistics(5)]);
    await Promise.all(nodes.map((n) => n.start()));
    const client = createClient({
      clusterId: CLUSTER,
      transport: new InProcessTransport(network, CLUSTER),
      gateway: addresses[0]!,
    }).registerGrain(PingGrain, { interfaces: [IPing] });
    try {
      await client.connect();
      await expect(client.getGrain(IPing, "shed").ping()).rejects.toThrow(GatewayTooBusyException);

      // Silo-1 learns silo-0's snapshot, then activation-count placement skips silo-0 even
      // though silo-0 has the lower count (random->0 would otherwise pick the first candidate).
      await nodes[0]!.publishLoadStats();
      await nodes[1]!.getGrain(IPing, "placed").ping();
      expect(nodes[1]!.isActive(new GrainId("Ping", "placed"))).toBe(true);
      expect(nodes[0]!.isActive(new GrainId("Ping", "placed"))).toBe(false);
    } finally {
      await client.close();
      await Promise.all(nodes.map((n) => n.stop()));
    }
  });

  it("carries the provider's readings in the load snapshot peers receive", async () => {
    const { nodes, addresses } = buildNodes([new FixedStatistics(42), undefined]);
    await Promise.all(nodes.map((n) => n.start()));
    try {
      await nodes[0]!.publishLoadStats();
      const stats = nodes[1]!.peerResourceStats(addresses[0]!);
      expect(stats?.cpuUsagePercentage).toBe(42);
      expect(stats?.maximumAvailableMemoryBytes).toBe(4 * GIB);
      expect(stats?.memoryUsageBytes).toBe(1 * GIB);
    } finally {
      await Promise.all(nodes.map((n) => n.stop()));
    }
  });
});

describe("load-shedding test hooks are test-only", () => {
  it("throws a clear error from every latch method on a silo built without test hooks", async () => {
    const { nodes } = buildNodes([new FixedStatistics(5)]);
    await nodes[0]!.start();
    try {
      const hooks = nodes[0]!.siloTestHooks();
      expect(() => hooks.enableOverloadDetection(false)).toThrow(/test hooks/i);
      expect(() => hooks.latchCpuUsage(99)).toThrow(/test hooks/i);
      expect(() => hooks.unlatchCpuUsage()).toThrow(/test hooks/i);
      expect(() => hooks.latchOverloaded()).toThrow(/test hooks/i);
      expect(() => hooks.unlatchOverloaded()).toThrow(/test hooks/i);
    } finally {
      await nodes[0]!.stop();
    }
  });

  it("refuses a grain's runtime.latchCpuUsage and leaves the silo's reading untouched", async () => {
    const { nodes } = buildNodes([new FixedStatistics(5)]);
    await nodes[0]!.start();
    try {
      await expect(nodes[0]!.getGrain(IPing, "g").latch(99)).rejects.toThrow(/test hooks/i);
      expect(nodes[0]!.isOverloaded()).toBe(false);
    } finally {
      await nodes[0]!.stop();
    }
  });

  it("still latches on a silo built with test hooks", async () => {
    const { nodes } = buildNodes([new FixedStatistics(5)], { testHooks: true });
    await nodes[0]!.start();
    try {
      await nodes[0]!.getGrain(IPing, "g").latch(99);
      expect(nodes[0]!.isOverloaded()).toBe(true);
      await nodes[0]!.siloTestHooks().unlatchCpuUsage();
      expect(nodes[0]!.isOverloaded()).toBe(false);
    } finally {
      await nodes[0]!.stop();
    }
  });
});
