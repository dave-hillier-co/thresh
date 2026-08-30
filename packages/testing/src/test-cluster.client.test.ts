import { describe, expect, it } from "vitest";
import { grain } from "@thresh/core/decorators";
import { Grain } from "@thresh/core/grain";
import { defineGrainInterface } from "@thresh/core/grain-interface";
import type { GrainWithStringKey } from "@thresh/core/key-kinds";
import type { SiloAddress } from "@thresh/core/silo-address";
import { InProcessNetwork } from "@thresh/messaging/in-process-transport";
import { TestCluster } from "@thresh/testing/test-cluster";

interface IEcho extends GrainWithStringKey {
  echo(text: string): Promise<string>;
}
const IEcho = defineGrainInterface<IEcho>("TestClusterClientEcho");

@grain({ name: "TestClusterClientEcho" })
class EchoGrain extends Grain implements IEcho {
  async echo(text: string): Promise<string> {
    return `echo:${text}`;
  }
}

const REGISTRATIONS = [{ ctor: EchoGrain, interfaces: [IEcho] }];

/**
 * Records every listener registration and removal, so a test can assert both
 * that no client endpoint appears until `cluster.client` is touched and the
 * ORDER in which endpoints go away at teardown.
 */
class RecordingNetwork extends InProcessNetwork {
  readonly registered: string[] = [];
  readonly unregistered: string[] = [];

  override register(endpoint: { address: SiloAddress; clusterId: string; onMessage: never }): void {
    this.registered.push(endpoint.address.endpoint);
    super.register(endpoint as never);
  }

  override unregister(address: SiloAddress): void {
    this.unregistered.push(address.endpoint);
    super.unregister(address);
  }
}

describe("TestCluster.client (Orleans TestCluster.Client / GrainFactory)", () => {
  it("resolves grain references that reach the cluster", async () => {
    const cluster = await TestCluster.start({ initialSilos: 2, grains: REGISTRATIONS });
    try {
      const client = await cluster.client;
      expect(await client.getGrain(IEcho, "a").echo("hi")).toBe("echo:hi");
    } finally {
      await cluster.dispose();
    }
  });

  it("issues calls from OUTSIDE every silo, so a silo's outgoing call filter does not wrap them", async () => {
    const throughSilo: string[] = [];
    const cluster = await TestCluster.start({
      initialSilos: 1,
      grains: REGISTRATIONS,
      configureSilo: (builder) => {
        builder.addOutgoingCallFilter(async (ctx) => {
          throughSilo.push(ctx.methodName);
          await ctx.invoke();
        });
      },
    });
    try {
      // The silo's own factory: the call is issued BY the silo, so the silo's
      // outgoing filter wraps it (Orleans: a grain-to-grain call).
      await cluster.getGrain(IEcho, "via-silo").echo("one");
      expect(throughSilo).toEqual(["echo"]);

      // The cluster client: a separate caller with its own (empty) outgoing
      // pipeline, exactly as Orleans' `TestCluster.GrainFactory` is.
      const client = await cluster.client;
      expect(await client.getGrain(IEcho, "via-client").echo("two")).toBe("echo:two");
      expect(throughSilo).toEqual(["echo"]);
    } finally {
      await cluster.dispose();
    }
  });

  it("is created lazily, so a cluster that never touches it puts nothing extra on the network", async () => {
    const network = new RecordingNetwork();
    const cluster = await TestCluster.start({ initialSilos: 2, grains: REGISTRATIONS, network });
    try {
      await cluster.getGrain(IEcho, "a").echo("hi");
      expect(network.registered).toEqual(["test-silo-0:11111", "test-silo-1:11111"]);

      await cluster.client;
      expect(network.registered).toHaveLength(3);
    } finally {
      await cluster.dispose();
    }
  });

  it("is the same client on every access", async () => {
    const cluster = await TestCluster.start({ initialSilos: 1, grains: REGISTRATIONS });
    try {
      const first = await cluster.client;
      expect(first).toBeDefined();
      expect(await cluster.client).toBe(first);
    } finally {
      await cluster.dispose();
    }
  });

  it("fails over when the silo it first connected through is killed", async () => {
    const cluster = await TestCluster.start({ initialSilos: 2, grains: REGISTRATIONS });
    try {
      const client = await cluster.client;
      expect(await client.getGrain(IEcho, "a").echo("one")).toBe("echo:one");

      // Every live silo is a gateway (Orleans' test client reads the same
      // membership the silos do), so losing the one it picked is recoverable.
      await cluster.killSilo(cluster.primary);

      expect(await client.getGrain(IEcho, "b").echo("two")).toBe("echo:two");
    } finally {
      await cluster.dispose();
    }
  });

  it("is closed by dispose(), BEFORE the silos it is connected to", async () => {
    const network = new RecordingNetwork();
    const cluster = await TestCluster.start({ initialSilos: 2, grains: REGISTRATIONS, network });
    const client = await cluster.client;
    const clientEndpoint = network.registered[2];
    expect(clientEndpoint).toBeDefined();

    await cluster.dispose();

    expect(network.unregistered[0]).toBe(clientEndpoint);
    expect(network.unregistered).toEqual([
      clientEndpoint,
      "test-silo-0:11111",
      "test-silo-1:11111",
    ]);
    await expect(client.getGrain(IEcho, "a").echo("hi")).rejects.toThrow();
  });

  it("dispose() is unaffected when the client was never used", async () => {
    const cluster = await TestCluster.start({ initialSilos: 1, grains: REGISTRATIONS });
    await expect(cluster.dispose()).resolves.toBeUndefined();
  });
});
