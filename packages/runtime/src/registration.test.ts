import { describe, expect, it } from "vitest";
import { grain } from "@thresh/core/decorators";
import { defineGrain } from "@thresh/core/define-grain";
import { Grain } from "@thresh/core/grain";
import { defineGrainInterface } from "@thresh/core/grain-interface";
import { InProcessNetwork, InProcessTransport } from "@thresh/messaging/in-process-transport";
import { SiloAddress } from "@thresh/core/silo-address";
import { ClusterNode } from "@thresh/runtime/cluster-node";
import { Silo } from "@thresh/runtime/silo";
import { StaticMembershipService } from "@thresh/runtime/static-membership";
import { FakeTimeProvider } from "@thresh/runtime/test-support/fake-time-provider";

interface IGreeter {
  greet(name: string): Promise<string>;
}

const Greeter = defineGrain<IGreeter>("RuntimeRegistrationGreeter", () => ({
  greet: async (name: string) => `hello ${name}`,
}));

/** A separately declared contract, the form the migrated call sites use. */
const IPinnedGreeter = defineGrainInterface<IGreeter>("runtime-registration.IPinnedGreeter");

@grain()
class ClassGreeter extends Grain implements IGreeter {
  async greet(name: string): Promise<string> {
    return `class ${name}`;
  }
}

const local = new SiloAddress("silo-0", "uid-0", "silo-0:11111");

function buildNode(): ClusterNode {
  return new ClusterNode({
    local,
    clusterId: "c1",
    membership: new StaticMembershipService(local, [local]),
    transport: new InProcessTransport(new InProcessNetwork(), "c1"),
  });
}

describe("Silo.registerGrain overloads", () => {
  it("registers a bare definition under its own interface", async () => {
    const silo = new Silo({ time: new FakeTimeProvider() });
    silo.registerGrain(Greeter);

    expect(await silo.getGrain(Greeter, "a").greet("world")).toBe("hello world");
  });

  it("registers a definition under exactly the interfaces it is given", async () => {
    const silo = new Silo({ time: new FakeTimeProvider() });
    silo.registerGrain(Greeter, { interfaces: [IPinnedGreeter] });

    expect(await silo.getGrain(IPinnedGreeter, "a").greet("world")).toBe("hello world");
    // The fused interface is NOT also registered: an explicit list wins outright,
    // so a migrated `{ interfaces: [ISeparate] }` site keeps registering one interface.
    expect(() => silo.getGrain(Greeter, "a")).toThrow(/no grain registered for interface/);
  });

  it("still registers a plain constructor under the interfaces it is given", async () => {
    const silo = new Silo({ time: new FakeTimeProvider() });
    silo.registerGrain(ClassGreeter, { interfaces: [IPinnedGreeter] });

    expect(await silo.getGrain(IPinnedGreeter, "a").greet("world")).toBe("class world");
  });

  it("rejects a plain constructor with no interfaces", () => {
    const silo = new Silo({ time: new FakeTimeProvider() });
    const register = silo.registerGrain.bind(silo) as (ctor: unknown) => unknown;

    expect(() => register(ClassGreeter)).toThrow(/interfaces/);
  });
});

describe("ClusterNode.registerGrain overloads", () => {
  it("registers a bare definition under its own interface", async () => {
    const node = buildNode();
    node.registerGrain(Greeter);
    await node.start();
    try {
      expect(await node.getGrain(Greeter, "a").greet("world")).toBe("hello world");
    } finally {
      await node.stop();
    }
  });

  it("drives the version manifest off the resolved interfaces list", () => {
    const node = buildNode();
    const IVersioned = defineGrainInterface<IGreeter>("runtime-registration.IVersioned", {
      version: 3,
    });
    node.registerGrain(Greeter, { interfaces: [IVersioned] });

    expect(node.localImplementedVersion(IVersioned.id)).toBe(3);
    expect(node.localImplementedVersion(Greeter.id)).toBeUndefined();
  });

  it("registers a bare definition's own version", () => {
    const node = buildNode();
    const Versioned = defineGrain<IGreeter>(
      "RuntimeRegistrationVersioned",
      () => ({ greet: async (name: string) => name }),
      { version: 4 },
    );
    node.registerGrain(Versioned);

    expect(node.localImplementedVersion(Versioned.id)).toBe(4);
  });
});
