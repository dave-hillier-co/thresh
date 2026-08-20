import { describe, expect, it } from "vitest";
import { grain } from "@thresh/core/decorators";
import { defineGrain, type GrainDefinition } from "@thresh/core/define-grain";
import { Grain } from "@thresh/core/grain";
import { defineGrainInterface } from "@thresh/core/grain-interface";
import { grainReferenceIdentity } from "@thresh/core/grain-reference";
import { SiloAddress } from "@thresh/core/silo-address";
import { InProcessNetwork, InProcessTransport } from "@thresh/messaging/in-process-transport";
import { createClient, type ClientNode } from "@thresh/client/client-node";

const CLUSTER = "registration-cluster";

interface IGreeter {
  greet(name: string): Promise<string>;
}

const Greeter = defineGrain("ClientRegistrationGreeter", () => ({
  greet: async (name: string) => `hello ${name}`,
}));

const ISeparateGreeter = defineGrainInterface<IGreeter>("client.ISeparateGreeter");

@grain({ name: "ClientRegistrationClassGreeter" })
class ClassGreeter extends Grain implements IGreeter {
  async greet(name: string): Promise<string> {
    return `hello ${name}`;
  }
}

@grain({ name: "ClientRegistrationUnconstructable" })
class UnconstructableGrain extends Grain implements IGreeter {
  constructor() {
    super();
    throw new Error("the client constructed a grain");
  }
  async greet(name: string): Promise<string> {
    return `hello ${name}`;
  }
}

const Unconstructable: GrainDefinition<IGreeter> = Object.assign(
  defineGrainInterface<IGreeter>("client.IUnconstructable"),
  { grain: UnconstructableGrain },
);

const newClient = (): ClientNode =>
  createClient({
    clusterId: CLUSTER,
    local: new SiloAddress("client", "uid-c", "client:22222"),
    transport: new InProcessTransport(new InProcessNetwork(), CLUSTER),
    gateway: new SiloAddress("gateway", "uid-g", "gateway:11111"),
  });

/** The grain type the client resolved for `def`, read off the reference it hands back. */
const resolvedType = (client: ClientNode, def: Parameters<ClientNode["getGrain"]>[0]): string => {
  const identity = grainReferenceIdentity(client.getGrain(def, "k"));
  if (identity === undefined) throw new Error("not a grain reference");
  return identity.grainId.type;
};

describe("ClientNode.registerGrain", () => {
  it("registers a definition under its own interface", () => {
    const client = newClient().registerGrain(Greeter);
    expect(resolvedType(client, Greeter)).toBe("ClientRegistrationGreeter");
  });

  it("registers a definition under exactly the interfaces it is given", () => {
    const client = newClient().registerGrain(Greeter, { interfaces: [ISeparateGreeter] });
    expect(resolvedType(client, ISeparateGreeter)).toBe("ClientRegistrationGreeter");
    // The explicit list wins outright: the fused interface is not also registered.
    expect(() => client.getGrain(Greeter, "k")).toThrow(/no grain registered/);
  });

  it("still registers a plain constructor under an interfaces list", () => {
    const client = newClient().registerGrain(ClassGreeter, { interfaces: [ISeparateGreeter] });
    expect(resolvedType(client, ISeparateGreeter)).toBe("ClientRegistrationClassGreeter");
  });

  it("never constructs the grain — a client hosts no activations", () => {
    // A client reads only the metadata. Instantiating the implementation would
    // drag its storage/stream/job dependencies into every client process, so a
    // definition whose constructor is unusable must still register cleanly.
    const client = newClient().registerGrain(Unconstructable);
    expect(resolvedType(client, Unconstructable)).toBe("ClientRegistrationUnconstructable");
  });

  it("rejects a constructor with no interfaces", () => {
    // @ts-expect-error a bare constructor carries no interface
    expect(() => newClient().registerGrain(ClassGreeter)).toThrow(/\{ interfaces/);
  });
});

describe("ClientNode.registerGrains", () => {
  it("accepts bare definitions alongside constructor entries", () => {
    const client = newClient().registerGrains([
      Greeter,
      { ctor: ClassGreeter, interfaces: [ISeparateGreeter] },
    ]);
    expect(resolvedType(client, Greeter)).toBe("ClientRegistrationGreeter");
    expect(resolvedType(client, ISeparateGreeter)).toBe("ClientRegistrationClassGreeter");
  });
});
