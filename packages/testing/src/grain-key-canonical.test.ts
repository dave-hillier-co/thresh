import { describe, expect, it } from "vitest";
import { grain } from "@thresh/core/decorators";
import { Grain } from "@thresh/core/grain";
import { GrainId } from "@thresh/core/grain-id";
import { defineGrainInterface } from "@thresh/core/grain-interface";
import { Guid } from "@thresh/core/guid";
import type { GrainKey } from "@thresh/core/key-kinds";
import { SiloAddress } from "@thresh/core/silo-address";
import { InProcessNetwork, InProcessTransport } from "@thresh/messaging/in-process-transport";
import { ClusterNode } from "@thresh/runtime/cluster-node";
import { StaticMembershipService } from "@thresh/runtime/static-membership";

interface ICanonicalInteger extends GrainKey<bigint> {
  keyAsSeen(): Promise<string>;
}
const ICanonicalInteger = defineGrainInterface<ICanonicalInteger>("ICanonicalInteger.identity", {
  key: "integer",
});

interface ICanonicalGuid extends GrainKey<Guid> {
  keyAsSeen(): Promise<string>;
}
const ICanonicalGuid = defineGrainInterface<ICanonicalGuid>("ICanonicalGuid.identity", {
  key: "guid",
});

interface IMarkerOnly extends GrainKey<bigint> {
  keyAsSeen(): Promise<string>;
}

let constructed = 0;

function describeKey(grainId: GrainId): string {
  const key = grainId.key;
  if (key instanceof Guid) return `guid:${key}`;
  return `${typeof key}:${String(key)}`;
}

@grain()
class CanonicalIntegerGrain extends Grain implements ICanonicalInteger {
  constructor() {
    super();
    constructed++;
  }
  async keyAsSeen(): Promise<string> {
    return describeKey(this.id);
  }
}

@grain()
class CanonicalGuidGrain extends Grain implements ICanonicalGuid {
  constructor() {
    super();
    constructed++;
  }
  async keyAsSeen(): Promise<string> {
    return describeKey(this.id);
  }
}

const local = new SiloAddress("silo-0", "uid-0", "silo-0:11111");

async function withNode(body: (node: ClusterNode) => Promise<void>): Promise<void> {
  const node = new ClusterNode({
    local,
    clusterId: "c1",
    membership: new StaticMembershipService(local, [local]),
    transport: new InProcessTransport(new InProcessNetwork(), "c1"),
  });
  node.registerGrain(CanonicalIntegerGrain, { interfaces: [ICanonicalInteger] });
  node.registerGrain(CanonicalGuidGrain, { interfaces: [ICanonicalGuid] });
  await node.start();
  try {
    constructed = 0;
    await body(node);
  } finally {
    await node.stop();
  }
}

describe("a string-built id with non-canonical key text names the grain its typed id names", () => {
  it.each(["005", "+5", " 5 ", "0x5"])(
    "activates an integer grain once for the text %j and for 5n",
    async (text) => {
      await withNode(async (node) => {
        const viaText = node.getExtensionReference(
          ICanonicalInteger,
          new GrainId("CanonicalInteger", text),
        );
        expect(await viaText.keyAsSeen()).toBe("bigint:5");
        const viaTyped = node.getGrain(ICanonicalInteger, 5n);
        expect(await viaTyped.keyAsSeen()).toBe("bigint:5");
        expect(constructed).toBe(1);
      });
    },
  );

  it("activates a guid grain once for an upper-case guid string and for the Guid", async () => {
    await withNode(async (node) => {
      const guid = Guid.newGuid();
      const viaText = node.getExtensionReference(
        ICanonicalGuid,
        new GrainId("CanonicalGuid", guid.toString().toUpperCase()),
      );
      expect(await viaText.keyAsSeen()).toBe(`guid:${guid}`);
      expect(await node.getGrain(ICanonicalGuid, guid).keyAsSeen()).toBe(`guid:${guid}`);
      expect(constructed).toBe(1);
    });
  });

  it("agrees on the activation whichever of the typed and text-built ids arrives first", async () => {
    await withNode(async (node) => {
      await node.getGrain(ICanonicalInteger, 5n).keyAsSeen();
      await node
        .getExtensionReference(ICanonicalInteger, new GrainId("CanonicalInteger", "005"))
        .keyAsSeen();
      expect(constructed).toBe(1);
    });
  });
});

describe("an interface whose key kind is only a type marker", () => {
  it("requires the kind to be stated as a value", () => {
    // @ts-expect-error a bigint marker alone is erased at runtime: the kind must be stated
    const unstated = defineGrainInterface<IMarkerOnly>("IMarkerOnly.identity.unstated");
    expect(unstated.key).toBeUndefined();
    const stated = defineGrainInterface<IMarkerOnly>("IMarkerOnly.identity.stated", {
      key: "integer",
    });
    expect(stated.key).toBe("integer");
  });
});
