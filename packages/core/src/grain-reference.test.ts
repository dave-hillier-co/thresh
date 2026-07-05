import { describe, expect, it } from "vitest";

import { GRAIN_REF, grainReferenceIdentity } from "./grain-reference";
import { GrainId } from "./grain-id";

describe("grainReferenceIdentity", () => {
  it("recognises a plain object carrying the GRAIN_REF marker", () => {
    const grainId = new GrainId("test", "key");
    const ref = { [GRAIN_REF]: { grainId, interfaceId: 3 } };

    expect(grainReferenceIdentity(ref)).toEqual({ grainId, interfaceId: 3 });
  });

  it("recognises a Proxy that only implements a `get` trap (no `has` trap)", () => {
    // Grain-reference proxies (packages/runtime/src/grain-factory.ts) are built
    // over an empty target with just a `get` trap. The `in` operator falls back
    // to the target's own `has`, which is always false for `{}`, so a lookup
    // that relies on `GRAIN_REF in value` never recognises a real grain ref.
    const grainId = new GrainId("test", "key");
    const identity = { grainId, interfaceId: 5 };
    const ref = new Proxy(
      {},
      {
        get: (_t, prop) => (prop === GRAIN_REF ? identity : undefined),
      },
    );

    expect(grainReferenceIdentity(ref)).toEqual(identity);
  });

  it("returns undefined for values without the marker", () => {
    expect(grainReferenceIdentity({})).toBeUndefined();
    expect(grainReferenceIdentity(null)).toBeUndefined();
    expect(grainReferenceIdentity(42)).toBeUndefined();
  });
});
