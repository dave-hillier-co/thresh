import { describe, expect, expectTypeOf, it } from "vitest";
import {
  defineGrainInterface,
  getGrainInterface,
  registerInterface,
  type GrainInterface,
} from "@thresh/core/grain-interface";
import { stableHash32 } from "@thresh/core/hash";
import type { GrainWithIntegerKey, KeyTypeOf } from "@thresh/core/key-kinds";

interface ICounter {
  increment(by: number): Promise<number>;
  decrement(by: number): Promise<number>;
  get(): Promise<number>;
}

describe("defineGrainInterface", () => {
  const ICounter = defineGrainInterface<ICounter>("ICounter", {
    options: { get: { readOnly: true } },
  });

  it("derives a stable interface id from the name", () => {
    const again = defineGrainInterface<ICounter>("ICounter");
    expect(ICounter.id).toBe(again.id);
  });

  it("gives different names different ids", () => {
    const other = defineGrainInterface<ICounter>("IOther");
    expect(ICounter.id).not.toBe(other.id);
  });

  it("carries per-method invocation options (no method table to declare)", () => {
    expect(ICounter.options.get?.readOnly).toBe(true);
    expect(ICounter.options.increment).toBeUndefined();
  });

  it("defaults to version 1 and carries an explicit version", () => {
    expect(defineGrainInterface("IVersioned.default").version).toBe(1);
    expect(defineGrainInterface("IVersioned.explicit", { version: 3 }).version).toBe(3);
  });

  it("keeps the id stable across versions of one interface (id is name-derived)", () => {
    const v1 = defineGrainInterface("IVersioned.same", { version: 1 });
    const v2 = defineGrainInterface("IVersioned.same", { version: 2 });
    expect(v1.id).toBe(v2.id);
    expect(v1.version).not.toBe(v2.version);
  });
});

/**
 * What a *second* definition of the same name does. Two definitions of one name
 * is not hypothetical: an interface module and an implementation module can each
 * declare the same contract (and a fused `defineGrain` declares one under the
 * grain type name), and the registry is what a receiving silo reads to recover
 * `extension` and per-method `options` for an inbound call it has no static type
 * for (`activation.ts` dispatches an extension call by `iface?.extension === true`).
 *
 * These started as characterization tests pinning the old clobber behaviour — the
 * last definition won outright and anything the first declared and the second
 * omitted was silently dropped. They now pin the deliberate replacement:
 * `defineGrainInterface` registers with `{ merge: true }`, so the receiving-side
 * fields (`options`, `extension`, `key`) are folded together and the later
 * definition's returned object *is* the merged view, not a partial one.
 *
 * `version` is deliberately excluded from the merge — see the version test.
 */
describe("defineGrainInterface redefinition (registry collision)", () => {
  interface IExtended {
    interleaved(): Promise<void>;
    plain(): Promise<void>;
  }

  it("merges into, rather than replaces, the registry entry for the id", () => {
    const first = defineGrainInterface<IExtended>("ICollide.replace", {
      extension: true,
      options: { interleaved: { alwaysInterleave: true } },
    });
    const second = defineGrainInterface<IExtended>("ICollide.replace");

    expect(second.id).toBe(first.id);
    expect(getGrainInterface(first.id)).toBe(second);
    expect(second.extension).toBe(true);
    expect(second.options.interleaved?.alwaysInterleave).toBe(true);
  });

  it("keeps per-method options declared only by the first definition", () => {
    const first = defineGrainInterface<IExtended>("ICollide.options", {
      options: { interleaved: { alwaysInterleave: true } },
    });
    defineGrainInterface<IExtended>("ICollide.options");

    expect(getGrainInterface(first.id)?.options.interleaved?.alwaysInterleave).toBe(true);
  });

  it("lets the later definition win per method where both declare one", () => {
    const first = defineGrainInterface<IExtended>("ICollide.options.override", {
      options: { interleaved: { alwaysInterleave: true }, plain: { readOnly: true } },
    });
    defineGrainInterface<IExtended>("ICollide.options.override", {
      options: { plain: { readOnly: false } },
    });

    const entry = getGrainInterface(first.id);
    expect(entry?.options.plain?.readOnly).toBe(false);
    expect(entry?.options.interleaved?.alwaysInterleave).toBe(true);
  });

  it("keeps `extension: true` declared only by the first definition", () => {
    const first = defineGrainInterface<IExtended>("ICollide.extension", { extension: true });
    defineGrainInterface<IExtended>("ICollide.extension");

    // A receiving silo must still dispatch this as an extension call.
    expect(getGrainInterface(first.id)?.extension).toBe(true);
  });

  it("keeps a key kind declared only by the first definition", () => {
    const first = defineGrainInterface<IExtended, "integer">("ICollide.key", { key: "integer" });
    defineGrainInterface<IExtended>("ICollide.key");

    expect(getGrainInterface(first.id)?.key).toBe("integer");
  });

  it("throws when two definitions of one name declare different key kinds", () => {
    defineGrainInterface<IExtended, "integer">("ICollide.key.conflict", { key: "integer" });

    expect(() =>
      defineGrainInterface<IExtended, "guid">("ICollide.key.conflict", { key: "guid" }),
    ).toThrow(/ICollide\.key\.conflict.*already registered with key kind "integer".*"guid"/s);
  });

  it("does not merge the version: it is per-definition, so the later one wins", () => {
    // Versions are deliberately *not* folded together. One name at two versions
    // is the rolling-upgrade path (`versioning.cluster.test.ts` registers
    // `ICounterAt(v)` per silo), each caller sends its own object's version on
    // the wire, and nothing reads the version back off the registry.
    const first = defineGrainInterface<IExtended>("ICollide.version", { version: 3 });
    defineGrainInterface<IExtended>("ICollide.version");

    expect(first.version).toBe(3);
    expect(getGrainInterface(first.id)?.version).toBe(1);
  });

  it("leaves the first definition's own object intact while the second sees the merge", () => {
    const first = defineGrainInterface<IExtended>("ICollide.divergence", {
      extension: true,
      options: { interleaved: { alwaysInterleave: true } },
    });
    const second = defineGrainInterface<IExtended>("ICollide.divergence");

    // Neither holder is impoverished any more: the first keeps what it declared,
    // and the second inherits it rather than silently dropping it.
    expect(first.extension).toBe(true);
    expect(first.options.interleaved?.alwaysInterleave).toBe(true);
    expect(second.extension).toBe(true);
    expect(second.options.interleaved?.alwaysInterleave).toBe(true);
  });
});

describe("registerInterface", () => {
  interface IThing {
    ping(): Promise<void>;
  }

  it("replaces the entry outright without `merge`", () => {
    const first = defineGrainInterface<IThing>("IRegister.replace", { extension: true });
    const replacement = { ...first, extension: undefined } as unknown as GrainInterface<IThing>;

    expect(registerInterface(replacement)).toBe(replacement);
    expect(getGrainInterface(first.id)?.extension).toBeUndefined();
  });

  it("registers as-is when there is no existing entry to merge with", () => {
    const iface = defineGrainInterface<IThing>("IRegister.fresh");
    expect(getGrainInterface(iface.id)).toBe(iface);
  });

  /**
   * The id is `stableHash32(name)`, a 32-bit value, so two different names can
   * land on one id — and every `defineGrain` now publishes an interface under
   * its grain-type name too, which widens the surface. Left undetected the
   * second registration takes over the first's id and inbound calls dispatch to
   * the wrong contract. The colliding pair is found by search, not hard-coded:
   * the point is to exercise the real hash.
   */
  it("rejects two different names that hash to the same id", () => {
    const [first, second] = collidingNames();

    defineGrainInterface<IThing>(first);
    expect(() => defineGrainInterface<IThing>(second)).toThrow(
      new RegExp(`${first}[\\s\\S]*${second}|${second}[\\s\\S]*${first}`),
    );
  });
});

/**
 * A fused definition (one carrying its implementation constructor under `grain`)
 * is merged like any other same-named definition — that is how a hand-written
 * `defineGrainInterface` module pairs with a same-named `defineGrain`. Two *fused*
 * definitions of one name are a different thing: two grain types competing for one
 * wire id, where the merge would silently route one's calls to the other.
 */
describe("registerInterface — fused definitions", () => {
  interface IThing {
    ping(): Promise<void>;
  }

  class GrainA {}
  class GrainB {}

  function fused(name: string, grain: new () => object): GrainInterface<IThing> {
    return { ...defineGrainInterface<IThing>(name), grain };
  }

  it("rejects two fused definitions of one name with different constructors", () => {
    registerInterface(fused("IFused.collide", GrainA), { merge: true });

    expect(() => registerInterface(fused("IFused.collide", GrainB), { merge: true })).toThrow(
      /IFused\.collide[\s\S]*distinct name[\s\S]*interfaceName/,
    );
  });

  it("accepts the same fused definition registered again (module re-evaluation)", () => {
    const first = fused("IFused.repeat", GrainA);
    registerInterface(first, { merge: true });

    expect(() => registerInterface(fused("IFused.repeat", GrainA), { merge: true })).not.toThrow();
  });

  it("still pairs a declared interface with a same-named fused definition", () => {
    const declared = defineGrainInterface<IThing>("IFused.paired", {
      extension: true,
      options: { ping: { readOnly: true } },
    });
    const merged = registerInterface(fused("IFused.paired", GrainA), { merge: true });

    expect(merged.id).toBe(declared.id);
    expect(merged.extension).toBe(true);
    expect(merged.options.ping?.readOnly).toBe(true);
    // The merged entry keeps the implementation, so a later declaration of the
    // same name is still checked against it.
    expect(getGrainInterface(declared.id)?.grain).toBe(GrainA);
  });

  it("keeps the implementation when a later declaration omits it", () => {
    const first = registerInterface(fused("IFused.inherit", GrainA), { merge: true });
    defineGrainInterface<IThing>("IFused.inherit");

    expect(getGrainInterface(first.id)?.grain).toBe(GrainA);
  });
});

/** Two distinct strings with the same `stableHash32`, by birthday search. */
function collidingNames(): [string, string] {
  const seen = new Map<number, string>();
  for (let i = 0; ; i++) {
    const name = `IRegister.collide.${i}`;
    const id = stableHash32(name);
    const previous = seen.get(id);
    if (previous !== undefined) return [previous, name];
    seen.set(id, name);
  }
}

/**
 * Declaring the key kind exactly once. TypeScript has no partial type-argument
 * inference, so supplying `T` explicitly pins `K` to its default: the tempting
 * `defineGrainInterface<ICounter>("ICounter", { key: "integer" })` does not
 * compile, and writing the kind in both places is not an option worth offering.
 *
 * An interface has no implementation to infer `T` from, so the type argument is
 * the single source of truth here: `defineGrainInterface<ICounter, "integer">`.
 * The runtime `key` option stays available for the paths where the kind can be
 * stated once *at runtime* (`defineGrain`, where the contract is inferred), and
 * where both are supplied they must agree.
 */
describe("defineGrainInterface — declaring the key kind", () => {
  interface ICounter {
    get(): Promise<number>;
  }

  it("takes the kind as the second type argument, needing no runtime option", () => {
    const IByInteger = defineGrainInterface<ICounter, "integer">("IKey.integer");

    expectTypeOf(IByInteger).toEqualTypeOf<GrainInterface<ICounter, "integer">>();
    expectTypeOf<KeyTypeOf<NonNullable<(typeof IByInteger)["__k"]>>>().toEqualTypeOf<bigint>();
    // No runtime evidence of the kind — the same as the legacy marker path, and
    // why `GrainInterface.key` is optional.
    expect(IByInteger.key).toBeUndefined();
  });

  it("records the kind at runtime when the option repeats it", () => {
    const iface = defineGrainInterface<ICounter, "integer">("IKey.integer.runtime", {
      key: "integer",
    });

    expect(iface.key).toBe("integer");
  });

  it("rejects a runtime key that disagrees with the type argument", () => {
    const iface = defineGrainInterface<ICounter, "integer">("IKey.integer.conflict", {
      // @ts-expect-error the declared kind is "integer"; the option must agree
      key: "guid",
    });

    // The compiler is the guard: at runtime the option is taken as written.
    expect(iface.key).toBe("guid");
  });

  it("still derives the kind from a legacy phantom marker when none is declared", () => {
    const iface = defineGrainInterface<ICounter & GrainWithIntegerKey>("IKey.marker");

    expectTypeOf<KeyTypeOf<NonNullable<(typeof iface)["__k"]>>>().toEqualTypeOf<bigint>();
    expect(iface.key).toBeUndefined();
  });
});
