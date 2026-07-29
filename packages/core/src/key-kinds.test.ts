import { describe, expect, expectTypeOf, it } from "vitest";
import type { GrainSetup } from "@thresh/core/define-grain";
import { Grain } from "@thresh/core/grain";
import { defineGrainInterface } from "@thresh/core/grain-interface";
import type { AnyGrainInterface, GrainInterface } from "@thresh/core/grain-interface";
import type { GrainRuntime } from "@thresh/core/grain-runtime";
import type { CompoundKey, GrainKey, GrainKeyKind } from "@thresh/core/grain-key";
import { Guid } from "@thresh/core/guid";
import type {
  GrainKeyFor,
  KeyKindFromMarker,
  KeyTypeOf,
  GrainWithGuidCompoundKey,
  GrainWithGuidKey,
  GrainWithIntegerCompoundKey,
  GrainWithIntegerKey,
  GrainWithStringKey,
} from "@thresh/core/key-kinds";

// Characterization of `GrainKeyFor<T>` as it behaves today. These assertions are
// type-level: the unit run only proves the module loads, `pnpm typecheck` is what
// actually checks them (the repo tsconfig includes `packages/*/src/**/*.ts`).

interface IUnmarked {
  ping(): Promise<void>;
}

interface IStringKeyed extends GrainWithStringKey {
  ping(): Promise<void>;
}

interface IIntegerKeyed extends GrainWithIntegerKey {
  ping(): Promise<void>;
}

interface IGuidKeyed extends GrainWithGuidKey {
  ping(): Promise<void>;
}

interface IGuidCompoundKeyed extends GrainWithGuidCompoundKey {
  ping(): Promise<void>;
}

interface IIntegerCompoundKeyed extends GrainWithIntegerCompoundKey {
  ping(): Promise<void>;
}

describe("GrainKeyFor", () => {
  it("maps each key marker to its factory key type", () => {
    expectTypeOf<GrainKeyFor<IStringKeyed>>().toEqualTypeOf<string>();
    expectTypeOf<GrainKeyFor<IIntegerKeyed>>().toEqualTypeOf<bigint>();
    expectTypeOf<GrainKeyFor<IGuidKeyed>>().toEqualTypeOf<Guid>();
    expectTypeOf<GrainKeyFor<IGuidCompoundKeyed>>().toEqualTypeOf<CompoundKey<Guid>>();
    expectTypeOf<GrainKeyFor<IIntegerCompoundKeyed>>().toEqualTypeOf<CompoundKey<bigint>>();
    expect(true).toBe(true);
  });

  it("falls through to string for an interface with no marker at all", () => {
    // Load-bearing and fragile: it holds only because the phantom `__key` is
    // optional-only, which makes each marker a *weak type*, and a member-bearing
    // interface with no `__key` is therefore not assignable to any of them.
    expectTypeOf<GrainKeyFor<IUnmarked>>().toEqualTypeOf<string>();
    expectTypeOf<
      GrainKeyFor<{ get(): Promise<number>; set(v: number): Promise<void> }>
    >().toEqualTypeOf<string>();
    expect(true).toBe(true);
  });

  it("does NOT fall through to string when weak-type detection cannot apply", () => {
    // Sharp edge worth knowing before this type is reworked: weak-type detection
    // needs the source to have at least one property and no index signature. A
    // memberless or index-signature interface is assignable to *every* marker, so
    // the first branch of the conditional wins.
    expectTypeOf<GrainKeyFor<object>>().toEqualTypeOf<CompoundKey<Guid>>();
    expectTypeOf<GrainKeyFor<Record<string, unknown>>>().toEqualTypeOf<CompoundKey<Guid>>();
    expect(true).toBe(true);
  });

  it("resolves a compound marker without falling into the plain-marker branches", () => {
    // The conditional tests the compound markers first; a compound-keyed interface
    // must not degrade to `Guid` or `bigint`.
    expectTypeOf<GrainKeyFor<IGuidCompoundKeyed>>().not.toEqualTypeOf<Guid>();
    expectTypeOf<GrainKeyFor<IIntegerCompoundKeyed>>().not.toEqualTypeOf<bigint>();
    expect(true).toBe(true);
  });

  it("distributes over unions, as a naked conditional type", () => {
    expectTypeOf<GrainKeyFor<IStringKeyed | IIntegerKeyed>>().toEqualTypeOf<string | bigint>();
    expect(true).toBe(true);
  });
});

// The declared-kind machinery `GrainKeyFor` is now expressed in terms of.

/** Extracts the key argument a `getGrain` call would demand for a definition. */
type KeyArgOf<D> =
  D extends GrainInterface<unknown, infer K extends GrainKeyKind> ? KeyTypeOf<K> : never;

describe("KeyTypeOf", () => {
  it("maps each kind to its factory key type", () => {
    expectTypeOf<KeyTypeOf<"string">>().toEqualTypeOf<string>();
    expectTypeOf<KeyTypeOf<"integer">>().toEqualTypeOf<bigint>();
    expectTypeOf<KeyTypeOf<"guid">>().toEqualTypeOf<Guid>();
    expectTypeOf<KeyTypeOf<"guid-compound">>().toEqualTypeOf<CompoundKey<Guid>>();
    expectTypeOf<KeyTypeOf<"integer-compound">>().toEqualTypeOf<CompoundKey<bigint>>();
    expect(true).toBe(true);
  });

  it("distributes over a union of kinds", () => {
    expectTypeOf<KeyTypeOf<"string" | "integer">>().toEqualTypeOf<string | bigint>();
    expectTypeOf<KeyTypeOf<GrainKeyKind>>().toEqualTypeOf<GrainKey>();
    expect(true).toBe(true);
  });
});

describe("KeyKindFromMarker", () => {
  it("recovers the declared kind from a legacy phantom marker", () => {
    expectTypeOf<KeyKindFromMarker<IStringKeyed>>().toEqualTypeOf<"string">();
    expectTypeOf<KeyKindFromMarker<IIntegerKeyed>>().toEqualTypeOf<"integer">();
    expectTypeOf<KeyKindFromMarker<IGuidKeyed>>().toEqualTypeOf<"guid">();
    expectTypeOf<KeyKindFromMarker<IGuidCompoundKeyed>>().toEqualTypeOf<"guid-compound">();
    expectTypeOf<KeyKindFromMarker<IIntegerCompoundKeyed>>().toEqualTypeOf<"integer-compound">();
    expectTypeOf<KeyKindFromMarker<IUnmarked>>().toEqualTypeOf<"string">();
    expect(true).toBe(true);
  });
});

describe("the `key` option", () => {
  const IMarkedInteger = defineGrainInterface<IIntegerKeyed>("test.IMarkedInteger");
  const IOptionInteger = defineGrainInterface<IUnmarked, "integer">("test.IOptionInteger", {
    key: "integer",
  });

  it("gives a marker-free interface the key type it declares", () => {
    expectTypeOf<KeyArgOf<typeof IOptionInteger>>().toEqualTypeOf<bigint>();
    expect(IOptionInteger.key).toBe("integer");
  });

  it("agrees with the marker path", () => {
    expectTypeOf<KeyArgOf<typeof IOptionInteger>>().toEqualTypeOf<
      KeyArgOf<typeof IMarkedInteger>
    >();
    expect(true).toBe(true);
  });

  it("leaves `key` absent when the kind comes from a marker", () => {
    // The kind is still carried in the type; only the runtime field is absent.
    expectTypeOf<KeyArgOf<typeof IMarkedInteger>>().toEqualTypeOf<bigint>();
    expect(IMarkedInteger.key).toBeUndefined();
  });

  it("defaults an undeclared, unmarked interface to a string key", () => {
    const IPlain = defineGrainInterface<IUnmarked>("test.IPlain");
    expectTypeOf<KeyArgOf<typeof IPlain>>().toEqualTypeOf<string>();
    expect(IPlain.key).toBeUndefined();
  });
});

describe("AnyGrainInterface", () => {
  it("accepts a definition of any declared kind", () => {
    const erased: AnyGrainInterface[] = [
      defineGrainInterface<IIntegerKeyed>("test.IErasedInteger"),
      defineGrainInterface<IGuidCompoundKeyed>("test.IErasedCompound"),
      defineGrainInterface<IUnmarked, "guid">("test.IErasedGuid", { key: "guid" }),
    ];
    expect(erased).toHaveLength(3);
  });
});

describe("the core getGrain seams", () => {
  const IDeclaredInteger = defineGrainInterface<IUnmarked, "integer">("test.IDeclaredInteger", {
    key: "integer",
  });
  const IDeclaredGuid = defineGrainInterface<IUnmarked, "guid">("test.IDeclaredGuid", {
    key: "guid",
  });

  it("takes the key type from the definition's declared kind", () => {
    expectTypeOf<GrainRuntime["getGrain"]>().toBeCallableWith(IDeclaredInteger, 1n);
    expectTypeOf<GrainSetup["getGrain"]>().toBeCallableWith(IDeclaredInteger, 1n);
    expectTypeOf<GrainRuntime["getGrain"]>().toBeCallableWith(IDeclaredGuid, Guid.newGuid());
    expect(IDeclaredInteger.key).toBe("integer");
  });

  it("rejects a key of the wrong kind", () => {
    const runtime = {} as GrainRuntime;
    // @ts-expect-error a string key does not address an integer-keyed interface
    expectTypeOf(() => runtime.getGrain(IDeclaredInteger, "a")).toBeCallableWith();
    expect(true).toBe(true);
  });

  it("still takes it from a legacy marker when no kind is declared", () => {
    const IMarked = defineGrainInterface<IIntegerKeyed>("test.ISeamMarked");
    expectTypeOf<GrainRuntime["getGrain"]>().toBeCallableWith(IMarked, 1n);
    expectTypeOf<GrainSetup["getGrain"]>().toBeCallableWith(IMarked, 1n);
    expect(IMarked.key).toBeUndefined();
  });

  it("reaches the protected accessor on the Grain base class", () => {
    class Caller extends Grain {
      callee(): IUnmarked {
        return this.getGrain(IDeclaredInteger, 1n);
      }
    }
    expect(new Caller()).toBeInstanceOf(Grain);
  });
});
