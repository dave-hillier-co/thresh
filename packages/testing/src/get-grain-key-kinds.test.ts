import { describe, expect, expectTypeOf, it } from "vitest";
import { defineGrainInterface } from "@thresh/core/grain-interface";
import type { AnyGrainInterface } from "@thresh/core/grain-interface";
import type { GrainWithIntegerKey } from "@thresh/core/key-kinds";
import type { GrainFactoryAccess } from "@thresh/hosting/silo-builder";
import type { SiloHost } from "@thresh/hosting/silo-host";
import type { GrainRegistrationSpec, TestCluster } from "@thresh/testing/test-cluster";

// Type-level coverage of the hosting/testing `getGrain` surfaces. The assertions
// live in a function that is never called: the values are declaration-only, so
// only `pnpm typecheck` (and vitest's own type inference) reads them.

interface ICounter {
  increment(): Promise<number>;
}

interface IMarkedCounter extends GrainWithIntegerKey {
  increment(): Promise<number>;
}

const IDeclaredCounter = defineGrainInterface<ICounter, "integer">("testing.IDeclaredCounter", {
  key: "integer",
});
const IMarkedCounterDef = defineGrainInterface<IMarkedCounter>("testing.IMarkedCounter");
const IStringCounter = defineGrainInterface<ICounter>("testing.IStringCounter");

function keyKindsAreEnforced(
  host: SiloHost,
  cluster: TestCluster,
  access: GrainFactoryAccess,
): void {
  expectTypeOf(host.getGrain(IDeclaredCounter, 1n)).toEqualTypeOf<ICounter>();
  expectTypeOf(cluster.getGrain(IDeclaredCounter, 1n)).toEqualTypeOf<ICounter>();
  expectTypeOf(access.getGrain(IDeclaredCounter, 1n)).toEqualTypeOf<ICounter>();

  // The legacy phantom marker keeps resolving to the same key type.
  expectTypeOf(host.getGrain(IMarkedCounterDef, 1n)).toEqualTypeOf<IMarkedCounter>();
  expectTypeOf(host.getGrain(IStringCounter, "one")).toEqualTypeOf<ICounter>();

  // @ts-expect-error a string key does not address an integer-keyed interface
  host.getGrain(IDeclaredCounter, "one");
  // @ts-expect-error a bigint key does not address a string-keyed interface
  cluster.getGrain(IStringCounter, 1n);
}

describe("getGrain key kinds (hosting and testing)", () => {
  it("demands the key type the definition declares", () => {
    expect(keyKindsAreEnforced).toBeTypeOf("function");
  });

  it("registers interfaces of mixed key kinds through the erased type", () => {
    const interfaces: AnyGrainInterface[] = [IDeclaredCounter, IMarkedCounterDef, IStringCounter];
    expectTypeOf<GrainRegistrationSpec["interfaces"]>().toEqualTypeOf<
      readonly AnyGrainInterface[]
    >();
    expect(interfaces).toHaveLength(3);
  });
});
