import { describe, expectTypeOf, it } from "vitest";
import type { CompoundKey } from "@thresh/core/grain-key";
import type { Guid } from "@thresh/core/guid";
import type {
  GrainKey,
  GrainKeyFor,
  GrainWithGuidCompoundKey,
  GrainWithIntegerKey,
  KeyedGrain,
} from "@thresh/core/key-kinds";

describe("GrainKey", () => {
  it("maps idiomatic GrainKey markers to getGrain key types", () => {
    type StringGrain = GrainKey<string> & { ping(): Promise<void> };
    type IntegerGrain = GrainKey<bigint> & { ping(): Promise<void> };
    type GuidCompoundGrain = GrainKey<CompoundKey<Guid>> & { ping(): Promise<void> };

    expectTypeOf<GrainKeyFor<StringGrain>>().toEqualTypeOf<string>();
    expectTypeOf<GrainKeyFor<IntegerGrain>>().toEqualTypeOf<bigint>();
    expectTypeOf<GrainKeyFor<GuidCompoundGrain>>().toEqualTypeOf<CompoundKey<Guid>>();
  });

  it("keeps Orleans-compatible marker names as aliases", () => {
    type IntegerGrain = GrainWithIntegerKey & { ping(): Promise<void> };
    type GuidCompoundGrain = GrainWithGuidCompoundKey & { ping(): Promise<void> };

    expectTypeOf<GrainKeyFor<IntegerGrain>>().toEqualTypeOf<bigint>();
    expectTypeOf<GrainKeyFor<GuidCompoundGrain>>().toEqualTypeOf<CompoundKey<Guid>>();
  });

  it("defaults unmarked grain surfaces to string keys", () => {
    type Greeter = KeyedGrain & { greet(name: string): Promise<string> };
    type LegacyUnmarked = { greet(name: string): Promise<string> };

    expectTypeOf<GrainKeyFor<Greeter>>().toEqualTypeOf<string>();
    expectTypeOf<GrainKeyFor<LegacyUnmarked>>().toEqualTypeOf<string>();
  });
});
