import { describe, expect, expectTypeOf, it } from "vitest";
import { defineGrainInterface } from "@thresh/core/grain-interface";
import type { GrainWithGuidKey } from "@thresh/core/key-kinds";
import type { Guid } from "@thresh/core/guid";
import type { ClientNode } from "@thresh/client/client-node";

// Type-level coverage of `ClientNode.getGrain`. The assertions live in a
// function that is never called: `client` is declaration-only, so only
// `pnpm typecheck` reads them.

interface ICounter {
  increment(): Promise<number>;
}

interface IMarkedCounter extends GrainWithGuidKey {
  increment(): Promise<number>;
}

const IDeclaredCounter = defineGrainInterface<ICounter, "guid">("client.IDeclaredCounter", {
  key: "guid",
});
const IMarkedCounterDef = defineGrainInterface<IMarkedCounter>("client.IMarkedCounter");
const IStringCounter = defineGrainInterface<ICounter>("client.IStringCounter");

function keyKindsAreEnforced(client: ClientNode, guid: Guid): void {
  expectTypeOf(client.getGrain(IDeclaredCounter, guid)).toEqualTypeOf<ICounter>();
  expectTypeOf(client.getGrain(IMarkedCounterDef, guid)).toEqualTypeOf<IMarkedCounter>();
  expectTypeOf(client.getGrain(IStringCounter, "one")).toEqualTypeOf<ICounter>();

  // @ts-expect-error a string key does not address a guid-keyed interface
  client.getGrain(IDeclaredCounter, "one");
}

describe("ClientNode.getGrain key kinds", () => {
  it("demands the key type the definition declares", () => {
    expect(keyKindsAreEnforced).toBeTypeOf("function");
  });
});
