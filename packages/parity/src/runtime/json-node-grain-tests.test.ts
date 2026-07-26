// Ported from dotnet/orleans test/Orleans.Runtime.Tests/JsonNodeGrainTests.cs @ v10.1.0 (MIT).
//
// Upstream reproduces .NET GitHub issue #9568: a grain method parameter
// statically typed as `System.Text.Json.Nodes.JsonNode` receiving a
// `JsonValue` (a derived runtime type) at the call site, which previously
// threw `InvalidCastException` because Orleans' serializer keyed the wire
// format off the declared (base) type rather than the actual runtime type.
// This framework's wire codec (`@thresh/core/value-codec`) is structural JSON
// with no declared-type tagging, so `unknown`-typed grain parameters are the
// faithful port and every case below is genuinely exercised (not gapped) —
// see json-node-test-grain-interfaces.ts for the framework-shape rationale.
import { afterAll, beforeAll, describe, expect } from "vitest";
import { orleansTest } from "@thresh/testing/orleans-test";
import { TestCluster } from "@thresh/testing/test-cluster";
import {
  IJsonNodeTestGrain,
  JsonNodeTestGrain,
} from "@thresh/parity/grains/impl/json-node-test-grain";

describe("DefaultCluster.Tests.JsonNodeGrainTests", () => {
  let cluster: TestCluster;

  beforeAll(async () => {
    cluster = await TestCluster.start({
      initialSilos: 1,
      grains: [{ ctor: JsonNodeTestGrain, interfaces: [IJsonNodeTestGrain] }],
    });
  });

  afterAll(async () => {
    await cluster.dispose();
  });

  orleansTest(
    "DefaultCluster.Tests.JsonNodeGrainTests.GrainCanProcessJsonValue_AsJsonNode",
    async () => {
      const grain = cluster.getGrain(IJsonNodeTestGrain, 1n);

      const result = await grain.processJsonNode(1);

      expect(result).not.toBeNull();
      expect(result).toBe(1);
    },
  );

  orleansTest(
    "DefaultCluster.Tests.JsonNodeGrainTests.GrainCanProcessVariousJsonNodeTypes",
    async () => {
      const grain = cluster.getGrain(IJsonNodeTestGrain, 2n);

      expect(await grain.processJsonNode(42)).toBe(42);
      expect(await grain.processJsonNode("hello")).toBe("hello");
      expect(await grain.processJsonNode(true)).toBe(true);

      const arrayResult = await grain.processJsonNode([1, 2, 3]);
      expect(Array.isArray(arrayResult)).toBe(true);
      expect((arrayResult as unknown[]).length).toBe(3);

      const objectResult = (await grain.processJsonNode({
        key: "value",
        number: 123,
      })) as Record<string, unknown>;
      expect(typeof objectResult).toBe("object");
      expect(objectResult.key).toBe("value");
      expect(objectResult.number).toBe(123);
    },
  );

  orleansTest(
    "DefaultCluster.Tests.JsonNodeGrainTests.GrainCanProcessComplexJsonStructure",
    async () => {
      const grain = cluster.getGrain(IJsonNodeTestGrain, 3n);

      const complexNode = {
        simpleValue: 123,
        arrayWithValues: ["first", 456, false],
        nestedObject: {
          deepValue: 3.14159,
          deepArray: [true, null],
        },
      };

      const result = (await grain.processJsonNode(complexNode)) as Record<string, unknown>;

      expect(result).not.toBeNull();
      expect(result.simpleValue).toBe(123);

      const array = result.arrayWithValues as unknown[];
      expect(array).not.toBeNull();
      expect(array[0]).toBe("first");
      expect(array[1]).toBe(456);
      expect(array[2]).toBe(false);

      const nested = result.nestedObject as Record<string, unknown>;
      expect(nested).not.toBeNull();
      expect(nested.deepValue).toBeCloseTo(3.14159, 5);
    },
  );

  orleansTest(
    "DefaultCluster.Tests.JsonNodeGrainTests.GrainCanConvertJsonNodeToString",
    async () => {
      const grain = cluster.getGrain(IJsonNodeTestGrain, 4n);

      expect(await grain.getJsonString(42)).toBe("42");
      expect(await grain.getJsonString({ test: "value" })).toContain('"test":"value"');
      expect(await grain.getJsonString([1, 2, 3])).toBe("[1,2,3]");
    },
  );

  orleansTest("DefaultCluster.Tests.JsonNodeGrainTests.GrainHandlesNullJsonNode", async () => {
    const grain = cluster.getGrain(IJsonNodeTestGrain, 5n);

    const result = await grain.processJsonNode(null);
    expect(result).toBeNull();

    const stringResult = await grain.getJsonString(null);
    expect(stringResult).toBe("null");
  });

  orleansTest(
    "DefaultCluster.Tests.JsonNodeGrainTests.MultipleConcurrentGrainCallsWithJsonNodes",
    async () => {
      const tasks = Array.from({ length: 10 }, async (_, i) => {
        const grainId = BigInt(i + 100);
        const value = i * 10;
        const grain = cluster.getGrain(IJsonNodeTestGrain, grainId);
        const result = await grain.processJsonNode(value);
        expect(result).toBe(value);
      });

      await Promise.all(tasks);
    },
  );
});
