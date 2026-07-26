// Ported from dotnet/orleans test/Orleans.Runtime.Tests/JsonNodeGrainTests.cs @ v10.1.0 (MIT).
//
// Upstream declares `IJsonNodeTestGrain`/`JsonNodeTestGrain` inline in the
// test file itself (namespace DefaultCluster.Tests), rather than under
// test/Grains — mirrored here as a dedicated grain pair for the same reason.
//
// Upstream's `JsonNode` parameter type exists to reproduce a .NET-serializer
// defect where a value statically typed as the base `JsonNode` but holding a
// derived-type instance (`JsonValue`/`JsonArray`/`JsonObject`) at runtime
// threw `InvalidCastException` during Orleans' type-tagged wire format. This
// framework's wire codec (`@thresh/core/value-codec`) is structural JSON with
// no declared-type tagging, so there is no base/derived distinction to get
// wrong — `unknown` is the faithful equivalent of "any JSON-shaped value".
import { defineGrainInterface } from "@thresh/core/grain-interface";
import type { GrainWithIntegerKey } from "@thresh/core/key-kinds";

export interface IJsonNodeTestGrain extends GrainWithIntegerKey {
  processJsonNode(node: unknown): Promise<unknown>;
  getJsonString(node: unknown): Promise<string>;
}

export const IJsonNodeTestGrain = defineGrainInterface<IJsonNodeTestGrain>(
  "DefaultCluster.Tests.IJsonNodeTestGrain",
);
