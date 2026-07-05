// Ported from dotnet/orleans test/Orleans.Runtime.Tests/JsonNodeGrainTests.cs @ v10.1.0 (MIT).
import { grain } from "@tsva/core/decorators";
import { Grain } from "@tsva/core/grain";
import { IJsonNodeTestGrain } from "@tsva/parity/grains/interfaces/json-node-test-grain-interfaces";

export { IJsonNodeTestGrain };

@grain({ name: "DefaultCluster.Tests.JsonNodeTestGrain" })
export class JsonNodeTestGrain extends Grain implements IJsonNodeTestGrain {
  async processJsonNode(node: unknown): Promise<unknown> {
    // Simply return the node - serialization should handle it
    return node;
  }

  async getJsonString(node: unknown): Promise<string> {
    // Convert to string to verify we received the correct data
    return node === null || node === undefined ? "null" : JSON.stringify(node);
  }
}
