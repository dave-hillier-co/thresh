// Ported from dotnet/orleans test/Grains/TestGrains/RoundtripSerializationGrain.cs @ v10.1.0 (MIT).
import { grain } from "@thresh/core/decorators";
import { Grain } from "@thresh/core/grain";
import type {
  CampaignEnemyTestType,
  ParamVal,
  RetVal,
} from "@thresh/parity/grains/interfaces/roundtrip-serialization-grain-interfaces";
import { IRoundtripSerializationGrain } from "@thresh/parity/grains/interfaces/roundtrip-serialization-grain-interfaces";

export { IRoundtripSerializationGrain };

@grain({ name: "UnitTests.Grains.RoundtripSerializationGrain" })
export class RoundtripSerializationGrain extends Grain implements IRoundtripSerializationGrain {
  async getEnemyType(): Promise<CampaignEnemyTestType> {
    return "Enemy2";
  }

  async getClosedGenericValue(): Promise<unknown> {
    // Upstream constructs a closed generic (`List<ImmutableList<HashSet<Tuple<int, string>>>>`)
    // unlikely to be pre-registered with the serializer, to prove the codegen
    // pipeline can still build a serializer for it on demand. Nested
    // collections are the closest TS analog of that generic-type-composition
    // shape; the test only asserts the result is non-null.
    return [[[[1, "a"]]]];
  }

  async getRetValForParamVal(param: ParamVal): Promise<RetVal> {
    return { value: param.value };
  }
}
