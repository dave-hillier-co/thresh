// Ported from dotnet/orleans test/Orleans.DefaultCluster.Tests/SerializationTests/RoundTripSerializerTests.cs @ v10.1.0 (MIT).
import { afterAll, beforeAll, describe, expect } from "vitest";
import { orleansTest } from "@tsva/testing/orleans-test";
import { TestCluster } from "@tsva/testing/test-cluster";
import {
  IRoundtripSerializationGrain,
  RoundtripSerializationGrain,
} from "@tsva/parity/grains/impl/roundtrip-serialization-grain";
import { randomIntegerKey } from "@tsva/parity/support/keys";

describe("UnitTests.SerializerTests.RoundTripSerializerTests", () => {
  let cluster: TestCluster;

  beforeAll(async () => {
    cluster = await TestCluster.start({
      initialSilos: 2,
      grains: [{ ctor: RoundtripSerializationGrain, interfaces: [IRoundtripSerializationGrain] }],
    });
  });

  afterAll(async () => {
    await cluster.dispose();
  });

  const getGrain = () => cluster.getGrain(IRoundtripSerializationGrain, randomIntegerKey());

  orleansTest(
    "UnitTests.SerializerTests.RoundTripSerializerTests.Serialize_TestMethodResultRecord",
    async () => {
      const grain = getGrain();
      const retVal = await grain.getRetValForParamVal({ value: 42 });
      expect(retVal.value).toBe(42);
    },
  );

  orleansTest(
    "UnitTests.SerializerTests.RoundTripSerializerTests.Serialize_TestMethodResultEnum",
    async () => {
      const grain = getGrain();
      const result = await grain.getEnemyType();
      // Enum return value wasn't transmitted properly
      expect(result).toBe("Enemy2");
    },
  );

  orleansTest(
    "UnitTests.SerializerTests.RoundTripSerializerTests.Serialize_TestMethodResultWithInheritedClosedGeneric",
    async () => {
      const grain = getGrain();
      const result = await grain.getClosedGenericValue();
      expect(result).not.toBeNull();
    },
  );
});
