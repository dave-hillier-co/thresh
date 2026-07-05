// Ported from dotnet/orleans test/Orleans.Core.Tests/Serialization/BuiltInSerializerTests.cs @ v10.1.0 (MIT).
import { describe, it } from "vitest";
import { orleansTest } from "@tsva/testing/orleans-test";

describe("UnitTests.Serialization.BuiltInSerializerTests", () => {
  orleansTest.excluded(
    ".NET serializer internals (Orleans binary serializer / message wire format / Newtonsoft, System.Text.Json & protobuf codec plumbing) have no equivalent in this framework",
    "UnitTests.Serialization.BuiltInSerializerTests.InternalSerializableTypesHaveSerializers",
  );
  orleansTest.excluded(
    ".NET serializer internals (Orleans binary serializer / message wire format / Newtonsoft, System.Text.Json & protobuf codec plumbing) have no equivalent in this framework",
    "UnitTests.Serialization.BuiltInSerializerTests.ValueTupleTypesHasSerializer",
  );
  orleansTest.excluded(
    ".NET serializer internals (Orleans binary serializer / message wire format / Newtonsoft, System.Text.Json & protobuf codec plumbing) have no equivalent in this framework",
    "UnitTests.Serialization.BuiltInSerializerTests.Serialize_ComplexAccessibleClass",
  );
  orleansTest.excluded(
    ".NET serializer internals (Orleans binary serializer / message wire format / Newtonsoft, System.Text.Json & protobuf codec plumbing) have no equivalent in this framework",
    "UnitTests.Serialization.BuiltInSerializerTests.Serialize_Type",
  );
  orleansTest.excluded(
    ".NET serializer internals (Orleans binary serializer / message wire format / Newtonsoft, System.Text.Json & protobuf codec plumbing) have no equivalent in this framework",
    "UnitTests.Serialization.BuiltInSerializerTests.Serialize_ComplexStruct",
  );
  orleansTest.excluded(
    ".NET serializer internals (Orleans binary serializer / message wire format / Newtonsoft, System.Text.Json & protobuf codec plumbing) have no equivalent in this framework",
    "UnitTests.Serialization.BuiltInSerializerTests.Serialize_EmptyList",
  );
  orleansTest.excluded(
    ".NET serializer internals (Orleans binary serializer / message wire format / Newtonsoft, System.Text.Json & protobuf codec plumbing) have no equivalent in this framework",
    "UnitTests.Serialization.BuiltInSerializerTests.Serialize_BasicDictionaries",
  );
  orleansTest.excluded(
    ".NET serializer internals (Orleans binary serializer / message wire format / Newtonsoft, System.Text.Json & protobuf codec plumbing) have no equivalent in this framework",
    "UnitTests.Serialization.BuiltInSerializerTests.Serialize_ReadOnlyDictionary",
  );
  orleansTest.excluded(
    ".NET serializer internals (Orleans binary serializer / message wire format / Newtonsoft, System.Text.Json & protobuf codec plumbing) have no equivalent in this framework",
    "UnitTests.Serialization.BuiltInSerializerTests.Serialize_DictionaryWithComparer",
  );
  orleansTest.excluded(
    ".NET serializer internals (Orleans binary serializer / message wire format / Newtonsoft, System.Text.Json & protobuf codec plumbing) have no equivalent in this framework",
    "UnitTests.Serialization.BuiltInSerializerTests.Serialize_SortedDictionaryWithComparer",
  );
  orleansTest.excluded(
    ".NET serializer internals (Orleans binary serializer / message wire format / Newtonsoft, System.Text.Json & protobuf codec plumbing) have no equivalent in this framework",
    "UnitTests.Serialization.BuiltInSerializerTests.Serialize_SortedListWithComparer",
  );
  orleansTest.excluded(
    ".NET serializer internals (Orleans binary serializer / message wire format / Newtonsoft, System.Text.Json & protobuf codec plumbing) have no equivalent in this framework",
    "UnitTests.Serialization.BuiltInSerializerTests.Serialize_HashSetWithComparer",
  );
  orleansTest.excluded(
    ".NET serializer internals (Orleans binary serializer / message wire format / Newtonsoft, System.Text.Json & protobuf codec plumbing) have no equivalent in this framework",
    "UnitTests.Serialization.BuiltInSerializerTests.Serialize_Stack",
  );
  orleansTest.excluded(
    ".NET serializer internals (Orleans binary serializer / message wire format / Newtonsoft, System.Text.Json & protobuf codec plumbing) have no equivalent in this framework",
    "UnitTests.Serialization.BuiltInSerializerTests.Serialize_TypeWithOnDeserializedHook",
  );
  orleansTest.excluded(
    ".NET serializer internals (Orleans binary serializer / message wire format / Newtonsoft, System.Text.Json & protobuf codec plumbing) have no equivalent in this framework",
    "UnitTests.Serialization.BuiltInSerializerTests.Serialize_SortedSetWithComparer",
  );
  orleansTest.excluded(
    ".NET serializer internals (Orleans binary serializer / message wire format / Newtonsoft, System.Text.Json & protobuf codec plumbing) have no equivalent in this framework",
    "UnitTests.Serialization.BuiltInSerializerTests.Serialize_Array",
  );
  orleansTest.excluded(
    ".NET serializer internals (Orleans binary serializer / message wire format / Newtonsoft, System.Text.Json & protobuf codec plumbing) have no equivalent in this framework",
    "UnitTests.Serialization.BuiltInSerializerTests.Serialize_ArrayOfArrays",
  );
  orleansTest.excluded(
    ".NET serializer internals (Orleans binary serializer / message wire format / Newtonsoft, System.Text.Json & protobuf codec plumbing) have no equivalent in this framework",
    "UnitTests.Serialization.BuiltInSerializerTests.Serialize_ArrayOfArrayOfArrays",
  );
  orleansTest.excluded(
    ".NET serializer internals (Orleans binary serializer / message wire format / Newtonsoft, System.Text.Json & protobuf codec plumbing) have no equivalent in this framework",
    "UnitTests.Serialization.BuiltInSerializerTests.Serialize_ReadOnlyCollection",
  );
  orleansTest.excluded(
    ".NET serializer internals (Orleans binary serializer / message wire format / Newtonsoft, System.Text.Json & protobuf codec plumbing) have no equivalent in this framework",
    "UnitTests.Serialization.BuiltInSerializerTests.Serialize_ObjectIdentity",
  );
  orleansTest.excluded(
    ".NET serializer internals (Orleans binary serializer / message wire format / Newtonsoft, System.Text.Json & protobuf codec plumbing) have no equivalent in this framework",
    "UnitTests.Serialization.BuiltInSerializerTests.Serialize_Unrecognized",
  );
  orleansTest.excluded(
    ".NET serializer internals (Orleans binary serializer / message wire format / Newtonsoft, System.Text.Json & protobuf codec plumbing) have no equivalent in this framework",
    "UnitTests.Serialization.BuiltInSerializerTests.Serialize_Immutable",
  );
  orleansTest.excluded(
    ".NET serializer internals (Orleans binary serializer / message wire format / Newtonsoft, System.Text.Json & protobuf codec plumbing) have no equivalent in this framework",
    "UnitTests.Serialization.BuiltInSerializerTests.Serialize_GrainReference",
  );
  orleansTest.excluded(
    ".NET serializer internals (Orleans binary serializer / message wire format / Newtonsoft, System.Text.Json & protobuf codec plumbing) have no equivalent in this framework",
    "UnitTests.Serialization.BuiltInSerializerTests.Serialize_CircularReference",
  );
  orleansTest.excluded(
    ".NET serializer internals (Orleans binary serializer / message wire format / Newtonsoft, System.Text.Json & protobuf codec plumbing) have no equivalent in this framework",
    "UnitTests.Serialization.BuiltInSerializerTests.Serialize_Enums",
  );

  // vitest requires at least one runtime test per file; all upstream Facts are
  // excluded above, so this placeholder keeps the file a valid suite.
  it.skip("(all tests in this file are orleansTest.excluded - see above)", () => undefined);
});
