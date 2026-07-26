// Ported from dotnet/orleans test/Orleans.Core.Tests/General/Identifiertests.cs @ v10.1.0 (MIT).
import { describe, it } from "vitest";
import { orleansTest } from "@thresh/testing/orleans-test";

// This framework's GrainId (packages/core/src/grain-id.ts) is a plain (type, key) value object
// with a custom string hash - it has no UniqueKey type (32-bit type-code + Guid/long + key-ext
// bit-packing), no Interner reference-cache pattern, no SiloAddress/GrainReference interning, and
// no GrainReferenceActivator/DI-based Orleans serializer. GrainIdUniformHashCodeIsStable also
// pins an exact magic hash value (831806783u) produced by Orleans' specific encoding, which this
// framework's different hash function cannot reproduce without weakening the assertion.
describe("UnitTests.General.IdentifierTests", () => {
  orleansTest.excluded(
    ".NET-specific: pins the exact magic hash value (831806783u) produced by Orleans' GrainId string encoding; this framework's stableHash32 is a different algorithm and cannot reproduce it without weakening the assertion",
    "UnitTests.General.IdentifierTests.GrainIdUniformHashCodeIsStable",
  );

  const uniqueKeyReason =
    ".NET-specific: UniqueKey (32-bit type-code + Guid/long + key-extension bit-packing) has no counterpart in this framework's simpler GrainKey model";

  orleansTest.excluded(
    uniqueKeyReason,
    "UnitTests.General.IdentifierTests.UniqueKeyKeyExtGrainCategoryDisallowsNullKeyExtension",
  );
  orleansTest.excluded(
    uniqueKeyReason,
    "UnitTests.General.IdentifierTests.UniqueKeyKeyExtGrainCategoryDisallowsEmptyKeyExtension",
  );
  orleansTest.excluded(
    uniqueKeyReason,
    "UnitTests.General.IdentifierTests.UniqueKeyKeyExtGrainCategoryDisallowsWhiteSpaceKeyExtension",
  );
  orleansTest.excluded(
    uniqueKeyReason,
    "UnitTests.General.IdentifierTests.ParsingUniqueKeyStringificationShouldReproduceAnIdenticalObject",
  );
  orleansTest.excluded(
    uniqueKeyReason,
    "UnitTests.General.IdentifierTests.UniqueTypeCodeDataShouldStore32BitsOfInformation",
  );
  orleansTest.excluded(
    uniqueKeyReason,
    "UnitTests.General.IdentifierTests.UniqueKeysShouldPreserveTheirPrimaryKeyValueIfItIsGuid",
  );
  orleansTest.excluded(
    uniqueKeyReason,
    "UnitTests.General.IdentifierTests.UniqueKeysShouldPreserveTheirPrimaryKeyValueIfItIsLong",
  );

  orleansTest.excluded(
    ".NET-specific: exercises GrainId Guid-key encode/decode through GrainIdKeyExtensions.CreateGuidKey/GetGuidKey, a byte-packing scheme this framework's GrainKey does not use",
    "UnitTests.General.IdentifierTests.GrainIdShouldEncodeAndDecodePrimaryKeyGuidCorrectly",
  );

  const grainIdRoundTripReason =
    ".NET-specific: MemberData-driven round-trip through GrainId.ToString/Parse/TryParse and System.Text.Json's custom GrainId JSON converter, tied to Orleans' printable-string and JSON-converter formats";

  orleansTest.excluded(
    grainIdRoundTripReason,
    "UnitTests.General.IdentifierTests.GrainId_ToFromPrintableString",
  );
  orleansTest.excluded(
    grainIdRoundTripReason,
    "UnitTests.General.IdentifierTests.GrainId_TryParseFromPrintableString",
  );
  orleansTest.excluded(
    grainIdRoundTripReason,
    "UnitTests.General.IdentifierTests.GrainId_RoundTripJsonConverter",
  );

  const internerReason =
    ".NET-specific: exercises Orleans.Runtime.Interner<TKey,TValue>, a reference-cache utility this framework has no counterpart for (GrainId/SiloAddress here are plain value objects, not interned singletons)";

  orleansTest.excluded(internerReason, "UnitTests.General.IdentifierTests.ID_Interning_GrainID");
  orleansTest.excluded(
    internerReason,
    "UnitTests.General.IdentifierTests.ID_Interning_string_equals",
  );
  orleansTest.excluded(
    internerReason,
    "UnitTests.General.IdentifierTests.ID_Intern_FindOrCreate_derived_class",
  );
  orleansTest.excluded(internerReason, "UnitTests.General.IdentifierTests.Interning_SiloAddress");
  orleansTest.excluded(internerReason, "UnitTests.General.IdentifierTests.Interning_SiloAddress2");
  orleansTest.excluded(
    internerReason,
    "UnitTests.General.IdentifierTests.Interning_SiloAddress_Serialization",
  );

  orleansTest.excluded(
    ".NET-specific: SiloAddress here is a Kubernetes-pod identity (podName/podUid/endpoint), not an IPEndPoint+generation encoded into a parsable string as in Orleans",
    "UnitTests.General.IdentifierTests.SiloAddress_ToFrom_ParsableString",
  );

  orleansTest.excluded(
    ".NET-specific: exercises GrainReference/GrainReferenceActivator DI-resolved round-trip and the DI-registered Orleans binary serializer, neither of which this framework exposes",
    "UnitTests.General.IdentifierTests.GrainReference_Test1",
  );

  // vitest requires at least one runtime test per file; all upstream Facts are
  // excluded above, so this placeholder keeps the file a valid suite.
  it.skip("(all tests in this file are orleansTest.excluded - see above)", () => undefined);
});
