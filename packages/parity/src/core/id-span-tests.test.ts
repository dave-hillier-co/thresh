// Ported from dotnet/orleans test/Orleans.Core.Tests/IdSpanTests.cs @ v10.1.0 (MIT).
import { describe, it } from "vitest";
import { orleansTest } from "@tsva/testing/orleans-test";

// Tests Orleans.Runtime.IdSpan, a byte-array-backed identity primitive with default-vs-empty
// distinctions, custom hashing, and ordinal comparison. This framework's GrainType
// (packages/core/src/grain-type.ts) is a plain string, with no byte-span identity primitive or
// null-vs-empty-array distinction to test.
describe("NonSilo.Tests.IdSpanTests", () => {
  const reason =
    ".NET-specific: exercises Orleans.Runtime.IdSpan, a byte-array-backed identity primitive with null-vs-empty semantics; this framework's GrainType is a plain string with no equivalent primitive";

  orleansTest.excluded(
    reason,
    "NonSilo.Tests.IdSpanTests.IdSpan_CreateEmptyString_NotEqualToDefault",
  );
  orleansTest.excluded(reason, "NonSilo.Tests.IdSpanTests.IdSpan_HashCode_ConsistentWithEquality");
  orleansTest.excluded(reason, "NonSilo.Tests.IdSpanTests.IdSpan_Default_HasExpectedProperties");
  orleansTest.excluded(
    reason,
    "NonSilo.Tests.IdSpanTests.IdSpan_CreateEmptyString_HasExpectedProperties",
  );
  orleansTest.excluded(reason, "NonSilo.Tests.IdSpanTests.IdSpan_SameContent_AreEqual");
  orleansTest.excluded(reason, "NonSilo.Tests.IdSpanTests.IdSpan_DifferentContent_AreNotEqual");
  orleansTest.excluded(reason, "NonSilo.Tests.IdSpanTests.IdSpan_CompareTo_HandlesNullAndEmpty");

  // vitest requires at least one runtime test per file; all upstream Facts are
  // excluded above, so this placeholder keeps the file a valid suite.
  it.skip("(all tests in this file are orleansTest.excluded - see above)", () => undefined);
});
