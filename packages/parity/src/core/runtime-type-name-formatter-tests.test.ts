// Ported from dotnet/orleans test/Orleans.Core.Tests/RuntimeTypeNameFormatterTests.cs @ v10.1.0 (MIT).
import { describe, it } from "vitest";
import { orleansTest } from "@tsva/testing/orleans-test";

describe("NonSilo.Tests.RuntimeTypeNameFormatterTests", () => {
  orleansTest.excluded(
    ".NET reflection-based Type name formatting/parsing (RuntimeTypeNameFormatter/RuntimeTypeNameParser) has no equivalent in this framework",
    "NonSilo.Tests.RuntimeTypeNameFormatterTests.FormattedTypeNamesAreRecoverable",
  );
  orleansTest.excluded(
    ".NET reflection-based Type name formatting/parsing (RuntimeTypeNameFormatter/RuntimeTypeNameParser) has no equivalent in this framework",
    "NonSilo.Tests.RuntimeTypeNameFormatterTests.ParsedTypeNamesAreIdenticalToFormattedNames",
  );
  orleansTest.excluded(
    ".NET reflection-based Type name formatting/parsing (RuntimeTypeNameFormatter/RuntimeTypeNameParser) has no equivalent in this framework",
    "NonSilo.Tests.RuntimeTypeNameFormatterTests.InvalidNamesThrowDescriptiveErrorMessage",
  );

  // vitest requires at least one runtime test per file; all upstream Facts are
  // excluded above, so this placeholder keeps the file a valid suite.
  it.skip("(all tests in this file are orleansTest.excluded - see above)", () => undefined);
});
