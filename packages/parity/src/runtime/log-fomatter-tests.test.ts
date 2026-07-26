// Ported from dotnet/orleans test/Orleans.Runtime.Tests/LogFomatterTests.cs @ v10.1.0 (MIT).
// (Upstream file name keeps its own "Fomatter" typo verbatim.)
//
// Every case builds a raw `Microsoft.Extensions.DependencyInjection`
// `ServiceCollection`, registers `IOptionFormatter`/`IOptionFormatterResolver`
// implementations via `ConfigureFormatter`/`ConfigureFormatterResolver`, and
// asserts on formatter resolution precedence (pre/post registration
// override), named-options formatting, and `[Redact]`/`[RedactConnectionString]`
// attribute-driven log redaction of Orleans `Options` classes when logged at
// startup. This is entirely Orleans' .NET-DI-based options-logging
// infrastructure (IOptionFormatter, IOptionFormatterResolver, ConfigureFormatter
// extension methods, redaction attributes) with no grain, cluster, or
// serialization surface at all — this framework has neither a DI container
// nor an Options-formatter/redaction system to port these against.
import { describe, it } from "vitest";
import { orleansTest } from "@thresh/testing/orleans-test";

describe("Tester.LogFomatterTests", () => {
  const reason =
    "Orleans' DI-based IOptionFormatter/IOptionFormatterResolver options-logging and [Redact]/[RedactConnectionString] infrastructure — no DI container or options-formatter system exists in this framework";

  orleansTest.excluded(reason, "Tester.LogFomatterTests.CanResolveFormatter");
  orleansTest.excluded(reason, "Tester.LogFomatterTests.CanResolveGenericFormatter");
  orleansTest.excluded(reason, "Tester.LogFomatterTests.GenericFormatterRedact");
  orleansTest.excluded(reason, "Tester.LogFomatterTests.GenericFormatterWithListAndDictionary");
  orleansTest.excluded(
    reason,
    "Tester.LogFomatterTests.FormatterConfiguredTwiceDoesNotLeadToDuplicatedFormatter",
  );
  orleansTest.excluded(
    reason,
    "Tester.LogFomatterTests.CustomFormatterOverridesDefaultFormatter_PreRegistration",
  );
  orleansTest.excluded(
    reason,
    "Tester.LogFomatterTests.CustomFormatterOverridesDefaultFormatter_PostRegistration",
  );
  orleansTest.excluded(reason, "Tester.LogFomatterTests.NamedFormatterGoldenPath");
  orleansTest.excluded(reason, "Tester.LogFomatterTests.NamedGenericFormatterGoldenPath");
  orleansTest.excluded(
    reason,
    "Tester.LogFomatterTests.CustomFormatterResolverOverridesDefaultFormatter_PreRegistration",
  );
  orleansTest.excluded(
    reason,
    "Tester.LogFomatterTests.CustomFormatterResolverOverridesDefaultFormatter_PostRegistration",
  );

  // This file's upstream Facts are entirely excluded (above); this
  // placeholder keeps the suite non-empty for the test runner and is not
  // itself part of the upstream accounting.
  it.skip("(all cases in this file are excluded — see above)", () => undefined);
});
