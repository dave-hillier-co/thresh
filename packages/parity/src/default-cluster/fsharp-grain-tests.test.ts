// Ported from dotnet/orleans test/Orleans.DefaultCluster.Tests/FSharpGrainTests.cs @ v10.1.0 (MIT).
//
// Every test here round-trips F# records, discriminated unions and
// `FSharpOption<T>` values (some generic, some carrying nested options) through
// a grain call, to validate Orleans' F#-interop serialization support. This is
// a TypeScript framework with no F# language or runtime present at all — there
// is no F# type to construct, no `Microsoft.FSharp.Core` assembly to reference,
// and nothing analogous to port any of these tests to.
import { describe, it } from "vitest";
import { orleansTest } from "@thresh/testing/orleans-test";

const reason =
  "Exercises Orleans' F#-interop serialization (F# records, discriminated unions, " +
  "FSharpOption<T>) round-tripped through a grain call; there is no F# language or " +
  "runtime in this TypeScript framework.";

describe("DefaultCluster.Tests.General.FSharpGrainTests", () => {
  orleansTest.excluded(reason, "DefaultCluster.Tests.General.FSharpGrainTests.FSharpGrains_Ping");
  orleansTest.excluded(
    reason,
    "DefaultCluster.Tests.General.FSharpGrainTests.FSharpGrains_Ping_Record_ofInt",
  );
  orleansTest.excluded(
    reason,
    "DefaultCluster.Tests.General.FSharpGrainTests.FSharpGrains_Ping_Record_ofIntOption_Some_WithNoAttributes",
  );
  orleansTest.excluded(
    reason,
    "DefaultCluster.Tests.General.FSharpGrainTests.FSharpGrains_Ping_Record_ofIntOption_Some",
  );
  orleansTest.excluded(
    reason,
    "DefaultCluster.Tests.General.FSharpGrainTests.FSharpGrains_Ping_Record_ofIntOption_None",
  );
  orleansTest.excluded(
    reason,
    "DefaultCluster.Tests.General.FSharpGrainTests.FSharpGrains_Ping_GenericRecord_ofInt",
  );
  orleansTest.excluded(
    reason,
    "DefaultCluster.Tests.General.FSharpGrainTests.FSharpGrains_Ping_GenericRecord_ofIntOption_Some",
  );
  orleansTest.excluded(
    reason,
    "DefaultCluster.Tests.General.FSharpGrainTests.FSharpGrains_Ping_GenericRecord_ofIntOption_None",
  );
  orleansTest.excluded(
    reason,
    "DefaultCluster.Tests.General.FSharpGrainTests.FSharpGrains_Ping_IntOption_Some",
  );
  orleansTest.excluded(
    reason,
    "DefaultCluster.Tests.General.FSharpGrainTests.FSharpGrains_Ping_IntOption_None",
  );

  // Every upstream Fact in this file is excluded (above); this placeholder
  // keeps the suite non-empty for the test runner and is not itself part of
  // the upstream accounting.
  it.skip("(all cases in this file are excluded — see above)", () => undefined);
});
