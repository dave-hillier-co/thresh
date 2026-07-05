// Ported from dotnet/orleans test/Orleans.GrainDirectory.Tests/GrainDirectory/DistributedGrainDirectoryTests.cs @ v10.1.0 (MIT).
import { describe, it } from "vitest";
import { orleansTest } from "@tsva/testing/orleans-test";

// `DefaultGrainDirectoryTests` runs the shared `GrainDirectoryTests<IGrainDirectory>`
// base-class facts (test/Orleans.Runtime.Tests/Directories/GrainDirectoryTests.cs)
// against the silo's `GrainDirectoryResolver.DefaultGrainDirectory` — Orleans'
// pluggable, CAS-based `IGrainDirectory` abstraction (Register/Lookup/Unregister,
// with a `previousAddress` compare-and-set on Register).
//
// This framework has an equivalent CAS directory internally
// (`DistributedGrainDirectory` / `LocalDirectoryPartition` in `@tsva/directory`,
// matching Register/Lookup/Unregister semantics entry-for-entry), but it is not
// exposed as a pluggable, testable surface from grain code or the parity test
// harness: `@tsva/parity` has no dependency on `@tsva/directory` (only
// `@tsva/core`/`@tsva/testing`/`@tsva/hosting`/`@tsva/durable-jobs`), and adding
// one is out of scope for this suite (package.json is a hard boundary). There is
// also no `TestCluster` accessor that hands back the active silo's directory
// instance for direct Register/Lookup/Unregister calls. So every fact this class
// inherits is a gap in test-surface reachability, not in underlying behaviour.

describe("UnitTests.GrainDirectory.DefaultGrainDirectoryTests", () => {
  const NS = "UnitTests.GrainDirectory.DefaultGrainDirectoryTests";

  orleansTest.gap("GAP-GRAIN-DIRECTORY-API", `${NS}.RegisterLookupUnregisterLookup`);
  orleansTest.gap("GAP-GRAIN-DIRECTORY-API", `${NS}.DoNotOverwriteEntry`);
  orleansTest.gap("GAP-GRAIN-DIRECTORY-API", `${NS}.OverwriteEntryIfMatch`);
  orleansTest.gap("GAP-GRAIN-DIRECTORY-API", `${NS}.DoNotDeleteDifferentActivationIdEntry`);
  orleansTest.gap("GAP-GRAIN-DIRECTORY-API", `${NS}.LookupNotFound`);

  // vitest requires at least one runtime test per file; all upstream Facts are
  // gapped above, so this placeholder keeps the file a valid suite.
  it.skip("(all tests in this file are orleansTest.gap — see above)", () => undefined);
});
