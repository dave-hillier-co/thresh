// Ported from dotnet/orleans test/Orleans.Core.Tests/Directory/GrainDirectoryResolverTests.cs @ v10.1.0 (MIT).
import { describe, it } from "vitest";
import { orleansTest } from "@tsva/testing/orleans-test";

// GrainDirectoryResolver resolves, per grain type, which of several
// registered IGrainDirectory implementations owns it (a grain class can opt
// into a named custom directory such as an Azure Table directory via an
// attribute), wired through a full Orleans HostBuilder/DI silo pipeline
// (UseOrleans, AddGrainDirectory, UseLocalhostClustering). This framework has
// a single GrainDirectory per cluster (packages/directory/src/grain-
// directory.ts) with no per-grain-type custom directory registration/
// resolution mechanism and no DI hosting builder to assemble one from.
const NS = "NonSilo.Tests.Directory.GrainDirectoryResolverTests";
const REASON =
  ".NET-specific mechanism: GrainDirectoryResolver picks a per-grain-type " +
  "custom IGrainDirectory (e.g. an attribute-selected Azure Table directory) " +
  "out of several registered via an Orleans HostBuilder/DI silo pipeline — " +
  "this framework has one GrainDirectory per cluster with no per-grain-type " +
  "custom-directory registration/resolution and no DI hosting builder to " +
  "assemble one from.";

describe(NS, () => {
  orleansTest.excluded(REASON, `${NS}.UserProvidedDirectory`);
  orleansTest.excluded(REASON, `${NS}.DefaultDhtDirectory`);
  orleansTest.excluded(REASON, `${NS}.ListAllDirectories`);

  // vitest requires at least one runtime test per file; all upstream Facts are
  // excluded above, so this placeholder keeps the file a valid suite.
  it.skip("(all tests in this file are orleansTest.excluded — see above)", () => undefined);
});
