// Ported from dotnet/orleans test/Orleans.Core.Tests/Directory/GrainLocatorResolverTests.cs @ v10.1.0 (MIT).
import { describe, it } from "vitest";
import { orleansTest } from "@tsva/testing/orleans-test";

// GrainLocatorResolver picks which IGrainLocator implementation (DhtGrainLocator
// for the default directory, CachedGrainLocator for a custom per-type
// directory, or the client grain locator for client grain types) serves a
// given grain type, wired through the same Orleans HostBuilder/DI silo
// pipeline as GrainDirectoryResolverTests. This framework has one
// GrainDirectory implementation per cluster (DistributedGrainDirectory) with
// no distinct DhtGrainLocator/CachedGrainLocator/client-grain-locator classes
// to choose between and no DI hosting builder to assemble one from.
const NS = "NonSilo.Tests.Directory.GrainLocatorResolverTests";
const REASON =
  ".NET-specific mechanism: GrainLocatorResolver picks between " +
  "DhtGrainLocator/CachedGrainLocator/client-grain-locator implementations " +
  "assembled via an Orleans HostBuilder/DI silo pipeline — this framework " +
  "has a single GrainDirectory implementation per cluster with no distinct " +
  "locator classes to resolve between and no DI hosting builder to " +
  "assemble one from.";

describe(NS, () => {
  orleansTest.excluded(REASON, `${NS}.ReturnsDhtGrainLocatorWhenUsingDhtDirectory`);
  orleansTest.excluded(REASON, `${NS}.ReturnsCachedGrainLocatorWhenUsingCustomDirectory`);
  orleansTest.excluded(REASON, `${NS}.ReturnsClientGrainLocatorWhenUsingClient`);

  // vitest requires at least one runtime test per file; all upstream Facts are
  // excluded above, so this placeholder keeps the file a valid suite.
  it.skip("(all tests in this file are orleansTest.excluded — see above)", () => undefined);
});
