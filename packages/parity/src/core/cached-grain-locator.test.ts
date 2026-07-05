// Ported from dotnet/orleans test/Orleans.Core.Tests/Directory/CachedGrainLocatorTests.cs @ v10.1.0 (MIT).
import { describe, it } from "vitest";
import { orleansTest } from "@tsva/testing/orleans-test";

// CachedGrainLocator is Orleans' per-silo caching wrapper around the
// pluggable IGrainDirectory abstraction: it composes a GrainDirectoryResolver
// (per-grain-type custom directory selection), a MockClusterMembershipService
// (SiloStatus gossip) and a SiloLifecycleSubject (stage-ordered startup), all
// wired through NSubstitute mocks and DI. This framework's directory design is
// deliberately different (docs/deviations.md: Kubernetes is the membership
// authority, not a gossip-based ClusterMembershipService) and has no
// per-grain-type directory resolver, no distinct caching layer class, and no
// silo-lifecycle-stage participation hook to drive — DistributedGrainDirectory
// (packages/directory/src/distributed-grain-directory.ts) folds
// caching/lookup/registration into one class over a single consistent-hash
// ring, with membership fed by Kubernetes EndpointSlice watches rather than a
// mockable gossip service. None of these upstream tests have a faithful
// equivalent to port.
const NS = "UnitTests.Directory.CachedGrainLocatorTests";
const REASON =
  ".NET-specific mechanism: exercises CachedGrainLocator composed from " +
  "NSubstitute mocks of IGrainDirectory/MockClusterMembershipService and a " +
  "SiloLifecycleSubject stage-ordered startup — this framework has no " +
  "per-grain-type directory resolver, no distinct grain-locator caching " +
  "class, and no gossip-based ClusterMembershipService to substitute for " +
  "(membership comes from Kubernetes EndpointSlice watches; see " +
  "docs/deviations.md).";

describe(NS, () => {
  orleansTest.excluded(REASON, `${NS}.RegisterWhenNoOtherEntryExists`);
  orleansTest.excluded(REASON, `${NS}.RegisterWhenOtherEntryExists`);
  orleansTest.excluded(REASON, `${NS}.RegisterWhenOtherEntryExistsAndPreviousAddressMatches`);
  orleansTest.excluded(REASON, `${NS}.RegisterWhenOtherEntryExistsAndPreviousAddressDoesNotMatch`);
  orleansTest.excluded(REASON, `${NS}.RegisterWhenOtherEntryExistsButSiloIsDead`);
  orleansTest.excluded(REASON, `${NS}.LookupPopulateTheCache`);
  orleansTest.excluded(REASON, `${NS}.LookupWhenEntryExistsButSiloIsDead`);
  orleansTest.excluded(REASON, `${NS}.LocalLookupWhenEntryExistsButSiloIsDead`);
  orleansTest.excluded(REASON, `${NS}.CleanupWhenSiloIsDead`);
  orleansTest.excluded(REASON, `${NS}.UnregisterCallDirectoryAndCleanCache`);
  orleansTest.excluded(REASON, `${NS}.UnregisterRemovesFromCacheFirst`);
  orleansTest.excluded(REASON, `${NS}.UnregisterRacesWithLookupSameId`);
  orleansTest.excluded(REASON, `${NS}.UnregisterRacesWithLookupDifferentId`);

  // vitest requires at least one runtime test per file; all upstream Facts are
  // excluded above, so this placeholder keeps the file a valid suite.
  it.skip("(all tests in this file are orleansTest.excluded — see above)", () => undefined);
});
