// Ported from dotnet/orleans test/Orleans.Core.Tests/Directory/DirectoryMembershipSnapshotTests.cs @ v10.1.0 (MIT).
import { describe, it } from "vitest";
import { orleansTest } from "@thresh/testing/orleans-test";

// DirectoryMembershipSnapshot is Orleans' per-membership-version view of ring
// ownership, exposing RingRange/RingRangeCollection arithmetic (Intersects,
// Size, SizePercent, IsFull, IsDefault) over a ClusterMembershipSnapshot and a
// configurable virtual-ring-bucket hash function, exercised via CsCheck
// property-based generators. This framework's placement ring
// (packages/directory/src/consistent-hash-ring.ts) resolves an owner per
// grain hash directly from a sorted vnode list — it has no RingRange /
// RingRangeCollection value types (no Intersects/Size/SizePercent/IsFull
// arithmetic), no ClusterMembershipSnapshot (membership comes from Kubernetes
// EndpointSlice watches, not a versioned snapshot type), and no configurable
// virtual-ring-bucket count to inject a custom hash function into. None of
// these upstream tests have a faithful equivalent to port.
const NS = "NonSilo.Tests.Directory.DirectoryMembershipSnapshotTests";
const REASON =
  ".NET-specific mechanism: exercises DirectoryMembershipSnapshot's RingRange" +
  "/RingRangeCollection arithmetic (Intersects, Size, SizePercent, IsFull) " +
  "over a versioned ClusterMembershipSnapshot with an injectable virtual-" +
  "ring-bucket hash function, via CsCheck property generators — this " +
  "framework's ConsistentHashRing has no RingRange/RingRangeCollection value " +
  "types, no ClusterMembershipSnapshot (Kubernetes EndpointSlice watches " +
  "drive membership; see docs/deviations.md), and no injectable virtual-" +
  "bucket hash function.";

describe(NS, () => {
  orleansTest.excluded(REASON, `${NS}.GetOwnerTest`);
  orleansTest.excluded(REASON, `${NS}.MembersDoNotIntersectTest`);
  orleansTest.excluded(REASON, `${NS}.ViewCoversRingTest`);
  orleansTest.excluded(REASON, `${NS}.MemberRangesCoverRingTest`);

  // vitest requires at least one runtime test per file; all upstream Facts are
  // excluded above, so this placeholder keeps the file a valid suite.
  it.skip("(all tests in this file are orleansTest.excluded — see above)", () => undefined);
});
