// Ported from dotnet/orleans test/Orleans.Core.Tests/Directory/RingRangeCollectionTests.cs @ v10.1.0 (MIT).
import { describe, it } from "vitest";
import { orleansTest } from "@tsva/testing/orleans-test";

// RingRangeCollection is a union-of-arcs value type over Orleans' RingRange
// (Contains, Intersects, Difference, ordered-arc containment with wraparound),
// exercised via CsCheck property-based generators. This framework's
// consistent-hash ring (packages/directory/src/consistent-hash-ring.ts)
// exposes ownership as `ownerOf(hash)` and reminder-partitioning arcs as
// plain `[begin, end]` tuples from `rangesFor(silo)` — it has no RingRange or
// RingRangeCollection value type at all, so there is no Contains/Intersects/
// Difference/union arithmetic to port a faithful equivalent of these tests
// against.
const NS = "NonSilo.Tests.Directory.RingRangeCollectionTests";
const REASON =
  ".NET-specific mechanism: exercises RingRangeCollection's union-of-arcs " +
  "arithmetic (Contains, Intersects, Difference) via CsCheck property " +
  "generators — this framework has no RingRange/RingRangeCollection value " +
  "type at all; ConsistentHashRing resolves ownership directly via " +
  "`ownerOf(hash)` and exposes reminder-partition arcs as plain " +
  "`[begin, end]` tuples with no Contains/Intersects/Difference API.";

describe(NS, () => {
  orleansTest.excluded(REASON, `${NS}.Contains`);
  orleansTest.excluded(REASON, `${NS}.Intersects`);
  orleansTest.excluded(REASON, `${NS}.Difference`);
  orleansTest.excluded(REASON, `${NS}.ContainsTest`);
  orleansTest.excluded(REASON, `${NS}.ContainsWrappedTest`);

  // vitest requires at least one runtime test per file; all upstream Facts are
  // excluded above, so this placeholder keeps the file a valid suite.
  it.skip("(all tests in this file are orleansTest.excluded — see above)", () => undefined);
});
