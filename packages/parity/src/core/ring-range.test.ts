// Ported from dotnet/orleans test/Orleans.Core.Tests/Directory/RingRangeTests.cs @ v10.1.0 (MIT).
import { describe, it } from "vitest";
import { orleansTest } from "@tsva/testing/orleans-test";

// RingRange is Orleans' single-arc value type over the hash ring (Create,
// Complement, Difference, Intersection, Contains, IsEmpty, IsFull), exercised
// via CsCheck property-based generators. This framework's consistent-hash
// ring (packages/directory/src/consistent-hash-ring.ts) has no RingRange
// value type at all — ownership is resolved directly by binary-searching a
// sorted vnode list (`ownerOf`/`owner`), and reminder-partitioning arcs are
// returned as plain `[begin, end]` number tuples with no Complement/
// Difference/Intersection/IsEmpty/IsFull arithmetic. There is no faithful
// equivalent of any of these tests to port.
const NS = "NonSilo.Tests.Directory.RingRangeTests";
const REASON =
  ".NET-specific mechanism: exercises RingRange's arc arithmetic (Complement, " +
  "Difference, Intersection, Contains, IsEmpty/IsFull) via CsCheck property " +
  "generators — this framework has no RingRange value type at all; " +
  "ConsistentHashRing resolves ownership by binary-searching a sorted vnode " +
  "list and returns reminder-partition arcs as plain `[begin, end]` tuples " +
  "with no Complement/Difference/Intersection/IsEmpty/IsFull API.";

describe(NS, () => {
  orleansTest.excluded(REASON, `${NS}.RingRangeDifference_EquallyDividedRange`);
  orleansTest.excluded(REASON, `${NS}.ComplementDoesNotIntersect`);
  orleansTest.excluded(REASON, `${NS}.ComplementComplementIsEqual`);
  orleansTest.excluded(REASON, `${NS}.RingRangeDifference_HolePunch`);
  orleansTest.excluded(REASON, `${NS}.RingRangeDifference_Empty`);
  orleansTest.excluded(REASON, `${NS}.RingRangeDifference_Empty_Two`);
  orleansTest.excluded(REASON, `${NS}.RingRangeIntersection`);
  orleansTest.excluded(REASON, `${NS}.RingRangeContains`);
  orleansTest.excluded(REASON, `${NS}.EqualRangeInvariants`);

  // vitest requires at least one runtime test per file; all upstream Facts are
  // excluded above, so this placeholder keeps the file a valid suite.
  it.skip("(all tests in this file are orleansTest.excluded — see above)", () => undefined);
});
