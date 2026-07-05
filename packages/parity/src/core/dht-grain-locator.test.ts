// Ported from dotnet/orleans test/Orleans.Core.Tests/Directory/DhtGrainLocatorTests.cs @ v10.1.0 (MIT).
import { describe, it } from "vitest";
import { orleansTest } from "@tsva/testing/orleans-test";

// DhtGrainLocator is a thin adapter over Orleans' ILocalGrainDirectory (the
// legacy in-silo DHT directory implementation) that marshals Unregister calls
// onto a UnitTestSchedulingContext (the runtime's cooperative-scheduler work
// item abstraction) via a MockLocalGrainDirectory test double. This framework
// has neither ILocalGrainDirectory nor a scheduling-context/work-item
// abstraction to marshal onto — unregistration is a plain async method on
// GrainDirectory (packages/directory/src/grain-directory.ts), with no
// scheduler-context indirection to test.
const NS = "UnitTests.Directory.DhtGrainLocatorTests";
const REASON =
  ".NET-specific mechanism: DhtGrainLocator marshals calls onto Orleans' " +
  "cooperative-scheduler UnitTestSchedulingContext via a mocked " +
  "ILocalGrainDirectory — this framework has no scheduling-context/work-item " +
  "abstraction and no separate ILocalGrainDirectory layer; GrainDirectory." +
  "unregister is a plain async method with no scheduler indirection to test.";

describe(NS, () => {
  orleansTest.excluded(REASON, `${NS}.SingleDeactivation`);
  orleansTest.excluded(REASON, `${NS}.MultipleDeactivations`);
  orleansTest.excluded(REASON, `${NS}.MultipleMixedDeactivations`);

  // vitest requires at least one runtime test per file; all upstream Facts are
  // excluded above, so this placeholder keeps the file a valid suite.
  it.skip("(all tests in this file are orleansTest.excluded — see above)", () => undefined);
});
