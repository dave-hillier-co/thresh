// Ported from dotnet/orleans test/Orleans.Runtime.Tests/StatelessWorkerActivationTests.cs @ v10.1.0 (MIT).
// All three tests depend on enforced [StatelessWorker] scaling (multiple
// concurrent activations up to MaxLocalWorkers, collected back down when
// idle) and on IManagementGrain.GetGrainActivationCount /
// ForceActivationCollection to observe it. This framework parses
// StatelessWorker placement but does not enforce multi-activation scaling
// (GAP-STATELESS-WORKER), and has no management grain surface
// (GAP-MGMT-GRAIN) to query activation counts.
import { describe } from "vitest";
import { orleansTest } from "@tsva/testing/orleans-test";

describe("UnitTests.General.StatelessWorkerActivationTests", () => {
  const testClass = "UnitTests.General.StatelessWorkerActivationTests";

  orleansTest.gap("GAP-STATELESS-WORKER", `${testClass}.SingleWorkerInvocationUnderLoad`);
  orleansTest.gap("GAP-STATELESS-WORKER", `${testClass}.MultipleWorkerInvocationUnderLoad`);
  orleansTest.gap("GAP-STATELESS-WORKER", `${testClass}.CatalogCleanupOnDeactivation`);
});
