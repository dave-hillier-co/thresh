// Ported from dotnet/orleans test/Orleans.Journaling.Tests/DurableGrainTests.cs @ v10.1.0 (MIT).
//
// Every test here forces a fresh grain activation via
// `grain.Cast<IGrainManagementExtension>().DeactivateOnIdle()`, then asserts
// `GetActivationId()` changed before re-checking durable state. That grain
// extension mechanism (Cast<T> onto a system-provided IGrainExtension) has no
// equivalent in this framework (GAP-GRAIN-EXTENSION: "no IGrainExtension
// equivalent") — there is no grain-reference-visible way to force a specific
// grain's deactivation the way Orleans does here, only a whole-silo restart
// (TestCluster.restartSilo), which is not the same operation upstream is
// exercising (it deliberately keeps other grains/activations untouched).
import { describe } from "vitest";
import { orleansTest } from "@tsva/testing/orleans-test";

describe("Orleans.Journaling.Tests.DurableGrainTests", () => {
  orleansTest.gap(
    "GAP-GRAIN-EXTENSION",
    "Orleans.Journaling.Tests.DurableGrainTests.DurableGrain_State_Persistence_Test",
  );
  orleansTest.gap(
    "GAP-GRAIN-EXTENSION",
    "Orleans.Journaling.Tests.DurableGrainTests.DurableGrain_Update_State_Test",
  );
  orleansTest.gap(
    "GAP-GRAIN-EXTENSION",
    "Orleans.Journaling.Tests.DurableGrainTests.DurableGrain_Complex_Types_Test",
  );
  orleansTest.gap(
    "GAP-GRAIN-EXTENSION",
    "Orleans.Journaling.Tests.DurableGrainTests.DurableGrain_Multiple_Collections_Test",
  );
  orleansTest.gap(
    "GAP-GRAIN-EXTENSION",
    "Orleans.Journaling.Tests.DurableGrainTests.DurableGrain_State_Modifications_Test",
  );
  orleansTest.gap(
    "GAP-GRAIN-EXTENSION",
    "Orleans.Journaling.Tests.DurableGrainTests.Grain_State_Should_Persist_Between_Activations",
  );
  orleansTest.gap(
    "GAP-GRAIN-EXTENSION",
    "Orleans.Journaling.Tests.DurableGrainTests.Grain_Should_Handle_Multiple_Collections",
  );
  orleansTest.gap(
    "GAP-GRAIN-EXTENSION",
    "Orleans.Journaling.Tests.DurableGrainTests.Grain_Should_Handle_Large_State",
  );
});
