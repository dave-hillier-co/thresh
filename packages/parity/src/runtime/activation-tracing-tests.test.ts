// Ported from dotnet/orleans test/Orleans.Runtime.Tests/ActivationTracingTests.cs @ v10.1.0 (MIT).
// Every case asserts on `System.Diagnostics.Activity`/`ActivitySource` spans
// (OpenTelemetry-compatible) created around grain activation, grain-call
// filters, persistent-state reads, migration dehydrate/rehydrate, and
// IAsyncEnumerable streaming — via a custom `ActivityListener` attached to
// `ActivitySources.ApplicationGrainActivitySourceName`.
//
// This framework now has the ACTIVATION-path span taxonomy
// (`@tsva/observability/activation-tracing`: `place grain`/`activate grain`/
// `register directory entry`/`read storage`, the Runtime/Lifecycle/Storage
// source analogues), wired so `ClusterNode.receiveRequest` extracts the
// incoming `traceparent` BEFORE placement/activation run
// (`DistributedDispatcher.deliverLocal` → `claimAndActivateLocally`), not
// just around method dispatch — so a first call that triggers activation
// shares its trace id with the caller. Two cases below are ported on that
// basis. The rest still need a placement-FILTER system
// (`FilterPlacementCandidates`/`IPlacementFilterDirector`, GAP-PLACEMENT-
// FILTER-DIRECTORS) or grain migration dehydrate/rehydrate spans — out of
// scope here — and stay GAP-TRACING.
import { afterAll, beforeAll, beforeEach, describe, expect } from "vitest";
import { getGrainMetadata } from "@tsva/core/grain-metadata";
import { ActivityNames } from "@tsva/observability/activation-tracing";
import { tracingFilters } from "@tsva/observability/tracing";
import { orleansTest } from "@tsva/testing/orleans-test";
import { TestCluster } from "@tsva/testing/test-cluster";
import type { ClientNode } from "@tsva/client/client-node";
import {
  ActivityGrain,
  IActivityGrain,
  IPersistentStateActivityGrain,
  PersistentStateActivityGrain,
} from "@tsva/parity/grains/impl/activation-tracing-grain";
import { createClusterClient } from "@tsva/parity/support/client";
import { randomIntegerKey } from "@tsva/parity/support/keys";
import { createTracingHarness } from "@tsva/parity/support/tracing";

describe("UnitTests.General.ActivationTracingTests", () => {
  const harness = createTracingHarness();
  let cluster: TestCluster;
  let client: ClientNode;

  beforeAll(async () => {
    cluster = await TestCluster.start({
      initialSilos: 1,
      grains: [
        { ctor: ActivityGrain, interfaces: [IActivityGrain] },
        { ctor: PersistentStateActivityGrain, interfaces: [IPersistentStateActivityGrain] },
      ],
      configureSilo: (builder) => builder.useTracing(),
    });
    client = await createClusterClient(
      cluster,
      [
        { ctor: ActivityGrain, interfaces: [IActivityGrain] },
        { ctor: PersistentStateActivityGrain, interfaces: [IPersistentStateActivityGrain] },
      ],
      undefined,
      [tracingFilters().outgoing],
    );
  });

  beforeEach(() => harness.reset());

  afterAll(async () => {
    await client.close();
    await cluster.dispose();
    await harness.teardown();
  });

  orleansTest(
    "UnitTests.General.ActivationTracingTests.ActivationSpanIsCreatedOnFirstCall",
    async () => {
      const { traceId: testParentTraceId } = await harness.withParentSpan("test-parent", () =>
        client.getGrain(IActivityGrain, randomIntegerKey()).getActivityId(),
      );

      const spans = harness.finishedSpans();

      // Placement span present, parented to the test's trace.
      const placementSpan = spans.find((s) => s.name === ActivityNames.PlaceGrain);
      expect(placementSpan).toBeDefined();
      expect(placementSpan!.traceId).toBe(testParentTraceId);

      // No placement-filter system here: absent, not just unasserted.
      expect(spans.find((s) => s.name === ActivityNames.FilterPlacementCandidates)).toBeUndefined();

      // Activation span present, parented to the test's trace.
      const activationSpan = spans.find((s) => s.name === ActivityNames.ActivateGrain);
      expect(activationSpan).toBeDefined();
      expect(activationSpan!.traceId).toBe(testParentTraceId);

      // A plain grain (no opt-in) gets no OnActivateAsync span (matches the
      // `IFilteredActivityGrain` contrast case, which stays gapped).
      expect(spans.find((s) => s.name === ActivityNames.OnActivate)).toBeUndefined();

      // Directory-register span present, parented to the test's trace AND
      // to the activation span specifically.
      const registerSpan = spans.find((s) => s.name === ActivityNames.RegisterDirectoryEntry);
      expect(registerSpan).toBeDefined();
      expect(registerSpan!.traceId).toBe(testParentTraceId);
      expect(registerSpan!.parentSpanId).toBe(activationSpan!.spanId);
    },
  );

  orleansTest.gap(
    "GAP-TRACING",
    "UnitTests.General.ActivationTracingTests.ActivationSpanIncludesFilter",
  );
  orleansTest.gap(
    "GAP-TRACING",
    "UnitTests.General.ActivationTracingTests.ActivationSpanIncludesMultipleFilters",
  );
  orleansTest(
    "UnitTests.General.ActivationTracingTests.PersistentStateReadSpanIsCreatedDuringActivation",
    async () => {
      const grainType = getGrainMetadata(PersistentStateActivityGrain)!.grainType;
      const { traceId: testParentTraceId } = await harness.withParentSpan(
        "test-parent-storage",
        () => client.getGrain(IPersistentStateActivityGrain, randomIntegerKey()).getActivityId(),
      );

      const spans = harness.finishedSpans();

      const activationSpan = spans.find(
        (s) => s.name === ActivityNames.ActivateGrain && s.attributes["orleans.grain.type"] === grainType,
      );
      expect(activationSpan).toBeDefined();
      expect(activationSpan!.traceId).toBe(testParentTraceId);

      const storageReadSpan = spans.find((s) => s.name === ActivityNames.StorageRead);
      expect(storageReadSpan).toBeDefined();
      expect(storageReadSpan!.traceId).toBe(testParentTraceId);
      expect(storageReadSpan!.attributes["orleans.storage.provider"]).toBe("MemoryGrainStorage");
      expect(storageReadSpan!.attributes["orleans.storage.state.name"]).toBe("state");
      expect(storageReadSpan!.attributes["orleans.grain.id"]).toBeDefined();
    },
  );
  orleansTest.gap(
    "GAP-TRACING",
    "UnitTests.General.ActivationTracingTests.MigrationSpansAreCreatedDuringGrainMigration",
  );
  orleansTest.gap(
    "GAP-TRACING",
    "UnitTests.General.ActivationTracingTests.MigrationPlacementFilterSpanIsParentedUnderPlaceGrainSpan",
  );
  orleansTest.gap(
    "GAP-TRACING",
    "UnitTests.General.ActivationTracingTests.MigrationSpansAreCreatedForGrainWithPersistentState",
  );
  orleansTest.gap(
    "GAP-TRACING",
    "UnitTests.General.ActivationTracingTests.DehydrateAndRehydrateSpansAreNotCreatedForGrainWithoutMigrationParticipant",
  );
  orleansTest.gap(
    "GAP-TRACING",
    "UnitTests.General.ActivationTracingTests.AsyncEnumerableSpansAreCreatedForMultipleElements",
  );
});
