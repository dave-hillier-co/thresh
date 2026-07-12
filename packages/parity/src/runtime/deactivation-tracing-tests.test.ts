// Ported from dotnet/orleans test/Orleans.Runtime.Tests/DeactivationTracingTests.cs @ v10.1.0 (MIT).
// Every case asserts on `System.Diagnostics.Activity` spans created around
// `OnDeactivateAsync`, deactivation-triggered storage writes, migration
// dehydrate ordering, exceptions during deactivation, and trace-context
// inheritance — same missing tracing/telemetry instrumentation as
// ActivationTracingTests.cs (GAP-TRACING).
//
// This framework now has the DEACTIVATE-path span (`OnDeactivate`, Lifecycle
// source — `@tsva/observability/activation-tracing`'s `withOnDeactivateSpan`,
// wired around `ActivationData.runDeactivateHook`'s `instance.onDeactivate`
// call) plus a `StorageWrite` span (Storage source) around
// `PersistentStateImpl.write`. Both inherit the ambient OTel context the same
// way the activation-path spans do (async-hooks context manager +
// `ClusterNode.receiveRequest`'s early trace-context extraction), so a
// deactivation triggered by (or observed from) a call under a given trace
// shares that trace id — no separate wiring needed.
//
// `deactivateOnIdle()` is lazy here (Orleans parity: flagged during the
// triggering turn, actually run on the NEXT call for that grain id — see
// `Catalog.finalizeStale`), unlike upstream's synchronous
// `HostedCluster.DeactivateAsync`. Since every case below wraps BOTH the
// triggering call and the follow-up call in the SAME `harness.withParentSpan`,
// this makes no observable difference to the trace-id assertions.
//
// Six cases stay GAP-TRACING: `OnDeactivateSpanIsNotCreatedForNonGrainBaseGrain`
// (every grain here extends the same `Grain` base — this framework has no
// analogue of Orleans' "implements IGrainBase directly, not via the `Grain`
// base class" distinction, so the negative case is untestable); the two
// migration cases, `OnDeactivateSpanPrecedesDehydrateDuringMigration` and
// `OnDeactivateSpanHasCorrectReasonTagForMigration` (migration here only runs
// off a background collection sweep — triggered by advancing a
// `FakeTimeProvider`, not from within a traced call — so it cannot inherit a
// test's ambient trace context the way upstream's synchronous
// `HostedCluster.MigrateAsync` does; no `ActivationDehydrate` span exists
// yet either); `OnDeactivateSpanIsParentedToAsyncEnumerableMethodCall` (no
// IAsyncEnumerable grain-method span story); `OnDeactivateSpanIsCreatedForInconsistentStateException`
// (no auto-deactivate-on-`InconsistentStateException` mechanism); and
// `OnDeactivateSpanIsCreatedForGrainContextDeactivate` (no
// `GrainContext.Deactivate(reason)` grain-facing API — only
// `deactivateOnIdle()`/`IGrainManagementExtension`, which carries a fixed
// reason, exists).
import { afterAll, beforeAll, beforeEach, describe, expect } from "vitest";
import { SpanStatusCode } from "@opentelemetry/api";
import { castGrainReference } from "@tsva/core/grain-reference";
import { ActivityNames } from "@tsva/observability/activation-tracing";
import { tracingFilters } from "@tsva/observability/tracing";
import { IGrainManagementExtension } from "@tsva/runtime/grain-management-extension";
import { orleansTest } from "@tsva/testing/orleans-test";
import { TestCluster } from "@tsva/testing/test-cluster";
import type { ClientNode } from "@tsva/client/client-node";
import {
  ActivationFailureDeactivationGrain,
  DeactivationTracingTestGrain,
  DeactivationWithExceptionTracingTestGrain,
  DeactivationWithWorkTracingTestGrain,
  IActivationFailureDeactivationGrain,
  IDeactivationTracingTestGrain,
  IDeactivationWithExceptionTracingTestGrain,
  IDeactivationWithWorkTracingTestGrain,
} from "@tsva/parity/grains/impl/deactivation-tracing-grain";
import { createClusterClient } from "@tsva/parity/support/client";
import { randomIntegerKey } from "@tsva/parity/support/keys";
import { createTracingHarness } from "@tsva/parity/support/tracing";

describe("UnitTests.General.DeactivationTracingTests", () => {
  const harness = createTracingHarness();
  let cluster: TestCluster;
  let client: ClientNode;

  beforeAll(async () => {
    cluster = await TestCluster.start({
      initialSilos: 1,
      grains: [
        { ctor: DeactivationTracingTestGrain, interfaces: [IDeactivationTracingTestGrain] },
        {
          ctor: DeactivationWithWorkTracingTestGrain,
          interfaces: [IDeactivationWithWorkTracingTestGrain],
        },
        {
          ctor: DeactivationWithExceptionTracingTestGrain,
          interfaces: [IDeactivationWithExceptionTracingTestGrain],
        },
        {
          ctor: ActivationFailureDeactivationGrain,
          interfaces: [IActivationFailureDeactivationGrain],
        },
      ],
      configureSilo: (builder) => builder.useTracing(),
    });
    client = await createClusterClient(
      cluster,
      [
        { ctor: DeactivationTracingTestGrain, interfaces: [IDeactivationTracingTestGrain] },
        {
          ctor: DeactivationWithWorkTracingTestGrain,
          interfaces: [IDeactivationWithWorkTracingTestGrain],
        },
        {
          ctor: DeactivationWithExceptionTracingTestGrain,
          interfaces: [IDeactivationWithExceptionTracingTestGrain],
        },
        {
          ctor: ActivationFailureDeactivationGrain,
          interfaces: [IActivationFailureDeactivationGrain],
        },
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
    "UnitTests.General.DeactivationTracingTests.OnDeactivateSpanIsCreatedOnDeactivateOnIdle",
    async () => {
      const grain = client.getGrain(IDeactivationTracingTestGrain, randomIntegerKey());
      await grain.getActivityId();

      const { traceId: testParentTraceId } = await harness.withParentSpan(
        "test-parent-deactivate",
        async () => {
          await castGrainReference(grain, IGrainManagementExtension).deactivateOnIdle();
          await grain.getActivityId();
        },
      );

      const spans = harness.finishedSpans();
      const onDeactivateSpans = spans.filter((s) => s.name === ActivityNames.OnDeactivate);
      expect(onDeactivateSpans.length).toBeGreaterThan(0);
      const onDeactivateSpan = onDeactivateSpans[0]!;

      expect(onDeactivateSpan.attributes["orleans.grain.id"]).toBeDefined();
      expect(onDeactivateSpan.attributes["orleans.grain.type"]).toBeDefined();
      expect(onDeactivateSpan.attributes["orleans.silo.id"]).toBeDefined();
      expect(onDeactivateSpan.attributes["orleans.activation.id"]).toBeDefined();

      const reasonTag = onDeactivateSpan.attributes["orleans.deactivation.reason"];
      expect(reasonTag).toBeDefined();
      expect(reasonTag as string).toContain("ApplicationRequested");

      expect(onDeactivateSpan.traceId).toBe(testParentTraceId);
    },
  );

  orleansTest(
    "UnitTests.General.DeactivationTracingTests.OnDeactivateSpanIncludesStorageWriteDuringDeactivation",
    async () => {
      const grain = client.getGrain(IDeactivationWithWorkTracingTestGrain, randomIntegerKey());
      await grain.getActivityId();

      await harness.withParentSpan("test-parent-deactivate-storage", async () => {
        await castGrainReference(grain, IGrainManagementExtension).deactivateOnIdle();
        await grain.getActivityId();
      });

      const wasDeactivated = await grain.wasDeactivated();
      expect(wasDeactivated).toBe(true);

      const spans = harness.finishedSpans();
      const onDeactivateSpans = spans.filter((s) => s.name === ActivityNames.OnDeactivate);
      expect(onDeactivateSpans.length).toBeGreaterThan(0);

      const storageWriteSpans = spans.filter((s) => s.name === ActivityNames.StorageWrite);
      expect(storageWriteSpans.length).toBeGreaterThan(0);
      expect(storageWriteSpans[0]!.attributes["orleans.storage.provider"]).toBe("MemoryGrainStorage");
    },
  );

  orleansTest(
    "UnitTests.General.DeactivationTracingTests.OnDeactivateSpanCapturesExceptionDuringDeactivation",
    async () => {
      const grain = client.getGrain(
        IDeactivationWithExceptionTracingTestGrain,
        randomIntegerKey(),
      );
      await grain.getActivityId();

      await harness.withParentSpan("test-parent-deactivate-exception", async () => {
        await castGrainReference(grain, IGrainManagementExtension).deactivateOnIdle();
        await grain.getActivityId();
      });

      const spans = harness.finishedSpans();
      const onDeactivateSpans = spans.filter((s) => s.name === ActivityNames.OnDeactivate);
      expect(onDeactivateSpans.length).toBeGreaterThan(0);
      const onDeactivateSpan = onDeactivateSpans[0]!;

      expect(onDeactivateSpan.statusCode).toBe(SpanStatusCode.ERROR);
      expect(onDeactivateSpan.statusMessage).toBe("on-deactivate-failed");
    },
  );

  orleansTest.gap(
    "GAP-TRACING",
    "UnitTests.General.DeactivationTracingTests.OnDeactivateSpanPrecedesDehydrateDuringMigration",
  );
  orleansTest.gap(
    "GAP-TRACING",
    "UnitTests.General.DeactivationTracingTests.OnDeactivateSpanIsNotCreatedForNonGrainBaseGrain",
  );

  orleansTest(
    "UnitTests.General.DeactivationTracingTests.OnDeactivateSpanInheritsTraceContextFromTriggeringCall",
    async () => {
      const grain = client.getGrain(IDeactivationTracingTestGrain, randomIntegerKey());
      await grain.getActivityId();

      const { traceId: testParentTraceId } = await harness.withParentSpan(
        "test-parent-trace-context",
        async () => {
          await castGrainReference(grain, IGrainManagementExtension).deactivateOnIdle();
          await grain.getActivityId();
        },
      );

      const spans = harness.finishedSpans();
      const onDeactivateSpans = spans.filter((s) => s.name === ActivityNames.OnDeactivate);
      expect(onDeactivateSpans.length).toBeGreaterThan(0);

      expect(onDeactivateSpans[0]!.traceId).toBe(testParentTraceId);
    },
  );

  orleansTest.gap(
    "GAP-TRACING",
    "UnitTests.General.DeactivationTracingTests.OnDeactivateSpanIsParentedToAsyncEnumerableMethodCall",
  );
  orleansTest.gap(
    "GAP-TRACING",
    "UnitTests.General.DeactivationTracingTests.OnDeactivateSpanHasCorrectReasonTagForMigration",
  );
  orleansTest.gap(
    "GAP-TRACING",
    "UnitTests.General.DeactivationTracingTests.OnDeactivateSpanIsCreatedForInconsistentStateException",
  );

  orleansTest(
    "UnitTests.General.DeactivationTracingTests.OnDeactivateSpanIsNotCreatedForActivationFailure",
    async () => {
      const grain = client.getGrain(IActivationFailureDeactivationGrain, randomIntegerKey());

      await harness.withParentSpan("test-parent-activation-failure", async () => {
        await expect(grain.getActivityId()).rejects.toThrow();
      });

      const spans = harness.finishedSpans();
      expect(spans.filter((s) => s.name === ActivityNames.OnDeactivate).length).toBe(0);
      expect(spans.filter((s) => s.name === ActivityNames.ActivateGrain).length).toBeGreaterThan(0);
    },
  );

  orleansTest.gap(
    "GAP-TRACING",
    "UnitTests.General.DeactivationTracingTests.OnDeactivateSpanIsCreatedForGrainContextDeactivate",
  );

  orleansTest(
    "UnitTests.General.DeactivationTracingTests.OnDeactivateSpanHasCorrectParentWhenTriggeredExternally",
    async () => {
      const grain = client.getGrain(IDeactivationTracingTestGrain, randomIntegerKey());
      await grain.getActivityId();

      const { traceId: testParentTraceId } = await harness.withParentSpan(
        "test-parent-external-deactivate",
        async () => {
          await castGrainReference(grain, IGrainManagementExtension).deactivateOnIdle();
          await grain.getActivityId();
        },
      );

      const spans = harness.finishedSpans();
      const onDeactivateSpans = spans.filter((s) => s.name === ActivityNames.OnDeactivate);
      expect(onDeactivateSpans.length).toBeGreaterThan(0);
      const onDeactivateSpan = onDeactivateSpans[0]!;

      expect(onDeactivateSpan.traceId).toBe(testParentTraceId);

      const reasonTag = onDeactivateSpan.attributes["orleans.deactivation.reason"];
      expect(reasonTag).toBeDefined();
      expect(reasonTag as string).toContain("ApplicationRequested");
    },
  );
});
