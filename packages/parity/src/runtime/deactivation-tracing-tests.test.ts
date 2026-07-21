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
// Four cases are EXCLUDED (`orleansTest.excluded`, not `orleansTest.gap` —
// see each call site for its precise reason):
// `OnDeactivateSpanIsNotCreatedForNonGrainBaseGrain` (every grain here
// extends the same `Grain` base — this framework has no analogue of Orleans'
// "implements IGrainBase directly, not via the `Grain` base class"
// distinction, so the negative case is untestable);
// `OnDeactivateSpanIsParentedToAsyncEnumerableMethodCall` (no IAsyncEnumerable
// grain-method span story); `OnDeactivateSpanIsCreatedForInconsistentStateException`
// (no auto-deactivate-on-`InconsistentStateException` mechanism); and
// `OnDeactivateSpanIsCreatedForGrainContextDeactivate` (no
// `GrainContext.Deactivate(reason)` grain-facing API — only
// `deactivateOnIdle()`/`IGrainManagementExtension`, which carries a fixed
// reason, exists).
//
// The two migration cases, `OnDeactivateSpanPrecedesDehydrateDuringMigration`
// and `OnDeactivateSpanHasCorrectReasonTagForMigration`, ARE now closeable:
// this framework's periodic `ActivationCollector` fires its sweep off the
// SAME `TimeProvider` a test advances (`FakeTimeProvider.advance`), and that
// advance call synchronously walks into the sweep's `collectIdle` →
// `collectOne` → `runDeactivateHook`/`dehydrate` chain, entirely as ordinary
// async/await continuations — no new `setTimeout`/timer boundary is crossed.
// `AsyncLocalStorageContextManager` (the harness's OTel context manager)
// propagates through that continuation exactly like any other awaited call,
// so driving `time.advance()` from inside `harness.withParentSpan(...)`
// (see the second `describe` block below) DOES carry the test's trace id
// into the resulting `OnDeactivate`/`Dehydrate` spans — the collection sweep
// only looks "detached" at the call-site (`forceMigrationSweep` in
// `migration-tests.test.ts`), not at the trace-context level. `collectOne`
// itself was reordered to run `runDeactivateHook` BEFORE
// `dehydrate`/hand-off (previously the reverse, to protect against an
// `onDeactivate` hook clearing state before dehydration) — matching upstream
// `ActivationData.FinishDeactivating`, which always calls
// `OnDeactivateAsync` before `OnDehydrate`, and is what
// `OnDeactivateSpanPrecedesDehydrateDuringMigration` asserts.
import { afterAll, beforeAll, beforeEach, describe, expect } from "vitest";
import { SpanStatusCode } from "@opentelemetry/api";
import { FakeTimeProvider } from "@tsva/core/test-support/fake-time-provider";
import { castGrainReference } from "@tsva/core/grain-reference";
import { GrainId } from "@tsva/core/grain-id";
import { getGrainMetadata } from "@tsva/core/grain-metadata";
import { ActivityNames } from "@tsva/observability/activation-tracing";
import { tracingFilters } from "@tsva/observability/tracing";
import { IGrainManagementExtension } from "@tsva/runtime/grain-management-extension";
import { orleansTest } from "@tsva/testing/orleans-test";
import { TestCluster, type TestSiloHandle } from "@tsva/testing/test-cluster";
import { waitFor } from "@tsva/testing/wait";
import type { ClientNode } from "@tsva/client/client-node";
import {
  ActivationFailureDeactivationGrain,
  DeactivationMigrationTracingTestGrain,
  DeactivationTracingTestGrain,
  DeactivationWithExceptionTracingTestGrain,
  DeactivationWithWorkTracingTestGrain,
  IActivationFailureDeactivationGrain,
  IDeactivationMigrationTracingTestGrain,
  IDeactivationTracingTestGrain,
  IDeactivationWithExceptionTracingTestGrain,
  IDeactivationWithWorkTracingTestGrain,
} from "@tsva/parity/grains/impl/deactivation-tracing-grain";
import { createClusterClient } from "@tsva/parity/support/client";
import { randomIntegerKey } from "@tsva/parity/support/keys";
import { createTracingHarness } from "@tsva/parity/support/tracing";

function hostOf(cluster: TestCluster, grainId: GrainId): TestSiloHandle | undefined {
  return cluster.silos.find((s) => s.host.isActive(grainId));
}

// Mirrors `migration-tests.test.ts`'s `forceMigrationSweep`: fires the
// collection sweep well past the 1s `collectionAgeSeconds` the migration
// grain below is registered with.
async function forceMigrationSweep(time: FakeTimeProvider): Promise<void> {
  time.advance(65_000);
  await new Promise((resolve) => setTimeout(resolve, 0));
}

// Shared across BOTH `describe` blocks below (not one per block): the OTel
// global tracer provider/context manager registration in
// `createTracingHarness` is a process-wide singleton
// (`trace.setGlobalTracerProvider` is a no-op, with a warning, if a provider
// is already registered) — a second `createTracingHarness()` call would
// silently keep tracing spans flowing to the FIRST harness's exporter while
// the second harness's `finishedSpans()` stayed permanently empty.
const harness = createTracingHarness();

describe("UnitTests.General.DeactivationTracingTests", () => {
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
      expect(storageWriteSpans[0]!.attributes["orleans.storage.provider"]).toBe(
        "MemoryGrainStorage",
      );
    },
  );

  orleansTest(
    "UnitTests.General.DeactivationTracingTests.OnDeactivateSpanCapturesExceptionDuringDeactivation",
    async () => {
      const grain = client.getGrain(IDeactivationWithExceptionTracingTestGrain, randomIntegerKey());
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

  orleansTest.excluded(
    "every grain in this port extends the same `Grain` base class — there is " +
      "no analogue of upstream's distinction between a grain implementing " +
      "`IGrainBase` directly (via the `Grain` base class, which unconditionally " +
      "wraps `OnDeactivateAsync` in an `OnDeactivate` span) versus one that " +
      "doesn't (no span). The negative case this test asserts — no span for a " +
      "non-`Grain`-base grain — is architecturally untestable here",
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

  orleansTest.excluded(
    "no IAsyncEnumerable grain-method span story in this port (no session/" +
      "StartEnumeration/MoveNext/DisposeAsync span taxonomy) — the same gap " +
      "`AsyncEnumerableSpansAreCreatedForMultipleElements` in " +
      "activation-tracing-tests.test.ts documents; there is no async-enumerable " +
      "method call for an `OnDeactivate` span to be parented under",
    "UnitTests.General.DeactivationTracingTests.OnDeactivateSpanIsParentedToAsyncEnumerableMethodCall",
  );
  orleansTest.excluded(
    "no auto-deactivate-on-`InconsistentStateException` mechanism in this " +
      "port — deactivation only happens via `deactivateOnIdle()`/collection, " +
      "never triggered automatically by a specific application exception type",
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

  orleansTest.excluded(
    "no `GrainContext.Deactivate(reason)` grain-facing API in this port — " +
      "only `deactivateOnIdle()`/`IGrainManagementExtension`, which always " +
      "carries a fixed `ApplicationRequested` reason, so a grain cannot " +
      "trigger deactivation with an arbitrary custom reason the way this test " +
      "requires",
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

describe("UnitTests.General.DeactivationTracingTests (migration)", () => {
  let cluster: TestCluster;
  let time: FakeTimeProvider;

  beforeAll(async () => {
    time = new FakeTimeProvider();
    cluster = await TestCluster.start({
      initialSilos: 2,
      time,
      grains: [
        {
          ctor: DeactivationMigrationTracingTestGrain,
          interfaces: [IDeactivationMigrationTracingTestGrain],
        },
      ],
      configureSilo: (builder) => builder.useTracing(),
    });
  });

  beforeEach(() => harness.reset());

  afterAll(async () => {
    await cluster.dispose();
  });

  orleansTest(
    "UnitTests.General.DeactivationTracingTests.OnDeactivateSpanPrecedesDehydrateDuringMigration",
    async () => {
      const key = randomIntegerKey();
      const grainType = getGrainMetadata(DeactivationMigrationTracingTestGrain)!.grainType;
      const grainId = new GrainId(grainType, key);
      const grain = cluster.getGrain(IDeactivationMigrationTracingTestGrain, key);
      await grain.setState(42);
      const originalHost = hostOf(cluster, grainId);
      const targetHost = cluster.silos.find((s) => s !== originalHost)!;

      const { traceId: testParentTraceId } = await harness.withParentSpan(
        "test-parent-deactivate-migrate",
        async () => {
          await grain.migrateOnIdle(targetHost.address);
          await forceMigrationSweep(time);
          await waitFor(() => hostOf(cluster, grainId) === targetHost);
        },
      );

      expect(await grain.getState()).toBe(42);

      // Filtered to THIS test's grain id: the shared cluster/clock (mirroring
      // `migration-tests.test.ts`) means a PRIOR test's now-idle activation
      // can also get swept in the same collection pass, emitting its own
      // (unrelated, non-migration) `OnDeactivate` span.
      const spans = harness
        .finishedSpans()
        .filter((s) => s.attributes["orleans.grain.id"] === grainId.toString());
      const onDeactivateSpans = spans.filter((s) => s.name === ActivityNames.OnDeactivate);
      expect(onDeactivateSpans.length).toBeGreaterThan(0);
      const dehydrateSpans = spans.filter((s) => s.name === ActivityNames.Dehydrate);
      expect(dehydrateSpans.length).toBeGreaterThan(0);

      const onDeactivateSpan = onDeactivateSpans[0]!;
      const dehydrateSpan = dehydrateSpans[0]!;
      expect(onDeactivateSpan.startTimeMs).toBeLessThanOrEqual(dehydrateSpan.startTimeMs);

      expect(onDeactivateSpan.attributes["orleans.grain.id"]).toBeDefined();
      expect(onDeactivateSpan.attributes["orleans.deactivation.reason"] as string).toContain(
        "Migrating",
      );

      expect(dehydrateSpan.attributes["orleans.grain.id"]).toBeDefined();
      expect(dehydrateSpan.attributes["orleans.migration.target.silo"]).toBe(
        targetHost.address.toString(),
      );

      expect(onDeactivateSpan.traceId).toBe(testParentTraceId);
      expect(dehydrateSpan.traceId).toBe(testParentTraceId);
    },
  );

  orleansTest(
    "UnitTests.General.DeactivationTracingTests.OnDeactivateSpanHasCorrectReasonTagForMigration",
    async () => {
      const key = randomIntegerKey();
      const grainType = getGrainMetadata(DeactivationMigrationTracingTestGrain)!.grainType;
      const grainId = new GrainId(grainType, key);
      const grain = cluster.getGrain(IDeactivationMigrationTracingTestGrain, key);
      await grain.setState(42);
      const originalHost = hostOf(cluster, grainId);
      const targetHost = cluster.silos.find((s) => s !== originalHost)!;

      const { traceId: testParentTraceId } = await harness.withParentSpan(
        "test-parent-reason-migration",
        async () => {
          await grain.migrateOnIdle(targetHost.address);
          await forceMigrationSweep(time);
          await waitFor(() => hostOf(cluster, grainId) === targetHost);
        },
      );

      const spans = harness
        .finishedSpans()
        .filter((s) => s.attributes["orleans.grain.id"] === grainId.toString());
      const onDeactivateSpans = spans.filter((s) => s.name === ActivityNames.OnDeactivate);
      expect(onDeactivateSpans.length).toBeGreaterThan(0);
      const onDeactivateSpan = onDeactivateSpans[0]!;

      const deactivationReasonTag = onDeactivateSpan.attributes["orleans.deactivation.reason"];
      expect(deactivationReasonTag).toBeDefined();
      expect(deactivationReasonTag as string).toContain("Migrating");

      expect(onDeactivateSpan.traceId).toBe(testParentTraceId);
    },
  );
});

afterAll(() => harness.teardown());
