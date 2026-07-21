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
// basis. It also now has the MIGRATION dehydrate/rehydrate spans
// (`withDehydrateSpan`/`withRehydrateSpan`, `ActivationData.dehydrate`/
// `applyRehydration`), emitted ONLY when the migrating activation has at
// least one migration participant (`migrationParticipantsOf`) — see the
// second `describe` block below, which drives migration directly via
// `TestCluster` (no wire `ClientNode`, matching `migration-tests.test.ts` —
// trace context still propagates in-process via `AsyncLocalStorage`
// regardless of whether a call crosses the wire client).
//
// The placement-FILTER system (`PlacementFilterRegistry`/`PlacementFilter`,
// GAP-PLACEMENT-FILTER-DIRECTORS) now exists too, and `PlaceGrain`
// (`DistributedDispatcher.placeAndInvoke`) / migration's own placement
// decision (`ClusterNode.migrateActivation`) both wrap their filter loop in
// `FilterPlacementCandidates` spans nested under `PlaceGrain` — closing
// `MigrationPlacementFilterSpanIsParentedUnderPlaceGrainSpan` below. The two
// `ActivationSpanIncludesFilter*` cases stay unportable for a DIFFERENT
// reason (see their `orleansTest.excluded` calls): they also require an
// `OnActivate` span present for a filtered grain but absent for a plain one,
// which needs upstream's `Grain`-base-vs-plain-implementer (`IGrainBase`)
// distinction — this port has no analogue (every grain extends the same
// `Grain` base), the same reasoning already established for
// `OnDeactivateSpanIsNotCreatedForNonGrainBaseGrain` in
// deactivation-tracing-tests.test.ts. The IAsyncEnumerable case stays
// excluded too — out of scope (no session/span taxonomy for streamed
// grain-call results).
//
// `MigrationSpansAreCreatedForGrainWithPersistentState` (upstream): despite
// its name, upstream's grain body has NO `IGrainMigrationParticipant` and the
// assertions actually check dehydrate/rehydrate spans are ABSENT — upstream's
// `IPersistentState<T>` does not itself implement the migration hooks. This
// port's `PersistentStateImpl` always does (every persistent-state facet
// auto-participates in migration, moving its value in-memory instead of
// falling back to a storage round-trip on the target — a deliberate,
// documented simplification, not a gap). So here the SAME scenario (a grain
// with only persistent state, no explicit participant) is genuinely a
// positive case matching the title: dehydrate/rehydrate spans ARE created,
// via the state facet.
import { afterAll, beforeAll, beforeEach, describe, expect } from "vitest";
import { FakeTimeProvider } from "@tsva/core/test-support/fake-time-provider";
import { getGrainMetadata } from "@tsva/core/grain-metadata";
import { ActivityNames } from "@tsva/observability/activation-tracing";
import { tracingFilters } from "@tsva/observability/tracing";
import { orleansTest } from "@tsva/testing/orleans-test";
import { TestCluster, type TestSiloHandle } from "@tsva/testing/test-cluster";
import { waitFor } from "@tsva/testing/wait";
import type { ClientNode } from "@tsva/client/client-node";
import {
  ActivityGrain,
  IActivityGrain,
  IPersistentStateActivityGrain,
  PersistentStateActivityGrain,
} from "@tsva/parity/grains/impl/activation-tracing-grain";
import {
  IMigrationTestGrain,
  IMigrationTestGrainGrainOfT,
  MigrationTestGrain,
  MigrationTestGrainWithMemoryStorage,
} from "@tsva/parity/grains/impl/migration-test-grain";
import {
  IMigrationFilterTracingGrain,
  ISimpleMigrationTracingGrain,
  MIGRATION_TRACING_TEST_PLACEMENT_FILTER,
  MigrationFilterTracingGrain,
  SimpleMigrationTracingGrain,
  TracingTestPlacementFilterStrategy,
} from "@tsva/parity/grains/impl/migration-tracing-grain";
import { createClusterClient } from "@tsva/parity/support/client";
import { randomIntegerKey } from "@tsva/parity/support/keys";
import { createTracingHarness } from "@tsva/parity/support/tracing";
import { GrainId } from "@tsva/core/grain-id";

function hostOf(cluster: TestCluster, grainId: GrainId): TestSiloHandle | undefined {
  return cluster.silos.find((s) => s.host.isActive(grainId));
}

// Fires the collection sweep (default interval 60s) well past the 1s
// `collectionAgeSeconds` every migration-tracing grain below is registered
// with (mirrors `migration-tests.test.ts`'s `forceMigrationSweep`).
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

describe("UnitTests.General.ActivationTracingTests", () => {
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

  orleansTest.excluded(
    "requires an `OnActivate` span present for a placement-filtered grain " +
      "but ABSENT for a plain one — upstream's `FilteredActivityGrain` " +
      "extends `Grain` (`IGrainBase`, unconditionally spanned) while " +
      "`ActivityGrain` (the plain-grain contrast case in " +
      "`ActivationSpanIsCreatedOnFirstCall`, ported above) implements its " +
      "interface directly with no `Grain` base. Every grain in this port " +
      "extends the same `Grain` base class — there is no such distinction to " +
      "hook the span's presence on, so it would have to be either always " +
      "present or always absent, breaking one assertion or the other; see the " +
      "identical reasoning for " +
      "`OnDeactivateSpanIsNotCreatedForNonGrainBaseGrain` in " +
      "deactivation-tracing-tests.test.ts",
    "UnitTests.General.ActivationTracingTests.ActivationSpanIncludesFilter",
  );
  orleansTest.excluded(
    "same `OnActivate`-span-presence architectural gap as " +
      "`ActivationSpanIncludesFilter` above, plus it asserts TWO " +
      "`FilterPlacementCandidates` spans (one per stacked filter) each " +
      "parented directly to the `PlaceGrain` span — moot without the " +
      "`OnActivate` span half of the assertion also being portable",
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
        (s) =>
          s.name === ActivityNames.ActivateGrain &&
          s.attributes["orleans.grain.type"] === grainType,
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
  orleansTest.excluded(
    "no IAsyncEnumerable grain-method span story in this port (no session/" +
      "StartEnumeration/MoveNext/DisposeAsync span taxonomy around streamed " +
      "grain-call results) — out of scope for the activation/deactivation/" +
      "placement span taxonomy this GAP-TRACING closure targets",
    "UnitTests.General.ActivationTracingTests.AsyncEnumerableSpansAreCreatedForMultipleElements",
  );
});

describe("UnitTests.General.ActivationTracingTests (migration)", () => {
  let cluster: TestCluster;
  let time: FakeTimeProvider;

  beforeAll(async () => {
    time = new FakeTimeProvider();
    cluster = await TestCluster.start({
      initialSilos: 2,
      time,
      grains: [
        { ctor: MigrationTestGrain, interfaces: [IMigrationTestGrain] },
        { ctor: MigrationTestGrainWithMemoryStorage, interfaces: [IMigrationTestGrainGrainOfT] },
        { ctor: SimpleMigrationTracingGrain, interfaces: [ISimpleMigrationTracingGrain] },
        { ctor: MigrationFilterTracingGrain, interfaces: [IMigrationFilterTracingGrain] },
      ],
      configureSilo: (builder) =>
        builder
          .useTracing()
          .addPlacementFilter(
            MIGRATION_TRACING_TEST_PLACEMENT_FILTER,
            new TracingTestPlacementFilterStrategy(),
          ),
    });
  });

  beforeEach(() => harness.reset());

  afterAll(async () => {
    await cluster.dispose();
  });

  orleansTest(
    "UnitTests.General.ActivationTracingTests.MigrationSpansAreCreatedDuringGrainMigration",
    async () => {
      const key = randomIntegerKey();
      const grainId = new GrainId(getGrainMetadata(MigrationTestGrain)!.grainType, key);
      const grain = cluster.getGrain(IMigrationTestGrain, key);
      await grain.setState(42);
      const originalHost = hostOf(cluster, grainId);
      const targetHost = cluster.silos.find((s) => s !== originalHost)!;

      const { traceId: testParentTraceId } = await harness.withParentSpan(
        "test-parent-migration",
        async () => {
          await grain.migrateOnIdle(targetHost.address);
          await forceMigrationSweep(time);
          await waitFor(() => hostOf(cluster, grainId) === targetHost);
        },
      );

      expect(await grain.getState()).toBe(42);

      const spans = harness.finishedSpans();

      const dehydrateSpan = spans.find((s) => s.name === ActivityNames.Dehydrate);
      expect(dehydrateSpan).toBeDefined();
      expect(dehydrateSpan!.attributes["orleans.grain.id"]).toBeDefined();
      expect(dehydrateSpan!.attributes["orleans.silo.id"]).toBeDefined();
      expect(dehydrateSpan!.attributes["orleans.activation.id"]).toBeDefined();
      expect(dehydrateSpan!.attributes["orleans.migration.target.silo"]).toBe(
        targetHost.address.toString(),
      );
      expect(dehydrateSpan!.traceId).toBe(testParentTraceId);

      const rehydrateSpan = spans.find((s) => s.name === ActivityNames.Rehydrate);
      expect(rehydrateSpan).toBeDefined();
      expect(rehydrateSpan!.attributes["orleans.grain.id"]).toBeDefined();
      expect(rehydrateSpan!.attributes["orleans.silo.id"]).toBeDefined();
      expect(rehydrateSpan!.attributes["orleans.activation.id"]).toBeDefined();
      expect(rehydrateSpan!.attributes["orleans.rehydrate.previousRegistration"]).toBeDefined();
      expect(rehydrateSpan!.traceId).toBe(testParentTraceId);
    },
  );

  orleansTest(
    "UnitTests.General.ActivationTracingTests.MigrationSpansAreCreatedForGrainWithPersistentState",
    async () => {
      const key = randomIntegerKey();
      const grainId = new GrainId(
        getGrainMetadata(MigrationTestGrainWithMemoryStorage)!.grainType,
        key,
      );
      const grain = cluster.getGrain(IMigrationTestGrainGrainOfT, key);
      await grain.setState(7);
      const originalHost = hostOf(cluster, grainId);
      const targetHost = cluster.silos.find((s) => s !== originalHost)!;

      await grain.migrateOnIdle(targetHost.address);
      await forceMigrationSweep(time);
      await waitFor(() => hostOf(cluster, grainId) === targetHost);

      expect(await grain.getState()).toBe(7);

      const spans = harness.finishedSpans();
      expect(spans.filter((s) => s.name === ActivityNames.Dehydrate).length).toBeGreaterThan(0);
      expect(spans.filter((s) => s.name === ActivityNames.Rehydrate).length).toBeGreaterThan(0);

      // The state facet's value moved in-memory via the migration bag — no
      // storage round-trip on the target (unlike a fresh activation), so
      // exactly one storage read happened total: the original activation's.
      expect(spans.filter((s) => s.name === ActivityNames.StorageRead).length).toBe(1);
    },
  );

  orleansTest(
    "UnitTests.General.ActivationTracingTests.DehydrateAndRehydrateSpansAreNotCreatedForGrainWithoutMigrationParticipant",
    async () => {
      const key = randomIntegerKey();
      const grainId = new GrainId(getGrainMetadata(SimpleMigrationTracingGrain)!.grainType, key);
      const grain = cluster.getGrain(ISimpleMigrationTracingGrain, key);
      await grain.setState(99);
      const originalHost = hostOf(cluster, grainId);
      const targetHost = cluster.silos.find((s) => s !== originalHost)!;

      await grain.migrateOnIdle(targetHost.address);
      await forceMigrationSweep(time);
      await waitFor(() => hostOf(cluster, grainId) === targetHost);

      // No migration participant: state is lost, but the grain is still
      // servable on the new host (a fresh reactivation).
      await grain.getState();

      const spans = harness.finishedSpans();
      expect(spans.filter((s) => s.name === ActivityNames.Dehydrate).length).toBe(0);
      expect(spans.filter((s) => s.name === ActivityNames.Rehydrate).length).toBe(0);
    },
  );

  orleansTest(
    "UnitTests.General.ActivationTracingTests.MigrationPlacementFilterSpanIsParentedUnderPlaceGrainSpan",
    async () => {
      const key = randomIntegerKey();
      const grainId = new GrainId(getGrainMetadata(MigrationFilterTracingGrain)!.grainType, key);
      const grain = cluster.getGrain(IMigrationFilterTracingGrain, key);
      await grain.setState(1);
      const originalHost = hostOf(cluster, grainId);
      const targetHost = cluster.silos.find((s) => s !== originalHost)!;

      // Clear activation-time spans: focus on the placement span migration's
      // `PlaceGrainAsync`-equivalent (`ClusterNode.migrateActivation`) creates.
      harness.reset();

      const { traceId: testParentTraceId } = await harness.withParentSpan(
        "test-parent-migration-filter",
        async () => {
          await grain.migrateOnIdle(targetHost.address);
          await forceMigrationSweep(time);
          await waitFor(() => hostOf(cluster, grainId) === targetHost);
        },
      );

      const spans = harness.finishedSpans();

      const placementSpans = spans.filter((s) => s.name === ActivityNames.PlaceGrain);
      expect(placementSpans.length).toBeGreaterThan(0);
      const placementSpan = placementSpans[0]!;
      expect(placementSpan.traceId).toBe(testParentTraceId);

      const filterSpans = spans.filter((s) => s.name === ActivityNames.FilterPlacementCandidates);
      expect(filterSpans.length).toBeGreaterThan(0);
      const filterSpan = filterSpans[0]!;
      expect(filterSpan.traceId).toBe(testParentTraceId);
      expect(filterSpan.attributes["orleans.placement.filter.type"]).toBe(
        "TracingTestPlacementFilterStrategy",
      );
      expect(filterSpan.parentSpanId).toBe(placementSpan.spanId);
    },
  );
});

afterAll(() => harness.teardown());
