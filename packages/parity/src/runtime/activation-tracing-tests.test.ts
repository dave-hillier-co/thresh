// Ported from dotnet/orleans test/Orleans.Runtime.Tests/ActivationTracingTests.cs @ v10.1.0 (MIT).
// Every case asserts on `System.Diagnostics.Activity`/`ActivitySource` spans
// (OpenTelemetry-compatible) created around grain activation, grain-call
// filters, persistent-state reads, migration dehydrate/rehydrate, and
// IAsyncEnumerable streaming — via a custom `ActivityListener` attached to
// `ActivitySources.ApplicationGrainActivitySourceName`. This framework has no
// tracing/telemetry/span instrumentation at all (no ActivitySource
// equivalent, no span-emitting hooks around activation, filters, storage
// reads, or migration) — GAP-TRACING.
import { describe } from "vitest";
import { orleansTest } from "@tsva/testing/orleans-test";

describe("UnitTests.General.ActivationTracingTests", () => {
  orleansTest.gap(
    "GAP-TRACING",
    "UnitTests.General.ActivationTracingTests.ActivationSpanIsCreatedOnFirstCall",
  );
  orleansTest.gap(
    "GAP-TRACING",
    "UnitTests.General.ActivationTracingTests.ActivationSpanIncludesFilter",
  );
  orleansTest.gap(
    "GAP-TRACING",
    "UnitTests.General.ActivationTracingTests.ActivationSpanIncludesMultipleFilters",
  );
  orleansTest.gap(
    "GAP-TRACING",
    "UnitTests.General.ActivationTracingTests.PersistentStateReadSpanIsCreatedDuringActivation",
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
