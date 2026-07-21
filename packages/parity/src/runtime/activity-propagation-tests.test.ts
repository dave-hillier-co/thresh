// Ported from dotnet/orleans test/Orleans.Runtime.Tests/ActivityPropagationTests.cs @ v10.1.0 (MIT).
// Verifies `ActivityPropagationGrainCallFilter` propagates
// `System.Diagnostics.Activity` trace context (W3C and Hierarchical ID
// formats, trace state, baggage) from client to grain across grain calls,
// requiring `.AddActivityPropagation()` on both silo and client builders.
//
// This framework has the OTel analogue via
// `@tsva/observability/tracing`'s `tracingFilters()` (wired through
// `SiloBuilder.useTracing()`/`ClientConfig.outgoingCallFilters`, the same
// mechanism `grain-call-trace-context-propagation-tests.test.ts` exercises
// for pure trace-context propagation) plus `setupTracePropagation()`'s W3C
// propagator, now a `CompositePropagator` of trace-context AND baggage
// propagators — Orleans' `Activity.Baggage` analogue — so baggage entries set
// on the ambient OTel context ride the wire alongside `traceparent`/
// `tracestate` the same way `Activity.Baggage` does upstream.
//
// 2 of upstream's 3 cases are ported: `WithoutParentActivity` (no ambient
// span: grain call opens a fresh root span, no trace state) and
// `WithParentActivity_W3C` (a real parent span's trace id, trace state, and
// baggage all propagate into the grain). `WithParentActivity_Hierarchical`
// is excluded — see its `orleansTest.excluded` call below: OTel has no
// `ActivityIdFormat` concept (there is only ever the W3C shape here), and
// upstream's OWN real behaviour on .NET 10+ (the pinned parity target) for
// Hierarchical format is IDENTICAL to `WithoutParentActivity` (the W3C
// propagator can't inject a Hierarchical id, so the grain gets a fresh root
// span with no baggage) — there is no distinct format switch here to give
// that a separately meaningful port.
import { afterAll, beforeAll, beforeEach, describe, expect } from "vitest";
import { context, propagation } from "@opentelemetry/api";
import { tracingFilters } from "@tsva/observability/tracing";
import { orleansTest } from "@tsva/testing/orleans-test";
import { TestCluster } from "@tsva/testing/test-cluster";
import type { ClientNode } from "@tsva/client/client-node";
import { ActivityGrain, IActivityGrain } from "@tsva/parity/grains/impl/activation-tracing-grain";
import { createClusterClient } from "@tsva/parity/support/client";
import { randomIntegerKey } from "@tsva/parity/support/keys";
import { createTracingHarness } from "@tsva/parity/support/tracing";

describe("UnitTests.General.ActivityPropagationTests", () => {
  const harness = createTracingHarness();
  let cluster: TestCluster;
  let client: ClientNode;

  beforeAll(async () => {
    cluster = await TestCluster.start({
      initialSilos: 1,
      grains: [{ ctor: ActivityGrain, interfaces: [IActivityGrain] }],
      configureSilo: (builder) => builder.useTracing(),
    });
    client = await createClusterClient(
      cluster,
      [{ ctor: ActivityGrain, interfaces: [IActivityGrain] }],
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

  // [Theory] over ActivityIdFormat.{W3C, Hierarchical} upstream — this
  // framework has only the one (W3C-shaped) format, so a single case covers
  // both `IGrainFactory` (in-process) AND external-client calls, mirroring
  // upstream's own `Test(_fixture.GrainFactory)` / `Test(_fixture.Client)`.
  orleansTest("UnitTests.General.ActivityPropagationTests.WithoutParentActivity", async () => {
    for (const grain of [
      cluster.getGrain(IActivityGrain, randomIntegerKey()),
      client.getGrain(IActivityGrain, randomIntegerKey()),
    ]) {
      const result = await harness.withNoActiveSpan(() => grain.getActivityData());

      expect(result).toBeDefined();
      expect(result.spanId).toBeTruthy();
      expect(result.traceState).toBeUndefined();
    }
  });

  orleansTest("UnitTests.General.ActivityPropagationTests.WithParentActivity_W3C", async () => {
    const traceStateStr = "vendor=value";
    const baggage = propagation.createBaggage({ foo: { value: "bar" } });

    for (const grain of [
      cluster.getGrain(IActivityGrain, randomIntegerKey()),
      client.getGrain(IActivityGrain, randomIntegerKey()),
    ]) {
      const { result, traceId } = await context.with(
        propagation.setBaggage(context.active(), baggage),
        () =>
          harness.withTraceState("activity-propagation-parent", traceStateStr, () =>
            grain.getActivityData(),
          ),
      );

      expect(result).toBeDefined();
      expect(result.spanId).toBeTruthy();
      // The trace id from the parent activity is preserved in the grain.
      expect(result.traceId).toBe(traceId);
      expect(result.traceState).toBe(traceStateStr);
      expect(result.baggage).toEqual({ foo: "bar" });
    }
  });

  orleansTest.excluded(
    "OTel has no `ActivityIdFormat` concept (only the one, W3C-shaped, " +
      "propagation format exists in this port) — and upstream's OWN real " +
      "behaviour for Hierarchical format on .NET 10+ (the pinned parity " +
      "target) is identical to `WithoutParentActivity` (the W3C propagator " +
      "can't inject a Hierarchical id, so the grain gets a fresh root span, " +
      "no baggage): there is no distinct format switch here to give that a " +
      "separately meaningful port",
    "UnitTests.General.ActivityPropagationTests.WithParentActivity_Hierarchical",
  );
});
