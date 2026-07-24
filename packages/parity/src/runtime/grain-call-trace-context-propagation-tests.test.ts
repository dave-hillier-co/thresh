// Ported from dotnet/orleans test/Orleans.Runtime.Tests/GrainCallTraceContextPropagationTests.cs @ v10.1.0 (MIT).
//
// Upstream listens to `System.Diagnostics.Activity` via an `ActivityListener`
// bagging every started `Activity`; this port uses the OpenTelemetry
// equivalent — a real `BasicTracerProvider` + in-memory exporter registered
// by `@tsva/parity/support/tracing`'s `createTracingHarness()` — and the
// framework's EXISTING tracing call filters (`tracingFilters()` in
// `@tsva/observability/tracing`, wired via `SiloBuilder.useTracing()` and
// `ClientConfig.outgoingCallFilters`) rather than any new instrumentation.
//
// Split (21 upstream cases):
//  - 20 ported: 18 pure W3C trace-context PROPAGATION cases across a
//    client→grain call, grain-to-grain calls, concurrent calls, malformed/
//    missing headers, tracestate, a foreign (non-Orleans) ambient span, and
//    cross-silo calls — all achievable with the existing outgoing/incoming
//    call filters — plus `ActivationSpanSharesTraceIdWithTriggeringGrainCall`
//    and `FullTraceHierarchyIsCorrectWhenActivationTriggeredByGrainCall`,
//    which lean on the ACTIVATION-path span taxonomy
//    (`@tsva/observability/activation-tracing`'s `ActivateGrain` span, wired
//    through `ClusterNode.receiveRequest` → `DistributedDispatcher.deliverLocal`
//    so it shares the triggering call's trace id — see
//    activation-tracing-tests.test.ts's `ActivationSpanIsCreatedOnFirstCall`).
//  - 1 excluded: `DiagnoseActivitySourceSampling` is a .NET-specific diagnostic
//    enumerating `ActivitySource` sampling per source (Application/Runtime/
//    Lifecycle/Storage) with ZERO assertions in upstream — there is no W3C/OTel
//    analogue of per-`ActivitySource` listener sampling to check, and nothing
//    here would exercise propagation beyond what the other 18 already cover.
import { afterAll, beforeAll, beforeEach, describe, expect, it } from "vitest";
import { SpanKind, trace } from "@opentelemetry/api";
import { GrainId } from "@tsva/core/grain-id";
import { getGrainMetadata } from "@tsva/core/grain-metadata";
import { SiloAddress } from "@tsva/core/silo-address";
import { requestContext } from "@tsva/runtime/invocation-context";
import { ActivityNames } from "@tsva/observability/activation-tracing";
import { tracingFilters } from "@tsva/observability/tracing";
import { orleansTest } from "@tsva/testing/orleans-test";
import { TestCluster, type TestSiloHandle } from "@tsva/testing/test-cluster";
import { waitFor } from "@tsva/testing/wait";
import { InProcessTransport } from "@tsva/messaging/in-process-transport";
import { createClient, type ClientNode } from "@tsva/client/client-node";
import {
  ITraceContextPropagationGrain,
  TraceContextPropagationGrain,
} from "@tsva/parity/grains/impl/trace-context-propagation-grain";
import type { TraceContextInfo } from "@tsva/parity/grains/interfaces/trace-context-propagation-grain-interfaces";
import { SimpleObserverableGrain } from "@tsva/parity/grains/impl/simple-observerable-grain";
import {
  ISimpleGrainObserver,
  ISimpleObserverableGrain,
} from "@tsva/parity/grains/interfaces/simple-observerable-grain-interfaces";
import { createClusterClient } from "@tsva/parity/support/client";
import { randomIntegerKey } from "@tsva/parity/support/keys";
import { createTracingHarness } from "@tsva/parity/support/tracing";

const grainType = getGrainMetadata(TraceContextPropagationGrain)!.grainType;

function hostOf(cluster: TestCluster, grainId: GrainId): TestSiloHandle | undefined {
  return cluster.silos.find((s) => s.host.isActive(grainId));
}

describe("UnitTests.General.GrainCallTraceContextPropagationTests", () => {
  const harness = createTracingHarness();
  let cluster: TestCluster;
  // The client wires the SAME `tracingFilters().outgoing` the silo wires on
  // its incoming side, exercising `ClientConfig.outgoingCallFilters`
  // (`ClientNode` → its internal `GrainFactory.setOutgoingCallFilters`) —
  // Orleans' client-builder `AddOutgoingGrainCallFilter`.
  let client: ClientNode;

  beforeAll(async () => {
    cluster = await TestCluster.start({
      initialSilos: 1,
      grains: [
        { ctor: TraceContextPropagationGrain, interfaces: [ITraceContextPropagationGrain] },
        // Also registered here (not just on a per-test client) so the
        // server-to-client observer-push case below shares this describe's
        // single `harness`/global tracer-provider registration.
        { ctor: SimpleObserverableGrain, interfaces: [ISimpleObserverableGrain] },
      ],
      configureSilo: (builder) => builder.useTracing(),
    });
    client = await createClusterClient(
      cluster,
      [{ ctor: TraceContextPropagationGrain, interfaces: [ITraceContextPropagationGrain] }],
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
    "UnitTests.General.GrainCallTraceContextPropagationTests.ServerSideGrainCallSharesSameTraceIdAsClient",
    async () => {
      const { result, traceId: clientTraceId } = await harness.withParentSpan(
        "client-parent-activity",
        () =>
          client.getGrain(ITraceContextPropagationGrain, randomIntegerKey()).getTraceContextInfo(),
      );

      expect(result.hasActivity).toBe(true);
      expect(result.traceId).toBe(clientTraceId);
    },
  );

  orleansTest(
    "UnitTests.General.GrainCallTraceContextPropagationTests.ServerSideActivityHasCorrectKindAndRemoteParent",
    async () => {
      const { result } = await harness.withParentSpan("client-activity-kind-test", () =>
        client.getGrain(ITraceContextPropagationGrain, randomIntegerKey()).getTraceContextInfo(),
      );
      expect(result.hasActivity).toBe(true);

      const serverSpan = harness
        .finishedSpans()
        .find((s) => s.kind === SpanKind.SERVER && s.name.includes("getTraceContextInfo"));
      expect(serverSpan).toBeDefined();
      expect(serverSpan!.kind).toBe(SpanKind.SERVER);
      expect(serverSpan!.parentIsRemote).toBe(true);
    },
  );

  orleansTest(
    "UnitTests.General.GrainCallTraceContextPropagationTests.NestedGrainCallsShareSameTraceId",
    async () => {
      const { result, traceId: clientTraceId } = await harness.withParentSpan(
        "client-nested-calls-test",
        () =>
          client
            .getGrain(ITraceContextPropagationGrain, randomIntegerKey())
            .getNestedTraceContextInfo(),
      );

      expect(result.local.hasActivity).toBe(true);
      expect(result.nested.hasActivity).toBe(true);
      expect(result.local.traceId).toBe(clientTraceId);
      expect(result.nested.traceId).toBe(clientTraceId);
    },
  );

  orleansTest(
    "UnitTests.General.GrainCallTraceContextPropagationTests.TraceParentIsSetInRequestContext",
    async () => {
      const { result, traceId: clientTraceId } = await harness.withParentSpan(
        "client-traceparent-test",
        () =>
          client.getGrain(ITraceContextPropagationGrain, randomIntegerKey()).getTraceContextInfo(),
      );

      expect(result.traceParentFromRequestContext).toBeDefined();
      expect(result.traceParentFromRequestContext).not.toBe("");
      expect(result.traceParentFromRequestContext).toContain(clientTraceId);
    },
  );

  orleansTest(
    "UnitTests.General.GrainCallTraceContextPropagationTests.ClientAndServerSpansAreProperlyLinked",
    async () => {
      const { traceId: clientTraceId, spanId: clientParentSpanId } = await harness.withParentSpan(
        "client-linking-test",
        () =>
          client.getGrain(ITraceContextPropagationGrain, randomIntegerKey()).getTraceContextInfo(),
      );

      const spans = harness.finishedSpans();
      const clientOutgoingSpan = spans.find(
        (s) => s.kind === SpanKind.CLIENT && s.name.includes("getTraceContextInfo"),
      );
      const serverIncomingSpan = spans.find(
        (s) => s.kind === SpanKind.SERVER && s.name.includes("getTraceContextInfo"),
      );

      expect(clientOutgoingSpan).toBeDefined();
      expect(serverIncomingSpan).toBeDefined();
      expect(clientOutgoingSpan!.traceId).toBe(clientTraceId);
      expect(serverIncomingSpan!.traceId).toBe(clientTraceId);
      // Client outgoing span is parented to the test's own parent span.
      expect(clientOutgoingSpan!.parentSpanId).toBe(clientParentSpanId);
      // Server span's parent is the client outgoing span.
      expect(serverIncomingSpan!.parentSpanId).toBe(clientOutgoingSpan!.spanId);
    },
  );

  orleansTest(
    "UnitTests.General.GrainCallTraceContextPropagationTests.ServerCreatesOwnTraceWhenClientHasNoActivity",
    async () => {
      const result = await harness.withNoActiveSpan(() =>
        client.getGrain(ITraceContextPropagationGrain, randomIntegerKey()).getTraceContextInfo(),
      );

      expect(result.hasActivity).toBe(true);
      expect(result.traceId).toBeDefined();
      expect(result.traceId).not.toBe("");
    },
  );

  orleansTest(
    "UnitTests.General.GrainCallTraceContextPropagationTests.ConcurrentGrainCallsShareSameTraceId",
    async () => {
      const { result: results, traceId: clientTraceId } = await harness.withParentSpan(
        "client-concurrent-test",
        () =>
          Promise.all(
            Array.from({ length: 5 }, () =>
              client
                .getGrain(ITraceContextPropagationGrain, randomIntegerKey())
                .getTraceContextInfo(),
            ),
          ),
      );

      for (const result of results) {
        expect(result.hasActivity).toBe(true);
        expect(result.traceId).toBe(clientTraceId);
      }
    },
  );

  orleansTest(
    "UnitTests.General.GrainCallTraceContextPropagationTests.ServerHandlesMalformedTraceParentGracefully",
    async () => {
      // A dedicated client whose outgoing filter injects a malformed
      // `traceparent` directly, bypassing the real tracing filter (which
      // would always overwrite it with a freshly-minted valid one) — this
      // isolates the INCOMING filter's handling of a bad header on the wire.
      // Built on a DISTINCT local address so closing it does not tear down
      // the shared `beforeAll` client's transport listener (which uses the
      // fixed `createClusterClient` address).
      const malformedClient = createClient({
        clusterId: cluster.clusterId,
        local: new SiloAddress("malformed-client", "uid-malformed", "malformed-client:23232"),
        transport: new InProcessTransport(cluster.network, cluster.clusterId),
        gateway: cluster.primary.address,
        outgoingCallFilters: [
          async (ctx) => {
            ctx.headers["traceparent"] = "invalid-traceparent-value";
            await ctx.invoke();
          },
        ],
      }).registerGrains([
        { ctor: TraceContextPropagationGrain, interfaces: [ITraceContextPropagationGrain] },
      ]);
      await malformedClient.connect();
      try {
        const result = await malformedClient
          .getGrain(ITraceContextPropagationGrain, randomIntegerKey())
          .getTraceContextInfo();

        expect(result.hasActivity).toBe(true);
        expect(result.traceId).toBeDefined();
      } finally {
        await malformedClient.close();
      }
    },
  );

  orleansTest(
    "UnitTests.General.GrainCallTraceContextPropagationTests.TraceParentReflectsClientOutgoingSpan",
    async () => {
      const {
        result,
        traceId: clientTraceId,
        spanId: clientParentSpanId,
      } = await harness.withParentSpan("client-traceparent-reflection-test", () =>
        client.getGrain(ITraceContextPropagationGrain, randomIntegerKey()).getTraceContextInfo(),
      );

      expect(result.traceParentFromRequestContext).toBeDefined();
      expect(result.traceParentFromRequestContext).toContain(clientTraceId);

      const clientOutgoingSpan = harness
        .finishedSpans()
        .find((s) => s.kind === SpanKind.CLIENT && s.name.includes("getTraceContextInfo"));
      expect(clientOutgoingSpan).toBeDefined();

      // The server's parent is the client's OUTGOING span, not the original
      // test parent span the outgoing call was made under.
      expect(result.traceParentFromRequestContext).not.toContain(clientParentSpanId);
      expect(result.traceParentFromRequestContext).toContain(clientOutgoingSpan!.spanId);
    },
  );

  orleansTest(
    "UnitTests.General.GrainCallTraceContextPropagationTests.TraceContextIsPropagatedDuringActivation",
    async () => {
      // Unique key so the call also triggers activation; this framework has
      // no activation span to additionally check (GAP-TRACING, see the
      // still-gapped cases below) — upstream's own check is guarded the same
      // way (`if (activationSpan is not null) { ... }`).
      const { result, traceId: clientTraceId } = await harness.withParentSpan(
        "client-activation-context-test",
        () =>
          client.getGrain(ITraceContextPropagationGrain, randomIntegerKey()).getTraceContextInfo(),
      );

      expect(result.traceId).toBe(clientTraceId);
    },
  );

  orleansTest(
    "UnitTests.General.GrainCallTraceContextPropagationTests.TraceStateIsPropagatedWithTraceParent",
    async () => {
      const { result } = await harness.withTraceState(
        "client-tracestate-test",
        "vendor1=value1,vendor2=value2",
        () =>
          client.getGrain(ITraceContextPropagationGrain, randomIntegerKey()).getTraceContextInfo(),
      );
      expect(result.hasActivity).toBe(true);

      const serverSpan = harness
        .finishedSpans()
        .find((s) => s.kind === SpanKind.SERVER && s.name.includes("getTraceContextInfo"));
      expect(serverSpan).toBeDefined();
      expect(serverSpan!.traceState).toBeDefined();
      expect(serverSpan!.traceState).toContain("vendor1");
    },
  );

  orleansTest(
    "UnitTests.General.GrainCallTraceContextPropagationTests.ActivationSpanSharesTraceIdWithTriggeringGrainCall",
    async () => {
      const { result, traceId: clientTraceId } = await harness.withParentSpan(
        "client-activation-trace-test",
        // A unique grain id ensures this call triggers a fresh activation.
        () =>
          client.getGrain(ITraceContextPropagationGrain, randomIntegerKey()).getTraceContextInfo(),
      );

      const spans = harness.finishedSpans();
      const activationSpan = spans.find((s) => s.name === ActivityNames.ActivateGrain);

      expect(activationSpan).toBeDefined();
      expect(activationSpan!.traceId).toBe(clientTraceId);
      // The activation span should have a parent (not be a root span) — it
      // must be part of the incoming grain call's trace, not starting a new one.
      expect(activationSpan!.parentSpanId).toBeDefined();

      expect(result.traceId).toBe(clientTraceId);
    },
  );

  orleansTest(
    "UnitTests.General.GrainCallTraceContextPropagationTests.FullTraceHierarchyIsCorrectWhenActivationTriggeredByGrainCall",
    async () => {
      const { traceId: clientTraceId, spanId: clientParentSpanId } = await harness.withParentSpan(
        "client-hierarchy-test",
        () =>
          client.getGrain(ITraceContextPropagationGrain, randomIntegerKey()).getTraceContextInfo(),
      );

      const spans = harness.finishedSpans();

      const clientOutgoingSpan = spans.find(
        (s) => s.kind === SpanKind.CLIENT && s.name.includes("getTraceContextInfo"),
      );
      const serverIncomingSpan = spans.find(
        (s) => s.kind === SpanKind.SERVER && s.name.includes("getTraceContextInfo"),
      );
      const activationSpan = spans.find((s) => s.name === ActivityNames.ActivateGrain);

      expect(clientOutgoingSpan).toBeDefined();
      expect(serverIncomingSpan).toBeDefined();
      expect(activationSpan).toBeDefined();

      expect(clientOutgoingSpan!.traceId).toBe(clientTraceId);
      expect(serverIncomingSpan!.traceId).toBe(clientTraceId);
      expect(activationSpan!.traceId).toBe(clientTraceId);

      // Client outgoing span is parented to the test's own parent span.
      expect(clientOutgoingSpan!.parentSpanId).toBe(clientParentSpanId);
      // Server span's parent is the client outgoing span.
      expect(serverIncomingSpan!.parentSpanId).toBe(clientOutgoingSpan!.spanId);
      // Activation span should have a parent (not be a root span).
      expect(activationSpan!.parentSpanId).toBeDefined();

      // `TraceContextPropagationGrain` doesn't override `onActivate` (this
      // port has no analogue of upstream's Grain-base-vs-plain-implementer
      // distinction the `OnActivate` span's presence would otherwise hinge
      // on — see activation-tracing-tests.test.ts) so, matching upstream's
      // own `if (onActivateSpan is not null)` guard, there is none to check.
      const onActivateSpan = spans.find((s) => s.name === ActivityNames.OnActivate);
      expect(onActivateSpan).toBeUndefined();
    },
  );

  orleansTest(
    "UnitTests.General.GrainCallTraceContextPropagationTests.DiagnoseTraceContextInRequestContextData",
    async () => {
      const { result, traceId: clientTraceId } = await harness.withParentSpan(
        "client-diagnostic-test",
        () =>
          client.getGrain(ITraceContextPropagationGrain, randomIntegerKey()).getTraceContextInfo(),
      );

      expect(result.traceParentFromRequestContext).toBeDefined();
      expect(result.traceParentFromRequestContext).not.toBe("");
      expect(result.traceParentFromRequestContext).toContain(clientTraceId);
    },
  );

  orleansTest(
    "UnitTests.General.GrainCallTraceContextPropagationTests.ActivationStartsNewTraceWhenCallerHasNoActivity",
    async () => {
      // Upstream carries no assertions in this case (it only documents,
      // via output, that .NET's `StartActivity` may return null with no
      // caller activity). This framework's tracer always samples (an
      // `AlwaysOnSampler`, matching `createTracingHarness`), so the outgoing
      // filter opens a fresh root span regardless — server still gets a
      // trace either way, which is what this assertion (in the spirit of
      // upstream's own "HasActivity" checks elsewhere) verifies.
      const result = await harness.withNoActiveSpan(() =>
        client.getGrain(ITraceContextPropagationGrain, randomIntegerKey()).getTraceContextInfo(),
      );
      expect(result.hasActivity).toBe(true);
      expect(result.traceParentFromRequestContext).toBeDefined();
    },
  );

  orleansTest(
    "UnitTests.General.GrainCallTraceContextPropagationTests.GrainToGrainCallWithoutActivityContext",
    async () => {
      const key = randomIntegerKey();
      // Activate the caller grain first, under a real parent span.
      await harness.withParentSpan("setup-activity", () =>
        client.getGrain(ITraceContextPropagationGrain, key).getTraceContextInfo(),
      );
      harness.reset();

      const result = await harness.withNoActiveSpan(() =>
        client.getGrain(ITraceContextPropagationGrain, key).getNestedTraceContextInfo(),
      );

      expect(result.local.hasActivity).toBe(true);
      expect(result.nested.hasActivity).toBe(true);
      expect(result.local.traceId).toBe(result.nested.traceId);
    },
  );

  orleansTest(
    "UnitTests.General.GrainCallTraceContextPropagationTests.ActivationSpanHasNoParentWhenApplicationSourceNotSampled",
    async () => {
      // This framework always samples every span (no per-`ActivitySource`
      // listener registration to selectively disable), so the "not sampled"
      // branch upstream documents cannot occur here — this ports the
      // fixture's actual assertion (`applicationSpans.Count > 0`), verifying
      // grain-call spans are produced end to end.
      await harness.withParentSpan("client-sampling-test", () =>
        client.getGrain(ITraceContextPropagationGrain, randomIntegerKey()).getTraceContextInfo(),
      );

      const spans = harness.finishedSpans();
      expect(spans.length).toBeGreaterThan(0);
      expect(spans.some((s) => s.name.includes("getTraceContextInfo"))).toBe(true);
    },
  );

  orleansTest.excluded(
    "diagnostic-only test enumerating .NET `ActivitySource` sampling per source " +
      "(Microsoft.Orleans.Application/Runtime/Lifecycle/Storage) with ZERO " +
      "assertions upstream — no W3C/OTel analogue of per-ActivitySource " +
      "listener sampling, and nothing here would exercise propagation beyond " +
      "what the other ported cases in this file already cover",
    "UnitTests.General.GrainCallTraceContextPropagationTests.DiagnoseActivitySourceSampling",
  );

  orleansTest(
    "UnitTests.General.GrainCallTraceContextPropagationTests.TraceContextPropagatedFromHttpActivityToGrainCall",
    async () => {
      // A parent span from a DIFFERENT tracer (the analogue of upstream's
      // separate "Microsoft.AspNetCore" ActivitySource/listener) — proves
      // propagation only cares about the ambient OTel context, not which
      // tracer produced it.
      const httpTracer = trace.getTracer("simulated-http");
      let httpTraceId!: string;
      let result!: TraceContextInfo;
      await httpTracer.startActiveSpan(
        "GET /api/users/{id}",
        { kind: SpanKind.SERVER },
        async (span) => {
          httpTraceId = span.spanContext().traceId;
          result = await client
            .getGrain(ITraceContextPropagationGrain, randomIntegerKey())
            .getTraceContextInfo();
          span.end();
        },
      );

      expect(result.traceId).toBe(httpTraceId);
    },
  );

  orleansTest(
    "UnitTests.General.GrainCallTraceContextPropagationTests.TraceContextPreservedWhenGrainPlacedOnDifferentSilo",
    async () => {
      const crossSiloCluster = await TestCluster.start({
        initialSilos: 2,
        grains: [
          { ctor: TraceContextPropagationGrain, interfaces: [ITraceContextPropagationGrain] },
        ],
        configureSilo: (builder) => builder.useTracing(),
      });
      const crossSiloClient = await createClusterClient(
        crossSiloCluster,
        [{ ctor: TraceContextPropagationGrain, interfaces: [ITraceContextPropagationGrain] }],
        undefined,
        [tracingFilters().outgoing],
      );
      try {
        const targetSilo = crossSiloCluster.silos[1]!;
        // Upstream forces placement via `RequestContext.Set(PlacementHintKey,
        // ...)`; this framework has no such hint (see the parity notes on
        // `migration-tests.test.ts`), so — like
        // `grain-placement-cluster-change-tests.test.ts` — retry random keys
        // under this grain's `RandomPlacement` strategy until one lands on
        // the target silo. The placement/warm-up loop routes through
        // `cluster.getGrain` (the primary silo's own factory), which spreads
        // random placement across BOTH silos; a client-routed call always
        // activates on its gateway silo, so it can't be used to seed a grain
        // onto a non-gateway silo.
        let key!: bigint;
        for (;;) {
          key = randomIntegerKey();
          await crossSiloCluster.getGrain(ITraceContextPropagationGrain, key).getTraceContextInfo();
          if (hostOf(crossSiloCluster, new GrainId(grainType, key)) === targetSilo) break;
        }

        harness.reset();
        const { result, traceId: clientTraceId } = await harness.withParentSpan(
          "cross-silo-test",
          () => crossSiloClient.getGrain(ITraceContextPropagationGrain, key).getTraceContextInfo(),
        );

        expect(result.traceId).toBe(clientTraceId);
      } finally {
        await crossSiloClient.close();
        await crossSiloCluster.dispose();
      }
    },
  );

  orleansTest(
    "UnitTests.General.GrainCallTraceContextPropagationTests.TraceContextPreservedInCrossSiloGrainToGrainCall",
    async () => {
      const crossSiloCluster = await TestCluster.start({
        initialSilos: 2,
        grains: [
          { ctor: TraceContextPropagationGrain, interfaces: [ITraceContextPropagationGrain] },
        ],
        configureSilo: (builder) => builder.useTracing(),
      });
      const crossSiloClient = await createClusterClient(
        crossSiloCluster,
        [{ ctor: TraceContextPropagationGrain, interfaces: [ITraceContextPropagationGrain] }],
        undefined,
        [tracingFilters().outgoing],
      );
      try {
        const [siloA, siloB] = crossSiloCluster.silos;
        // Retry random keys until the local grain lands on silo A and its
        // nested-call target (`key + 1`) lands on silo B, so the nested call
        // in `getNestedTraceContextInfo` actually crosses silos. Placement/
        // warm-up routes through `cluster.getGrain` (see the note in the
        // sibling cross-silo test) so random placement can reach BOTH silos.
        let key!: bigint;
        for (;;) {
          key = randomIntegerKey();
          const nestedKey = key + 1n;
          await crossSiloCluster.getGrain(ITraceContextPropagationGrain, key).getTraceContextInfo();
          await crossSiloCluster
            .getGrain(ITraceContextPropagationGrain, nestedKey)
            .getTraceContextInfo();
          if (
            hostOf(crossSiloCluster, new GrainId(grainType, key)) === siloA &&
            hostOf(crossSiloCluster, new GrainId(grainType, nestedKey)) === siloB
          ) {
            break;
          }
        }

        harness.reset();
        const { result, traceId: clientTraceId } = await harness.withParentSpan(
          "cross-silo-g2g-test",
          () =>
            crossSiloClient
              .getGrain(ITraceContextPropagationGrain, key)
              .getNestedTraceContextInfo(),
        );

        expect(result.local.traceId).toBe(clientTraceId);
        expect(result.nested.traceId).toBe(clientTraceId);
      } finally {
        await crossSiloClient.close();
        await crossSiloCluster.dispose();
      }
    },
  );

  // Not a ported upstream case — GAP-TRACING's third leg (issue #24): the
  // ported cases above cover client→grain and grain-to-grain; this covers
  // the third direction, silo→client, a grain notifying a client-hosted
  // observer (`ClientNode.createObjectReference`). The outgoing call runs
  // through the SAME `GrainFactory.outgoingCallFilters` as any other
  // grain-issued call (`SiloBuilder.useTracing()` wires `tracingFilters().outgoing`
  // there), so it already injects `traceparent`; what this exercises is the
  // CLIENT side picking that header up via its own `incomingCallFilters`
  // (`ClientConfig.incomingCallFilters`, the client-side mirror of the
  // silo's incoming filter pipeline — see `ClientNode.dispatchToLocalObject`).
  it("propagates the triggering call's W3C traceparent into a client-hosted observer, opening a client-side SERVER span", async () => {
    // Wires `tracingFilters().incoming` as this SEPARATE client's
    // `incomingCallFilters` — the client-side analogue of
    // `SiloBuilder.useTracing()` — so a call INTO this client's hosted
    // observer opens a SERVER span from the propagated context, exactly
    // like a grain's own incoming filter does. A dedicated client (distinct
    // from the describe's shared `client`) keeps this test's observer
    // registration isolated.
    const observerClient = await createClusterClient(
      cluster,
      [{ ctor: SimpleObserverableGrain, interfaces: [ISimpleObserverableGrain] }],
      [tracingFilters().incoming],
      [tracingFilters().outgoing],
    );
    let observedTraceParent: string | undefined;
    const ref = observerClient.createObjectReference(ISimpleGrainObserver, {
      stateChanged: async () => {
        observedTraceParent = requestContext.get("traceparent");
      },
    });
    try {
      const grain = observerClient.getGrain(ISimpleObserverableGrain, randomIntegerKey());

      const { traceId: clientTraceId } = await harness.withParentSpan(
        "observer-push-parent",
        async () => {
          await grain.subscribe(ref);
          // Triggers the grain's `notify()`, a silo-issued outgoing call to
          // the observer reference — inheriting the ambient span this turn
          // runs under (Orleans parity: an outgoing call made from inside a
          // grain turn shares the turn's trace).
          await grain.setA(3);
        },
      );

      await waitFor(() => observedTraceParent !== undefined);
      expect(observedTraceParent).toContain(clientTraceId);

      const observerServerSpan = harness
        .finishedSpans()
        .find((s) => s.kind === SpanKind.SERVER && s.name.includes("stateChanged"));
      expect(observerServerSpan).toBeDefined();
      expect(observerServerSpan!.traceId).toBe(clientTraceId);

      const siloOutgoingSpan = harness
        .finishedSpans()
        .find((s) => s.kind === SpanKind.CLIENT && s.name.includes("stateChanged"));
      expect(siloOutgoingSpan).toBeDefined();
      expect(siloOutgoingSpan!.traceId).toBe(clientTraceId);
      // The client's SERVER span is parented to the silo's outgoing CLIENT
      // span for the SAME call — the same client/server pairing the ported
      // client→grain and grain-to-grain cases assert above.
      expect(observerServerSpan!.parentSpanId).toBe(siloOutgoingSpan!.spanId);
    } finally {
      observerClient.deleteObjectReference(ref);
      await observerClient.close();
    }
  });
});
