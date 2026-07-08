// Ported from dotnet/orleans test/Orleans.Runtime.Tests/GrainCallFilterTests.cs @ v10.1.0 (MIT).
import { afterAll, beforeAll, describe, expect } from "vitest";
import { orleansTest } from "@tsva/testing/orleans-test";
import { TestCluster } from "@tsva/testing/test-cluster";
import type {
  IncomingGrainCallFilter,
  OutgoingGrainCallFilter,
} from "@tsva/core/grain-call-filter";
import { castGrainReference } from "@tsva/core/grain-reference";
import {
  GRAIN_CALL_FILTER_TEST_KEY,
  GrainCallFilterTestGrain,
  IGrainCallFilterTestGrain,
  IMethodInterceptionGrain,
  IMyGrainExtension,
  IOutgoingMethodInterceptionGrain,
  MethodInterceptionGrain,
  MyGrainExtension,
  OutgoingMethodInterceptionGrain,
} from "@tsva/parity/grains/impl/method-interception-grain";
import { randomIntegerKey } from "@tsva/parity/support/keys";

// System-wide incoming filter: mirrors the upstream fixture's
// `SiloInvokerTestSiloBuilderConfigurator` incoming filter, trimmed to just
// the `SystemWideCallFilterMarker` short-circuit this file exercises (the
// extension-value-negation branch belongs to a gapped test — see the
// file-level notes below). Also starts the RequestContext value the
// `GrainCallFilter_Incoming_Order_Test` below reads back: "1".
const systemWideIncoming: IncomingGrainCallFilter = async (ctx) => {
  if (
    ctx.methodName === "getRequestContext" &&
    ctx.headers[GRAIN_CALL_FILTER_TEST_KEY] === undefined
  ) {
    ctx.headers[GRAIN_CALL_FILTER_TEST_KEY] = "1";
  }
  if (ctx.methodName === "systemWideCallFilterMarker") {
    // explicitly do not continue calling invoke()
    return;
  }
  // Request manipulation reaching through an extension call
  // (`GrainCallFilter_GrainExtension`): negate the value argument before it
  // reaches `MyGrainExtension.setExtensionValue`, proving a silo-wide filter
  // sees and can rewrite an extension method's args exactly like an ordinary
  // grain method's.
  if (ctx.methodName === "setExtensionValue") {
    ctx.args[0] = (ctx.args[0] as number) * -1;
  }
  await ctx.invoke();
};

// A second silo-wide incoming filter, mirroring upstream's separately
// registered `GrainCallFilterWithDependencies` (a distinct filter class with
// its own DI-resolved dependency): continues the RequestContext build-up
// "1" -> "12", proving two independently registered silo-wide filters (not
// just one) each see the same ambient headers.
const requestContextContinuation: IncomingGrainCallFilter = async (ctx) => {
  if (ctx.methodName === "getRequestContext") {
    const existing = ctx.headers[GRAIN_CALL_FILTER_TEST_KEY];
    if (existing !== undefined) ctx.headers[GRAIN_CALL_FILTER_TEST_KEY] = `${existing}2`;
  }
  await ctx.invoke();
};

// System-wide outgoing filter pairing: short-circuits `SystemWideCallFilterMarker`
// and retries `IOutgoingMethodInterceptionGrain.ThrowIfGreaterThanZero` once,
// mirroring upstream's client-side `RetryCertainCalls` + marker filters.
const systemWideOutgoing: OutgoingGrainCallFilter = async (ctx) => {
  if (ctx.methodName === "systemWideCallFilterMarker") {
    // explicitly do not continue calling invoke()
    return;
  }

  let attemptsRemaining = 2;
  while (attemptsRemaining > 0) {
    try {
      await ctx.invoke();
      return;
    } catch (error) {
      // A cross-silo outgoing call surfaces the remote failure as a
      // GrainCallError carrying just the message (the original error class
      // does not survive the wire), so match on message text rather than
      // `instanceof RangeError` here. Scoped to `IOutgoingMethodInterceptionGrain`
      // only — this filter is silo-wide (every outgoing call from `TestCluster`'s
      // one silo host goes through it, including calls this file's other tests
      // make to `IGrainCallFilterTestGrain.throwIfGreaterThanZero`), so it must
      // not also retry that unrelated grain's identically-named method.
      const canRetry =
        attemptsRemaining > 1 &&
        error instanceof Error &&
        error.message.includes("is greater than zero!") &&
        ctx.interfaceName === "UnitTests.GrainInterfaces.IOutgoingMethodInterceptionGrain" &&
        ctx.methodName === "throwIfGreaterThanZero" &&
        typeof ctx.args[0] === "number";
      if (!canRetry) throw error;
      ctx.args[0] = (ctx.args[0] as number) - 1;
      attemptsRemaining -= 1;
    }
  }
};

describe("UnitTests.General.GrainCallFilterTests", () => {
  let cluster: TestCluster;

  beforeAll(async () => {
    cluster = await TestCluster.start({
      grains: [
        { ctor: MethodInterceptionGrain, interfaces: [IMethodInterceptionGrain] },
        { ctor: OutgoingMethodInterceptionGrain, interfaces: [IOutgoingMethodInterceptionGrain] },
        { ctor: GrainCallFilterTestGrain, interfaces: [IGrainCallFilterTestGrain] },
      ],
      configureSilo: (builder) => {
        builder.addIncomingCallFilter(systemWideIncoming);
        builder.addIncomingCallFilter(requestContextContinuation);
        builder.addOutgoingCallFilter(systemWideOutgoing);
        builder.addGrainExtension(IMyGrainExtension, () => new MyGrainExtension());
      },
    });
  });

  afterAll(async () => {
    await cluster.dispose();
  });

  // Needs a distinct client-vs-grain outgoing-filter layer (client uppercases,
  // grain1's own outgoing filter doubles, grain2's incoming filter reverses,
  // then the client's outgoing filter rewrites the response) — `TestCluster`
  // routes every call through one primary silo host with one set of filters,
  // so "client-issued" and "grain-issued" outgoing calls cannot be filtered
  // separately here.
  orleansTest.gap(
    "GAP-CALL-FILTER-CLIENT-LAYER",
    "UnitTests.General.GrainCallFilterTests.GrainCallFilter_Outgoing_Test",
  );

  // Builds up RequestContext across filters ("1"->"12"->"123") then reads it
  // in the grain method ("1234"): two silo-wide filters each add a digit,
  // the grain's own filter adds a third, and the method body appends the
  // last, reading the ambient value back via `runtime.getRequestContext`.
  orleansTest(
    "UnitTests.General.GrainCallFilterTests.GrainCallFilter_Incoming_Order_Test",
    async () => {
      const grain = cluster.getGrain(IGrainCallFilterTestGrain, randomIntegerKey());

      const context = await grain.getRequestContext();

      expect(context).toBe("1234");
    },
  );

  // Stream providers are not wired into TestCluster/the parity harness.
  orleansTest.gap(
    "GAP-STREAM-PROVIDER-WIRING",
    "UnitTests.General.GrainCallFilterTests.GrainCallFilter_Incoming_Stream_Test",
  );

  orleansTest(
    "UnitTests.General.GrainCallFilterTests.GrainCallFilter_Incoming_Retry_Test",
    async () => {
      const grain = cluster.getGrain(IGrainCallFilterTestGrain, 0n);

      const result = await grain.throwIfGreaterThanZero(1);
      expect(result).toBe("Thanks for nothing");

      await expect(grain.throwIfGreaterThanZero(2)).rejects.toThrow();
    },
  );

  orleansTest(
    "UnitTests.General.GrainCallFilterTests.GrainCallFilter_Incoming_HashSet_Test",
    async () => {
      const grain = cluster.getGrain(IGrainCallFilterTestGrain, 0n);

      const result = await grain.sumSet([1, 2, 3]);
      expect(result).toBe(6);
    },
  );

  orleansTest(
    "UnitTests.General.GrainCallFilterTests.GrainCallFilter_Outgoing_Retry_Test",
    async () => {
      const grain = cluster.getGrain(IOutgoingMethodInterceptionGrain, 0n);

      const result = await grain.throwIfGreaterThanZero(1);
      expect(result).toBe("Thanks for nothing");

      await expect(grain.throwIfGreaterThanZero(2)).rejects.toThrow();
    },
  );

  orleansTest(
    "UnitTests.General.GrainCallFilterTests.GrainCallFilter_Incoming_GrainLevel_Test",
    async () => {
      const grain = cluster.getGrain(IMethodInterceptionGrain, 0n);
      let result = await grain.one();
      expect(result).toBe("intercepted one with no args");

      result = await grain.echo("stao erom tae");
      // Grain interceptors should receive the MethodInfo of the implementation, not the interface.
      expect(result).toBe("eat more oats");

      result = await grain.notIntercepted();
      expect(result).toBe("not intercepted");

      result = await grain.sayHello();
      expect(result).toBe("Hello");
    },
  );

  orleansTest.gap(
    "GAP-GENERIC-GRAINS",
    "UnitTests.General.GrainCallFilterTests.GrainCallFilter_Incoming_GenericGrain_Test",
  );

  orleansTest.gap(
    "GAP-GENERIC-GRAINS",
    "UnitTests.General.GrainCallFilterTests.GrainCallFilter_Incoming_ConstructedGenericInheritance_Test",
  );

  orleansTest(
    "UnitTests.General.GrainCallFilterTests.GrainCallFilter_Incoming_ExceptionHandling_Test",
    async () => {
      const grain = cluster.getGrain(IMethodInterceptionGrain, randomIntegerKey());

      const result = await grain.doThrow();
      expect(result).toBe("EXCEPTION! Oi!");
    },
  );

  orleansTest(
    "UnitTests.General.GrainCallFilterTests.GrainCallFilter_Incoming_FilterThrows_Test",
    async () => {
      const grain = cluster.getGrain(IMethodInterceptionGrain, randomIntegerKey());

      await expect(grain.filterThrows()).rejects.toThrow("Filter THROW!");
    },
  );

  // Relies on the CLR unwrapping a `Task<string>` whose boxed `Result` is
  // actually a `Guid`, raising `InvalidCastException`; JS values are untyped
  // at runtime so there is no analogous cast failure to trigger.
  orleansTest.excluded(
    ".NET-specific: relies on CLR generic Task<T> result-casting (InvalidCastException); no analogue for untyped JS runtime values",
    "UnitTests.General.GrainCallFilterTests.GrainCallFilter_Incoming_SetIncorrectResultType_Test",
  );

  // Filters run for extension calls too (Orleans parity): the silo-wide
  // `systemWideIncoming` filter negates `setExtensionValue`'s argument before
  // it reaches `MyGrainExtension`, exactly like it would for an ordinary
  // grain method.
  orleansTest(
    "UnitTests.General.GrainCallFilterTests.GrainCallFilter_GrainExtension",
    async () => {
      const grain = cluster.getGrain(IMethodInterceptionGrain, randomIntegerKey());
      const extension = castGrainReference(grain, IMyGrainExtension);

      await extension.setExtensionValue(42);
      const result = await extension.getExtensionValue();

      expect(result).toBe(-42);
    },
  );

  orleansTest.gap(
    "GAP-GENERIC-GRAINS",
    "UnitTests.General.GrainCallFilterTests.GrainCallFilter_Incoming_GenericInterface_ConcreteGrain_Test",
  );

  // A filter that returns without calling invoke() is an error: the call
  // surfaces to the caller as a failure (Orleans throws InvalidOperationException
  // here).
  orleansTest(
    "UnitTests.General.GrainCallFilterTests.GrainCallFilter_Incoming_SystemWideDoesNotCallContextInvoke_Test",
    async () => {
      const grain = cluster.getGrain(IGrainCallFilterTestGrain, randomIntegerKey());
      await expect(grain.systemWideCallFilterMarker()).rejects.toThrow();
    },
  );

  orleansTest(
    "UnitTests.General.GrainCallFilterTests.GrainCallFilter_Incoming_GrainSpecificDoesNotCallContextInvoke_Test",
    async () => {
      const grain = cluster.getGrain(IGrainCallFilterTestGrain, randomIntegerKey());
      await expect(grain.grainSpecificCallFilterMarker()).rejects.toThrow();
    },
  );

  orleansTest(
    "UnitTests.General.GrainCallFilterTests.GrainCallFilter_Outgoing_SystemWideDoesNotCallContextInvoke_Test",
    async () => {
      const grain = cluster.getGrain(IMethodInterceptionGrain, randomIntegerKey());
      await expect(grain.systemWideCallFilterMarker()).rejects.toThrow();
    },
  );

  // Observer_* cases below exercise the same scenarios above through a
  // `CreateObjectReference` client observer; grain observers do not exist here
  // yet (see default-cluster/observer.test.ts).
  orleansTest.gap(
    "GAP-OBSERVERS",
    "UnitTests.General.GrainCallFilterTests.Observer_GrainCallFilter_Incoming_Order_Test",
  );

  orleansTest.gap(
    "GAP-OBSERVERS",
    "UnitTests.General.GrainCallFilterTests.Observer_GrainCallFilter_Incoming_Retry_Test",
  );

  orleansTest.gap(
    "GAP-OBSERVERS",
    "UnitTests.General.GrainCallFilterTests.Observer_GrainCallFilter_Incoming_HashSet_Test",
  );

  orleansTest.gap(
    "GAP-OBSERVERS",
    "UnitTests.General.GrainCallFilterTests.Observer_GrainCallFilter_Incoming_SystemWideDoesNotCallContextInvoke_Test",
  );

  orleansTest.gap(
    "GAP-OBSERVERS",
    "UnitTests.General.GrainCallFilterTests.Observer_GrainCallFilter_Incoming_GrainSpecificDoesNotCallContextInvoke_Test",
  );

  orleansTest.gap(
    "GAP-OBSERVERS",
    "UnitTests.General.GrainCallFilterTests.Observer_GrainCallFilter_Incoming_GrainLevel_Test",
  );

  orleansTest.gap(
    "GAP-OBSERVERS",
    "UnitTests.General.GrainCallFilterTests.Observer_GrainCallFilter_Incoming_GenericGrain_Test",
  );

  orleansTest.gap(
    "GAP-OBSERVERS",
    "UnitTests.General.GrainCallFilterTests.Observer_GrainCallFilter_Incoming_ConstructedGenericInheritance_Test",
  );

  orleansTest.gap(
    "GAP-OBSERVERS",
    "UnitTests.General.GrainCallFilterTests.Observer_GrainCallFilter_Incoming_ExceptionHandling_Test",
  );

  orleansTest.gap(
    "GAP-OBSERVERS",
    "UnitTests.General.GrainCallFilterTests.Observer_GrainCallFilter_Incoming_FilterThrows_Test",
  );

  orleansTest.excluded(
    ".NET-specific: relies on CLR generic Task<T> result-casting (InvalidCastException); no analogue for untyped JS runtime values",
    "UnitTests.General.GrainCallFilterTests.Observer_GrainCallFilter_Incoming_SetIncorrectResultType_Test",
  );
});
