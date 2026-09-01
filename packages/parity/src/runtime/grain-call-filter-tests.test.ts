// Ported from dotnet/orleans test/Orleans.Runtime.Tests/GrainCallFilterTests.cs @ v10.1.0 (MIT).
import { afterAll, beforeAll, describe, expect } from "vitest";
import { orleansTest } from "@thresh/testing/orleans-test";
import { TestCluster } from "@thresh/testing/test-cluster";
import {
  INCOMING_CALL_FILTER,
  type IncomingGrainCallContext,
  type IncomingGrainCallFilter,
  type OutgoingGrainCallFilter,
} from "@thresh/core/grain-call-filter";
import { grain } from "@thresh/core/decorators";
import { Grain } from "@thresh/core/grain";
import { defineGrainInterface } from "@thresh/core/grain-interface";
import type { GrainKey } from "@thresh/core/key-kinds";
import { castGrainReference } from "@thresh/core/grain-reference";
import type { ClientNode } from "@thresh/client/client-node";
import { invocationContext } from "@thresh/runtime/invocation-context";
import {
  GRAIN_CALL_FILTER_TEST_KEY,
  GrainCallFilterTestGrain,
  GrainCallFilterTestGrainObserver,
  IGrainCallFilterTestGrain,
  IGrainCallFilterTestGrainObserver,
  IMethodInterceptionGrain,
  IMethodInterceptionGrainObserver,
  IMyGrainExtension,
  IOutgoingMethodInterceptionGrain,
  MethodInterceptionGrain,
  MethodInterceptionGrainObserver,
  MyGrainExtension,
  OutgoingMethodInterceptionGrain,
} from "@thresh/parity/grains/impl/method-interception-grain";
import { createClusterClient } from "@thresh/parity/support/client";
import { randomGuidKey, randomIntegerKey } from "@thresh/parity/support/keys";
import type { Guid } from "@thresh/core/guid";

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

  // Doubles the string arg of an outgoing "echo" call — but only a
  // grain-to-grain one, i.e. one issued from inside a currently-executing
  // grain turn on this silo (`invocationContext.getStore() !== undefined`),
  // mirroring upstream's silo-wide outgoing filter
  // (`SiloInvokerTestSiloBuilderConfigurator`), which only ever sees
  // grain-issued calls — a real Orleans client's calls never reach a silo's
  // `AddOutgoingGrainCallFilter` pipeline at all. This port's single
  // `GrainFactory` is shared between `TestCluster.getGrain` (host/test-level,
  // no turn in scope) and genuine grain-to-grain calls, so the ambient-turn
  // check is this port's stand-in for that distinction — it keeps
  // `GrainCallFilter_Incoming_GrainLevel_Test`'s direct `grain.echo(...)`
  // call (host-level, no doubling) from colliding with
  // `GrainCallFilter_Outgoing_Test`'s grain1-calls-grain2 `echo` (doubled).
  // `ctx.source` cannot serve as this signal: it carries the ORIGINAL
  // external caller's id (propagated transitively as `sender`), which is
  // `undefined` for a client-issued call with no ambient sender of its own —
  // exactly the same as a host-level call, even though the resulting
  // grain1-to-grain2 hop genuinely originates from within grain1's turn.
  if (invocationContext.getStore() !== undefined && ctx.methodName === "echo") {
    const orig = ctx.args[0] as string;
    ctx.args[0] = orig + orig;
  }

  let attemptsRemaining = 2;
  while (attemptsRemaining > 0) {
    try {
      await ctx.invoke();
      return;
    } catch (error) {
      // A cross-silo outgoing call surfaces a remote `RangeError` as an
      // `UnavailableExceptionFallbackException` whose `name` is "RangeError" — the built-in class
      // itself is not rebuilt, only recorded — so `instanceof RangeError` is still the wrong
      // predicate here and the filter matches on message text. Scoped to `IOutgoingMethodInterceptionGrain`
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

// The client's own outgoing filter (Orleans `IClientBuilder.AddOutgoingGrainCallFilter`,
// upstream's `ClientConfigurator`): uppercases `EchoViaOtherGrain`'s message
// argument before it leaves the client, then — after the call returns —
// rewrites the result dictionary, proving a filter runs both before AND
// after a client-issued call distinctly from the silo's own outgoing filter
// (`systemWideOutgoing`, which only sees grain-to-grain calls).
const clientOutgoing: OutgoingGrainCallFilter = async (ctx) => {
  if (ctx.methodName === "echoViaOtherGrain" && typeof ctx.args[1] === "string") {
    ctx.args[1] = ctx.args[1].toUpperCase();
  }
  await ctx.invoke();
  if (ctx.methodName === "echoViaOtherGrain") {
    const result = ctx.result as Record<string, unknown>;
    result["orig"] = result["result"];
    result["result"] = "intercepted!";
  }
};

// Ported from test/Grains/TestGrains/StreamInterceptionGrain.cs. An explicit
// stream subscriber (subscribes in `onActivate`) that is ALSO its own incoming
// call filter: the filter snapshots `lastStreamValue`, invokes (the wrapped
// `onNext` writes the delivered value), then — if it changed — doubles it,
// proving a stream delivery flows through the grain's incoming call-filter
// pipeline exactly like an ordinary method call (Orleans parity).
interface IStreamInterceptionGrain extends GrainKey<Guid> {
  getLastStreamValue(): Promise<number>;
}
const IStreamInterceptionGrain = defineGrainInterface<IStreamInterceptionGrain>(
  "IStreamInterceptionGrain",
);

@grain()
class StreamInterceptionGrain extends Grain implements IStreamInterceptionGrain {
  private lastStreamValue = 0;

  override async onActivate(): Promise<void> {
    const streams = this.runtime.getStreamProvider("MemoryStreamProvider");
    const stream = streams.getStream<number>("InterceptedStream", this.id.key);
    await stream.subscribe({
      onNext: async (value) => {
        this.lastStreamValue = value;
      },
    });
  }

  async getLastStreamValue(): Promise<number> {
    return this.lastStreamValue;
  }

  async [INCOMING_CALL_FILTER](context: IncomingGrainCallContext): Promise<void> {
    const initialLastStreamValue = this.lastStreamValue;
    await context.invoke();
    // If the last stream value changed after the invoke, then the stream must
    // have produced a value; double it for testing purposes.
    if (this.lastStreamValue !== initialLastStreamValue) {
      this.lastStreamValue *= 2;
    }
  }
}

describe("UnitTests.General.GrainCallFilterTests", () => {
  let cluster: TestCluster;
  // The `Observer_*` cases below drive a client-hosted object
  // (`ClientNode.createObjectReference`) directly rather than a grain
  // activation; `client`'s own incoming filters mirror upstream's
  // `ClientConfigurator` — the SAME `systemWideIncoming`/
  // `requestContextContinuation` filters registered on the silo above,
  // reused verbatim since the method names they match (`getRequestContext`,
  // `systemWideCallFilterMarker`) are identical on the observer interfaces.
  // `client`'s outgoing filter (`clientOutgoing`) is a genuinely separate
  // pipeline from the silo's `systemWideOutgoing` — `GrainCallFilter_Outgoing_Test`
  // relies on that separation.
  let client: ClientNode;

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
    client = await createClusterClient(
      cluster,
      [{ ctor: OutgoingMethodInterceptionGrain, interfaces: [IOutgoingMethodInterceptionGrain] }],
      [systemWideIncoming, requestContextContinuation],
      [clientOutgoing],
    );
  });

  afterAll(async () => {
    await client.close();
    await cluster.dispose();
  });

  // Needs a distinct client-vs-grain outgoing-filter layer: `client`'s own
  // `clientOutgoing` filter uppercases the message on the way out, grain1
  // (`OutgoingMethodInterceptionGrain`) calls grain2's `echo` — a genuine
  // grain-to-grain outgoing call, doubled by the silo's `systemWideOutgoing`
  // — grain2's own incoming filter reverses the result, and `clientOutgoing`
  // rewrites the response dictionary after the call returns. Closed by
  // giving `client` its own `outgoingCallFilters` (`ClientConfig`,
  // `GrainFactory.setOutgoingCallFilters`), separate from the silo's.
  orleansTest("UnitTests.General.GrainCallFilterTests.GrainCallFilter_Outgoing_Test", async () => {
    const grain = client.getGrain(IOutgoingMethodInterceptionGrain, randomIntegerKey());
    const grain2 = cluster.getGrain(IMethodInterceptionGrain, randomIntegerKey());

    // This grain method reads the context and returns it.
    const result = await grain.echoViaOtherGrain(grain2, "ab");

    // Original arg should have been:
    // 1. Converted to upper case on the way out of the client: ab -> AB.
    // 2. Doubled on the way out of grain1: AB -> ABAB.
    // 3. Reversed on the way in to grain2: ABAB -> BABA.
    expect(result["orig"]).toBe("BABA");
    expect(result["result"]).toBe("intercepted!");
  });

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

  // A stream delivery to a grain flows through that grain's own incoming
  // call-filter pipeline (Orleans parity): `StreamInterceptionGrain` filters
  // its own calls, so the value delivered on the stream is doubled by the time
  // it can be read back. Runs on its own single-silo cluster — the memory
  // stream provider is per-silo, so the publishing test code and the
  // subscribed activation must share one silo (see `runStreamDelivery` in
  // `activation.ts`, which routes memory-stream `onNext` through the filter
  // pipeline the same way a pulling-agent delivery reaches `callMethod`).
  orleansTest(
    "UnitTests.General.GrainCallFilterTests.GrainCallFilter_Incoming_Stream_Test",
    async () => {
      const streamCluster = await TestCluster.start({
        initialSilos: 1,
        grains: [{ ctor: StreamInterceptionGrain, interfaces: [IStreamInterceptionGrain] }],
        configureSilo: (builder) => {
          builder.useMemoryStreams("MemoryStreamProvider");
        },
      });
      try {
        const id = randomGuidKey();
        const streamProvider = streamCluster.getStreamProvider("MemoryStreamProvider");
        if (streamProvider === undefined) throw new Error("no MemoryStreamProvider configured");
        const stream = streamProvider.getStream<number>("InterceptedStream", id);
        const grain = streamCluster.getGrain(IStreamInterceptionGrain, id);

        // Force activation so the grain's `onActivate` subscription is in place
        // before we publish. This port's memory subscription starts "from now",
        // so a pre-subscription publish would be missed — upstream's rewindable
        // pulling-agent cache hides this ordering, immaterial to what the test
        // asserts (that a delivered value is doubled by the grain's filter).
        expect(await grain.getLastStreamValue()).toBe(0);

        const testValue = 43;
        await stream.publish(testValue);

        let actual = 0;
        const deadline = Date.now() + 30_000;
        while (actual === 0 && Date.now() < deadline) {
          actual = await grain.getLastStreamValue();
          if (actual !== 0) break;
          await new Promise((resolve) => setTimeout(resolve, 10));
        }

        // The intercepted grain doubles the value passed to the stream.
        expect(actual).toBe(testValue * 2);
      } finally {
        await streamCluster.dispose();
      }
    },
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

  orleansTest.excluded(
    "open generic grain interfaces (e.g. IGenericGrain<T,U>/ISimpleGenericGrain<T>) are unrepresentable in this framework: TypeScript erases generics at runtime, and the grain-interface registry keys interfaces by a fixed string name/id with no notion of a type parameter to instantiate",
    "UnitTests.General.GrainCallFilterTests.GrainCallFilter_Incoming_GenericGrain_Test",
  );

  orleansTest.excluded(
    "open generic grain interfaces (e.g. IGenericGrain<T,U>/ISimpleGenericGrain<T>) are unrepresentable in this framework: TypeScript erases generics at runtime, and the grain-interface registry keys interfaces by a fixed string name/id with no notion of a type parameter to instantiate",
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
  orleansTest("UnitTests.General.GrainCallFilterTests.GrainCallFilter_GrainExtension", async () => {
    const grain = cluster.getGrain(IMethodInterceptionGrain, randomIntegerKey());
    const extension = castGrainReference(grain, IMyGrainExtension);

    await extension.setExtensionValue(42);
    const result = await extension.getExtensionValue();

    expect(result).toBe(-42);
  });

  orleansTest.excluded(
    "open generic grain interfaces (e.g. IGenericGrain<T,U>/ISimpleGenericGrain<T>) are unrepresentable in this framework: TypeScript erases generics at runtime, and the grain-interface registry keys interfaces by a fixed string name/id with no notion of a type parameter to instantiate",
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
  // `CreateObjectReference` client-hosted observer instead of a grain
  // activation: the call round-trips client -> gateway silo -> back to this
  // same client's `dispatchToLocalObject`, which now runs the SAME incoming
  // call-filter pipeline (`runCallFilters`) the silo's catalog runs for a
  // grain dispatch.
  orleansTest(
    "UnitTests.General.GrainCallFilterTests.Observer_GrainCallFilter_Incoming_Order_Test",
    async () => {
      const observer = new GrainCallFilterTestGrainObserver();
      const grain = client.createObjectReference(IGrainCallFilterTestGrainObserver, observer);

      const context = await grain.getRequestContext();

      expect(context).toBe("1234");
      client.deleteObjectReference(grain);
    },
  );

  orleansTest(
    "UnitTests.General.GrainCallFilterTests.Observer_GrainCallFilter_Incoming_Retry_Test",
    async () => {
      const observer = new GrainCallFilterTestGrainObserver();
      const grain = client.createObjectReference(IGrainCallFilterTestGrainObserver, observer);

      const result = await grain.throwIfGreaterThanZero(1);
      expect(result).toBe("Thanks for nothing");

      await expect(grain.throwIfGreaterThanZero(2)).rejects.toThrow();
      client.deleteObjectReference(grain);
    },
  );

  orleansTest(
    "UnitTests.General.GrainCallFilterTests.Observer_GrainCallFilter_Incoming_HashSet_Test",
    async () => {
      const observer = new GrainCallFilterTestGrainObserver();
      const grain = client.createObjectReference(IGrainCallFilterTestGrainObserver, observer);

      const result = await grain.sumSet([1, 2, 3]);
      expect(result).toBe(6);
      client.deleteObjectReference(grain);
    },
  );

  orleansTest(
    "UnitTests.General.GrainCallFilterTests.Observer_GrainCallFilter_Incoming_SystemWideDoesNotCallContextInvoke_Test",
    async () => {
      const observer = new GrainCallFilterTestGrainObserver();
      const grain = client.createObjectReference(IGrainCallFilterTestGrainObserver, observer);

      await expect(grain.systemWideCallFilterMarker()).rejects.toThrow();
      client.deleteObjectReference(grain);
    },
  );

  orleansTest(
    "UnitTests.General.GrainCallFilterTests.Observer_GrainCallFilter_Incoming_GrainSpecificDoesNotCallContextInvoke_Test",
    async () => {
      const observer = new GrainCallFilterTestGrainObserver();
      const grain = client.createObjectReference(IGrainCallFilterTestGrainObserver, observer);

      await expect(grain.grainSpecificCallFilterMarker()).rejects.toThrow();
      client.deleteObjectReference(grain);
    },
  );

  orleansTest(
    "UnitTests.General.GrainCallFilterTests.Observer_GrainCallFilter_Incoming_GrainLevel_Test",
    async () => {
      const observer = new MethodInterceptionGrainObserver();
      const grain = client.createObjectReference(IMethodInterceptionGrainObserver, observer);

      let result = await grain.one();
      expect(result).toBe("intercepted one with no args");

      result = await grain.echo("stao erom tae");
      expect(result).toBe("eat more oats");

      result = await grain.notIntercepted();
      expect(result).toBe("not intercepted");

      result = await grain.sayHello();
      expect(result).toBe("Hello");
      client.deleteObjectReference(grain);
    },
  );

  // Same reason as the non-observer `GrainCallFilter_Incoming_GenericGrain_Test`
  // above: open generic grain interfaces are unrepresentable in this framework.
  orleansTest.excluded(
    "open generic grain interfaces (e.g. IGenericGrain<T,U>/ISimpleGenericGrain<T>) are unrepresentable in this framework: TypeScript erases generics at runtime, and the grain-interface registry keys interfaces by a fixed string name/id with no notion of a type parameter to instantiate",
    "UnitTests.General.GrainCallFilterTests.Observer_GrainCallFilter_Incoming_GenericGrain_Test",
  );

  orleansTest.excluded(
    "open generic grain interfaces (e.g. IGenericGrain<T,U>/ISimpleGenericGrain<T>) are unrepresentable in this framework: TypeScript erases generics at runtime, and the grain-interface registry keys interfaces by a fixed string name/id with no notion of a type parameter to instantiate",
    "UnitTests.General.GrainCallFilterTests.Observer_GrainCallFilter_Incoming_ConstructedGenericInheritance_Test",
  );

  orleansTest(
    "UnitTests.General.GrainCallFilterTests.Observer_GrainCallFilter_Incoming_ExceptionHandling_Test",
    async () => {
      const observer = new MethodInterceptionGrainObserver();
      const grain = client.createObjectReference(IMethodInterceptionGrainObserver, observer);

      const result = await grain.doThrow();
      expect(result).toBe("EXCEPTION! Oi!");
      client.deleteObjectReference(grain);
    },
  );

  orleansTest(
    "UnitTests.General.GrainCallFilterTests.Observer_GrainCallFilter_Incoming_FilterThrows_Test",
    async () => {
      const observer = new MethodInterceptionGrainObserver();
      const grain = client.createObjectReference(IMethodInterceptionGrainObserver, observer);

      await expect(grain.filterThrows()).rejects.toThrow("Filter THROW!");
      client.deleteObjectReference(grain);
    },
  );

  orleansTest.excluded(
    ".NET-specific: relies on CLR generic Task<T> result-casting (InvalidCastException); no analogue for untyped JS runtime values",
    "UnitTests.General.GrainCallFilterTests.Observer_GrainCallFilter_Incoming_SetIncorrectResultType_Test",
  );
});
