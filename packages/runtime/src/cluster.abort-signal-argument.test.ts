import { describe, expect, it } from "vitest";
import { grain } from "@thresh/core/decorators";
import { Grain } from "@thresh/core/grain";
import { defineGrainInterface } from "@thresh/core/grain-interface";
import type { GrainWithStringKey } from "@thresh/core/key-kinds";
import { TestCluster, type TestSiloHandle } from "@thresh/testing/test-cluster";
import { waitFor } from "@thresh/testing/wait";

/**
 * A plain `AbortSignal` in a grain method's argument slot — the shape a ported .NET
 * `CancellationToken` parameter takes (`docs/orleans-to-thresh-port.md`, "Concurrency and
 * cancellation"). The codec has no representation for a signal, so without the grain-factory's
 * conversion a cross-silo callee receives an inert plain object and faults the moment it touches
 * `signal.aborted` — a failure that a same-silo test can never see, because a local call hands the
 * argument over by reference.
 */
interface ISignalGrain extends GrainWithStringKey {
  /** What the callee actually received: whether it is a signal, and whether it has fired. */
  inspect(signal?: AbortSignal): Promise<{ isSignal: boolean; aborted: boolean }>;
  /** Park until the received signal fires, so a LATER abort has to reach this activation. */
  watch(signal?: AbortSignal): Promise<void>;
  /** Whether `watch`'s signal has fired on this activation yet. */
  fired(): Promise<boolean>;
  /** The same question, for a signal reached through an object argument. */
  inspectNested(request: NestedRequest): Promise<{ isSignal: boolean; aborted: boolean }>;
  /** Park until the signal nested in `request` fires. */
  watchNested(request: NestedRequest): Promise<void>;
  /** A signal buried under an array, a `Map` and a `Set`, to prove the walk is not one level deep. */
  inspectDeep(request: DeepRequest): Promise<{ isSignal: boolean; aborted: boolean }>;
  /** A nested grain reference beside a nested signal: both must arrive usable. */
  inspectWithReference(
    request: ReferenceRequest,
  ): Promise<{ isSignal: boolean; refAnswered: boolean }>;
}

/** The shape a ported .NET request record takes: the token travels INSIDE the record. */
interface NestedRequest {
  readonly name: string;
  readonly signal?: AbortSignal;
  /** Present only in the cycle case, where it points back at the request itself. */
  self?: unknown;
}

interface DeepRequest {
  readonly steps: readonly { readonly by: Map<string, { readonly signal?: AbortSignal }> }[];
  readonly tags: ReadonlySet<string>;
}

interface ReferenceRequest {
  readonly ref: ISignalGrain;
  readonly signal?: AbortSignal;
}
const ISignalGrain = defineGrainInterface<ISignalGrain>("test.ISignalGrain.abortArg");

@grain()
class SignalGrain extends Grain implements ISignalGrain {
  private sawAbort = false;

  async inspect(signal?: AbortSignal): Promise<{ isSignal: boolean; aborted: boolean }> {
    return { isSignal: signal instanceof AbortSignal, aborted: signal?.aborted === true };
  }

  async watch(signal?: AbortSignal): Promise<void> {
    if (signal === undefined) throw new Error("watch: no signal");
    if (signal.aborted) {
      this.sawAbort = true;
      return;
    }
    signal.addEventListener(
      "abort",
      () => {
        this.sawAbort = true;
      },
      { once: true },
    );
  }

  async fired(): Promise<boolean> {
    return this.sawAbort;
  }

  async inspectNested(request: NestedRequest): Promise<{ isSignal: boolean; aborted: boolean }> {
    const signal = request.signal;
    return { isSignal: signal instanceof AbortSignal, aborted: signal?.aborted === true };
  }

  async watchNested(request: NestedRequest): Promise<void> {
    await this.watch(request.signal);
  }

  async inspectDeep(request: DeepRequest): Promise<{ isSignal: boolean; aborted: boolean }> {
    const signal = request.steps[0]?.by.get("a")?.signal;
    return { isSignal: signal instanceof AbortSignal, aborted: signal?.aborted === true };
  }

  async inspectWithReference(
    request: ReferenceRequest,
  ): Promise<{ isSignal: boolean; refAnswered: boolean }> {
    const refAnswered = (await request.ref.fired()) === false;
    return { isSignal: request.signal instanceof AbortSignal, refAnswered };
  }
}

/** Two silos, `random -> 0` so every activation lands on silo-0 and silo-1's calls cross the wire. */
function buildCluster() {
  return TestCluster.start({
    clusterId: "abort-signal-argument-cluster",
    initialSilos: 2,
    random: () => 0,
    grains: [{ ctor: SignalGrain, interfaces: [ISignalGrain] }],
  });
}

describe("an AbortSignal argument crossing a grain call", () => {
  it("arrives at a CROSS-SILO callee as a real AbortSignal", async () => {
    const cluster = await buildCluster();
    try {
      const caller: TestSiloHandle = cluster.silos[1]!;
      const controller = new AbortController();
      const seen = await caller.host.getGrain(ISignalGrain, "g1").inspect(controller.signal);
      expect(seen).toEqual({ isSignal: true, aborted: false });
    } finally {
      await cluster.dispose();
    }
  });

  it("carries an ALREADY-aborted signal's state to a cross-silo callee", async () => {
    const cluster = await buildCluster();
    try {
      const caller: TestSiloHandle = cluster.silos[1]!;
      const controller = new AbortController();
      controller.abort();
      const seen = await caller.host.getGrain(ISignalGrain, "g2").inspect(controller.signal);
      expect(seen).toEqual({ isSignal: true, aborted: true });
    } finally {
      await cluster.dispose();
    }
  });

  it("propagates an abort raised AFTER the call to the cross-silo callee", async () => {
    const cluster = await buildCluster();
    try {
      const caller: TestSiloHandle = cluster.silos[1]!;
      const grain = caller.host.getGrain(ISignalGrain, "g3");
      const controller = new AbortController();
      await grain.watch(controller.signal);
      expect(await grain.fired()).toBe(false);

      controller.abort();
      await waitFor(async () => await grain.fired());
    } finally {
      await cluster.dispose();
    }
  });

  it("delivers a signal to a SAME-SILO callee too, so the callee sees the same shape either way", async () => {
    const cluster = await buildCluster();
    try {
      const caller: TestSiloHandle = cluster.silos[0]!;
      const controller = new AbortController();
      const seen = await caller.host.getGrain(ISignalGrain, "g4").inspect(controller.signal);
      expect(seen).toEqual({ isSignal: true, aborted: false });
    } finally {
      await cluster.dispose();
    }
  });
});

/**
 * The same contract for a signal reached THROUGH an object — the shape a ported .NET
 * `CancellationToken` takes when it rides inside a request record rather than occupying its own
 * parameter slot. The grain factory's conversion and the two callee-side unwraps must both reach
 * it, or the callee is handed an inert object (cross-silo) or a token where its signature declares
 * a signal (same-silo).
 */
describe("an AbortSignal nested inside an object argument", () => {
  it("arrives at a CROSS-SILO callee as a real AbortSignal", async () => {
    const cluster = await buildCluster();
    try {
      const caller: TestSiloHandle = cluster.silos[1]!;
      const controller = new AbortController();
      const seen = await caller.host
        .getGrain(ISignalGrain, "n1")
        .inspectNested({ name: "n1", signal: controller.signal });
      expect(seen).toEqual({ isSignal: true, aborted: false });
    } finally {
      await cluster.dispose();
    }
  });

  it("carries an ALREADY-aborted nested signal's state to a cross-silo callee", async () => {
    const cluster = await buildCluster();
    try {
      const caller: TestSiloHandle = cluster.silos[1]!;
      const controller = new AbortController();
      controller.abort();
      const seen = await caller.host
        .getGrain(ISignalGrain, "n2")
        .inspectNested({ name: "n2", signal: controller.signal });
      expect(seen).toEqual({ isSignal: true, aborted: true });
    } finally {
      await cluster.dispose();
    }
  });

  it("propagates an abort raised AFTER the call to the cross-silo callee", async () => {
    const cluster = await buildCluster();
    try {
      const caller: TestSiloHandle = cluster.silos[1]!;
      const grain = caller.host.getGrain(ISignalGrain, "n3");
      const controller = new AbortController();
      await grain.watchNested({ name: "n3", signal: controller.signal });
      expect(await grain.fired()).toBe(false);

      controller.abort();
      await waitFor(async () => await grain.fired());
    } finally {
      await cluster.dispose();
    }
  });

  it("delivers a nested signal to a SAME-SILO callee too, so the callee sees the same shape either way", async () => {
    const cluster = await buildCluster();
    try {
      const caller: TestSiloHandle = cluster.silos[0]!;
      const controller = new AbortController();
      const seen = await caller.host
        .getGrain(ISignalGrain, "n4")
        .inspectNested({ name: "n4", signal: controller.signal });
      expect(seen).toEqual({ isSignal: true, aborted: false });
    } finally {
      await cluster.dispose();
    }
  });

  it("leaves the CALLER's own record untouched", async () => {
    const cluster = await buildCluster();
    try {
      const caller: TestSiloHandle = cluster.silos[1]!;
      const controller = new AbortController();
      const request: NestedRequest = { name: "n5", signal: controller.signal };
      await caller.host.getGrain(ISignalGrain, "n5").inspectNested(request);
      // The conversion copies the containers on the path to the signal; the caller's own record
      // still holds the caller's own signal.
      expect(request.signal).toBe(controller.signal);
    } finally {
      await cluster.dispose();
    }
  });

  it("reaches a signal under an array, a Map and a Set", async () => {
    const cluster = await buildCluster();
    try {
      const caller: TestSiloHandle = cluster.silos[1]!;
      const controller = new AbortController();
      controller.abort();
      const seen = await caller.host.getGrain(ISignalGrain, "n6").inspectDeep({
        steps: [{ by: new Map([["a", { signal: controller.signal }]]) }],
        tags: new Set(["x", "y"]),
      });
      expect(seen).toEqual({ isSignal: true, aborted: true });
    } finally {
      await cluster.dispose();
    }
  });

  it("survives a CYCLIC argument graph (same-silo, where a cycle is legal)", async () => {
    const cluster = await buildCluster();
    try {
      const caller: TestSiloHandle = cluster.silos[0]!;
      const controller = new AbortController();
      const request: NestedRequest = { name: "n7", signal: controller.signal };
      request.self = request;
      const seen = await caller.host.getGrain(ISignalGrain, "n7").inspectNested(request);
      expect(seen).toEqual({ isSignal: true, aborted: false });
    } finally {
      await cluster.dispose();
    }
  });

  it("does not damage a grain reference sitting beside the nested signal", async () => {
    const cluster = await buildCluster();
    try {
      const caller: TestSiloHandle = cluster.silos[1]!;
      const controller = new AbortController();
      const seen = await caller.host.getGrain(ISignalGrain, "n8").inspectWithReference({
        ref: caller.host.getGrain(ISignalGrain, "n8-ref"),
        signal: controller.signal,
      });
      expect(seen).toEqual({ isSignal: true, refAnswered: true });
    } finally {
      await cluster.dispose();
    }
  });
});
