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
