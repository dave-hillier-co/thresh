import { describe, expect, it } from "vitest";
import { grain } from "@thresh/core/decorators";
import { GrainTaskCanceledError } from "@thresh/core/errors";
import { Grain } from "@thresh/core/grain";
import type { GrainCancellationToken } from "@thresh/core/grain-cancellation-token";
import { defineGrainInterface } from "@thresh/core/grain-interface";
import type { GrainKey } from "@thresh/core/key-kinds";
import { TestCluster, type TestSiloHandle } from "@thresh/testing/test-cluster";

/** A grain whose `longWait` honours a `GrainCancellationToken` (Orleans cooperative cancellation). */
interface ILongWaitGrain extends GrainKey<string> {
  longWait(token: GrainCancellationToken, delayMs: number, callId: string): Promise<void>;
  wasCancelled(callId: string): Promise<boolean>;
}
const ILongWaitGrain = defineGrainInterface<ILongWaitGrain>("ILongWaitGrain.cancellation");

@grain()
class LongWaitGrain extends Grain implements ILongWaitGrain {
  private readonly cancelledCallIds = new Set<string>();

  async longWait(token: GrainCancellationToken, delayMs: number, callId: string): Promise<void> {
    await new Promise<void>((resolve, reject) => {
      if (token.isCancellationRequested) {
        this.cancelledCallIds.add(callId);
        reject(new GrainTaskCanceledError());
        return;
      }
      const timer = setTimeout(resolve, delayMs);
      token.signal.addEventListener(
        "abort",
        () => {
          clearTimeout(timer);
          this.cancelledCallIds.add(callId);
          reject(new GrainTaskCanceledError());
        },
        { once: true },
      );
    });
  }

  async wasCancelled(callId: string): Promise<boolean> {
    return this.cancelledCallIds.has(callId);
  }
}

/** Two silos, deterministic placement (random -> 0 always picks silo-0 as host). */
function buildCluster() {
  return TestCluster.start({
    clusterId: "cancellation-cluster",
    initialSilos: 2,
    random: () => 0,
    grains: [{ ctor: LongWaitGrain, interfaces: [ILongWaitGrain] }],
  });
}

describe("cooperative grain cancellation", () => {
  it("completes normally when the token is never cancelled (cross-silo)", async () => {
    const cluster = await buildCluster();
    try {
      // random -> 0 places "LongWait/g1" on silo-0; call from silo-1.
      const caller: TestSiloHandle = cluster.silos[1]!;
      const source = cluster.newCancellationTokenSource(caller);
      await caller.host.getGrain(ILongWaitGrain, "g1").longWait(source.token, 20, "call-1");
      expect(await caller.host.getGrain(ILongWaitGrain, "g1").wasCancelled("call-1")).toBe(false);
    } finally {
      await cluster.dispose();
    }
  });

  it("rejects when cancelled mid-flight, propagated cross-silo via the cancellation extension", async () => {
    const cluster = await buildCluster();
    try {
      const caller: TestSiloHandle = cluster.silos[1]!;
      const source = cluster.newCancellationTokenSource(caller);
      const call = caller.host
        .getGrain(ILongWaitGrain, "g2")
        .longWait(source.token, 10_000, "call-2");
      // Let the call actually reach and register on the hosting activation
      // before cancelling, so this exercises "cancel after the call arrived"
      // rather than the pre-cancelled/racing case covered below.
      await new Promise((resolve) => setTimeout(resolve, 20));
      await source.cancel();
      // Assert on the message, not the class: which of the cancellation shapes the callee raises
      // depends on which abort path wins the race. Error TYPE does cross the wire now (see
      // `cluster.error-fidelity.test.ts`); the grain-side state (`wasCancelled`) is what proves the
      // *callee* actually observed `GrainTaskCanceledError`.
      await expect(call).rejects.toThrow(/cancelled/i);
      expect(await caller.host.getGrain(ILongWaitGrain, "g2").wasCancelled("call-2")).toBe(true);
    } finally {
      await cluster.dispose();
    }
  });

  it("rejects promptly when the token was already cancelled before the call was sent (cross-silo)", async () => {
    const cluster = await buildCluster();
    try {
      const caller: TestSiloHandle = cluster.silos[1]!;
      const source = cluster.newCancellationTokenSource(caller);
      // Cancel before the call is even made: the wire-arrived placeholder
      // carries `cancelled: true`, so the activation pre-aborts the bound
      // controller before the grain method ever runs.
      await source.cancel();
      const call = caller.host
        .getGrain(ILongWaitGrain, "g3")
        .longWait(source.token, 10_000, "call-3");
      await expect(call).rejects.toThrow(/cancelled/i);
      expect(await caller.host.getGrain(ILongWaitGrain, "g3").wasCancelled("call-3")).toBe(true);
    } finally {
      await cluster.dispose();
    }
  });

  it("propagates the exact GrainTaskCanceledError when cancelled in-process (same silo, no wire)", async () => {
    const cluster = await buildCluster();
    try {
      // Calling from silo-0 (the grain's own host) never crosses the wire —
      // the dispatcher delivers locally — so the thrown error's identity
      // survives, unlike the cross-silo cases above.
      const caller: TestSiloHandle = cluster.silos[0]!;
      const source = cluster.newCancellationTokenSource(caller);
      const call = caller.host
        .getGrain(ILongWaitGrain, "g4")
        .longWait(source.token, 10_000, "call-4");
      await new Promise((resolve) => setTimeout(resolve, 20));
      await source.cancel();
      await expect(call).rejects.toBeInstanceOf(GrainTaskCanceledError);
      expect(await caller.host.getGrain(ILongWaitGrain, "g4").wasCancelled("call-4")).toBe(true);
    } finally {
      await cluster.dispose();
    }
  });
});
