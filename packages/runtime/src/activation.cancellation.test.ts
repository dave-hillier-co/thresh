import { describe, expect, it } from "vitest";
import { GrainCallAbortedError } from "@thresh/core/errors";
import { CancellationTokenPlaceholder } from "@thresh/core/grain-cancellation-token";
import { Grain } from "@thresh/core/grain";
import { GrainId } from "@thresh/core/grain-id";
import type { InvocationRequest } from "@thresh/core/request";
import type { DeactivationReason } from "@thresh/core/reasons";
import { ActivationData } from "@thresh/runtime/activation";
import { GrainFactory } from "@thresh/runtime/grain-factory";
import { GrainRuntimeImpl } from "@thresh/runtime/grain-runtime-impl";
import { FakeTimeProvider } from "@thresh/runtime/test-support/fake-time-provider";

// GAP-CANCELLATION-AMBIENT (issue #18): ambient per-call cancellation and
// deadlines threaded through the InvocationRequest/InvocationContext/
// TurnScheduler/onDeactivate chokepoints.

let gate: { promise: Promise<void>; resolve: () => void } | undefined;

class ProbeGrain extends Grain {
  lastDeactivateSignal: AbortSignal | undefined;
  lastDeactivateSignalGiven = false;

  async signalAborted(): Promise<boolean> {
    return this.runtime.getCancellationSignal()?.aborted ?? false;
  }

  async hasSignal(): Promise<boolean> {
    return this.runtime.getCancellationSignal() !== undefined;
  }

  async probeWithToken(_token: unknown): Promise<boolean> {
    return this.runtime.getCancellationSignal()?.aborted ?? false;
  }

  /** Snapshots `getCancellationSignal()?.aborted` only after `gate` resolves, for cross-await observation. */
  async waitThenCheckSignal(): Promise<boolean> {
    await gate!.promise;
    return this.runtime.getCancellationSignal()?.aborted ?? false;
  }

  override async onDeactivate(_reason: DeactivationReason, signal?: AbortSignal): Promise<void> {
    this.lastDeactivateSignalGiven = signal !== undefined;
    this.lastDeactivateSignal = signal;
  }
}

const id = new GrainId("ProbeGrain", "a");

function makeActivation(time: FakeTimeProvider): ActivationData {
  const activation = new ActivationData(id, time, 30_000, false, "act-1");
  const grain = new ProbeGrain();
  grain.setContext(activation);
  activation.instance = grain;
  const factory = new GrainFactory(() => {
    throw new Error("not used in this test");
  });
  activation.runtime = new GrainRuntimeImpl(factory, activation);
  activation.beginActivate("incoming-call");
  return activation;
}

const call = (method: string, args: unknown[] = []): InvocationRequest => ({
  target: id,
  interfaceId: 0,
  method,
  args,
  options: {},
  reentrancyId: "r1",
});

describe("ambient cancellation on ActivationData.invoke", () => {
  it("has no ambient signal when neither opts.signal nor req.deadline is given", async () => {
    const activation = makeActivation(new FakeTimeProvider());
    await expect(activation.invoke(call("hasSignal"))).resolves.toBe(false);
  });

  it("exposes opts.signal via GrainRuntime.getCancellationSignal() inside the turn", async () => {
    const activation = makeActivation(new FakeTimeProvider());
    const controller = new AbortController();

    await expect(
      activation.invoke(call("signalAborted"), { signal: controller.signal }),
    ).resolves.toBe(false);
  });

  it("lets an already-running turn observe its signal firing mid-turn, without being preempted", async () => {
    const activation = makeActivation(new FakeTimeProvider());
    const controller = new AbortController();
    let resolveGate!: () => void;
    gate = {
      promise: new Promise<void>((r) => {
        resolveGate = r;
      }),
      resolve: () => resolveGate(),
    };

    const pending = activation.invoke(call("waitThenCheckSignal"), { signal: controller.signal });
    await new Promise((r) => setTimeout(r, 0)); // let the turn start and reach the gate

    controller.abort(); // fires after the turn already started
    gate.resolve();

    // The turn ran to completion (was not preempted) and observed the fired signal.
    await expect(pending).resolves.toBe(true);
  });

  it("derives a present-but-unfired ambient signal from a req.deadline still in the future", async () => {
    const time = new FakeTimeProvider();
    const activation = makeActivation(time);

    await expect(
      activation.invoke({ ...call("hasSignal"), deadline: time.now() + 50 }),
    ).resolves.toBe(true); // signal present
    await expect(
      activation.invoke({ ...call("signalAborted"), deadline: time.now() + 50 }),
    ).resolves.toBe(false); // but not yet fired
  });

  it("rejects a not-yet-started turn with GrainCallAbortedError when its deadline has already passed", async () => {
    const time = new FakeTimeProvider();
    const activation = makeActivation(time);
    time.advance(1000);

    // A deadline in the past derives an already-aborted signal synchronously
    // (`deadlineSignal`), so the turn is rejected at admission — it never runs
    // the grain method at all (Orleans has no analogue: JS-only ambient
    // cancellation).
    await expect(
      activation.invoke({ ...call("signalAborted"), deadline: time.now() - 1 }),
    ).rejects.toBeInstanceOf(GrainCallAbortedError);
  });

  it("still runs an already-started turn to completion even if its deadline passes mid-turn", async () => {
    const time = new FakeTimeProvider();
    let released: (() => void) | undefined;
    class SlowGrain extends Grain {
      async slow(): Promise<string> {
        await new Promise<void>((resolve) => {
          released = resolve;
        });
        return "finished";
      }
    }
    const slowId = new GrainId("SlowGrain", "a");
    const activation = new ActivationData(slowId, time, 30_000, false, "act-1");
    const grain = new SlowGrain();
    grain.setContext(activation);
    activation.instance = grain;
    activation.beginActivate("incoming-call");

    const req: InvocationRequest = {
      target: slowId,
      interfaceId: 0,
      method: "slow",
      args: [],
      options: {},
      reentrancyId: "r1",
      deadline: time.now() + 100,
    };
    const pending = activation.invoke(req);
    await new Promise((r) => setTimeout(r, 0)); // let the turn actually start
    time.advance(200); // deadline elapses while the turn is running
    released?.();
    await expect(pending).resolves.toBe("finished");
  });
});

describe("GrainCancellationToken composes into the ambient signal", () => {
  it("reflects an already-cancelled bound token in getCancellationSignal()", async () => {
    const activation = makeActivation(new FakeTimeProvider());
    const placeholder = new CancellationTokenPlaceholder("token-1", true);

    await expect(activation.invoke(call("probeWithToken", [placeholder]))).resolves.toBe(true);
  });

  it("leaves getCancellationSignal() unfired for a token that never cancels", async () => {
    const activation = makeActivation(new FakeTimeProvider());
    const placeholder = new CancellationTokenPlaceholder("token-2", false);

    await expect(activation.invoke(call("probeWithToken", [placeholder]))).resolves.toBe(false);
  });
});

describe("onDeactivate receives an optional ambient signal", () => {
  it("passes undefined through when the caller supplies none", async () => {
    const activation = makeActivation(new FakeTimeProvider());
    await activation.invoke(call("hasSignal"));
    await activation.runDeactivateHook({ code: "idle", description: "test" });
    const grain = activation.instance as ProbeGrain;
    expect(grain.lastDeactivateSignalGiven).toBe(false);
  });

  it("passes the given signal through to the grain's onDeactivate", async () => {
    const activation = makeActivation(new FakeTimeProvider());
    await activation.invoke(call("hasSignal"));
    const controller = new AbortController();
    await activation.runDeactivateHook({ code: "idle", description: "test" }, controller.signal);
    const grain = activation.instance as ProbeGrain;
    expect(grain.lastDeactivateSignal).toBe(controller.signal);
  });
});
