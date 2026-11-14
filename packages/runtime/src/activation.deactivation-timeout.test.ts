import { describe, expect, it } from "vitest";
import { Grain } from "@thresh/core/grain";
import { GrainId } from "@thresh/core/grain-id";
import type { InvocationRequest } from "@thresh/core/request";
import { ActivationData } from "@thresh/runtime/activation";
import { FakeTimeProvider } from "@thresh/runtime/test-support/fake-time-provider";

/**
 * What the deactivation timeout measures from. Orleans arms it when the deactivation is
 * REQUESTED, not when `OnDeactivateAsync` starts: `ActivationData.Deactivate` does
 * `cts.CancelAfter(CollectionOptions.DeactivationTimeout)` as it schedules the deactivate command
 * (src/Orleans.Runtime/Catalog/ActivationData.cs:643-645, Orleans v10.1.0-86-gf704b02a7), and that command runs only once the requests in flight
 * have drained; `FinishDeactivating` then awaits
 * `OnDeactivateAsync(DeactivationReason, cancellationToken).WaitAsync(cancellationToken)`
 * (ActivationData.cs:1972) on the same token. So a drain that takes most of the timeout leaves
 * the hook only what remains, and one that outlasts it leaves the hook no time at all. These pin
 * that Thresh does the same.
 */

const id = new GrainId("DeactivationTimeoutGrain", "a");
const TIMEOUT_MS = 1_000;

const flush = async (): Promise<void> => {
  for (let i = 0; i < 10; i += 1) await new Promise((resolve) => setImmediate(resolve));
};

class DrainingGrain extends Grain {
  hookStartedAt: number | undefined;

  constructor(private readonly clock: FakeTimeProvider) {
    super();
  }
  private finishWork!: () => void;

  async work(): Promise<string> {
    await new Promise<void>((resolve) => (this.finishWork = resolve));
    return "done";
  }

  /** Let the request in flight finish after `ms` on the fake clock. */
  finishWorkAfter(time: FakeTimeProvider, ms: number): void {
    time.setTimer(() => this.finishWork(), ms);
  }

  override async onDeactivate(): Promise<void> {
    this.hookStartedAt = this.clock.now();
    await new Promise<never>(() => {}); // never finishes on its own
  }
}

const workCall: InvocationRequest = {
  target: id,
  interfaceId: 0,
  method: "work",
  args: [],
  options: {},
  reentrancyId: "r-work",
};

async function drainingActivation(
  time: FakeTimeProvider,
): Promise<{ activation: ActivationData; grain: DrainingGrain }> {
  const activation = new ActivationData(id, time, 30_000, false, "act-1", {
    deactivationTimeoutMs: TIMEOUT_MS,
  });
  const grain = new DrainingGrain(time);
  grain.setContext(activation);
  activation.instance = grain;
  activation.beginActivate("incoming-call");
  await flush();
  return { activation, grain };
}

describe("the deactivation timeout counts from the deactivation request (Orleans DeactivationTimeout)", () => {
  it("leaves the hook only what a long drain did not use", async () => {
    const time = new FakeTimeProvider();
    const { activation, grain } = await drainingActivation(time);
    const working = activation.invoke(workCall);
    await flush();

    let finished = false;
    const deactivating = activation
      .deactivate({ code: "application-requested", description: "test" })
      .then(() => (finished = true));
    grain.finishWorkAfter(time, 900);

    time.advance(900); // the drain takes 90% of the timeout
    await expect(working).resolves.toBe("done");
    await flush();
    expect(grain.hookStartedAt).toBe(900);

    time.advance(99);
    await flush();
    expect(finished).toBe(false);

    time.advance(1); // 1 000 ms after the request, 100 ms into the hook
    await deactivating;
    expect(activation.state).toBe("invalid");
  });

  it("gives the hook no time when the drain outlasts the timeout", async () => {
    const time = new FakeTimeProvider();
    const { activation, grain } = await drainingActivation(time);
    const working = activation.invoke(workCall);
    await flush();

    const deactivating = activation.deactivate({
      code: "application-requested",
      description: "test",
    });
    grain.finishWorkAfter(time, 1_200);

    time.advance(1_200);
    await expect(working).resolves.toBe("done");
    await deactivating; // no further time passes: the hook starts and is abandoned at once
    expect(grain.hookStartedAt).toBe(1_200);
    expect(activation.state).toBe("invalid");
  });
});
