import { describe, expect, it } from "vitest";
import { Grain } from "@thresh/core/grain";
import { GrainId } from "@thresh/core/grain-id";
import { ActivationData } from "@thresh/runtime/activation";
import { FakeTimeProvider } from "@thresh/runtime/test-support/fake-time-provider";

// Orleans' `GrainTimerCreationOptions.KeepAlive` (via the tick message's
// `IsKeepAlive`, `ActivationData.OnCompletedRequest` — ActivationData.cs:1486)
// resets the idle timer when a keep-alive tick completes; an ordinary tick
// does not, matching `GrainTimer`'s own doc: "A timer does not keep a grain
// alive by itself" unless `keepAlive` says otherwise.

const id = new GrainId("KeepAliveTimer", "a");
const flush = (): Promise<void> => new Promise((r) => setTimeout(r, 0));

class NoopGrain extends Grain {}

async function makeActivation(
  time: FakeTimeProvider,
  collectionAgeMs = 1000,
): Promise<ActivationData> {
  const activation = new ActivationData(id, time, collectionAgeMs, false, "act-1");
  const grain = new NoopGrain();
  grain.setContext(activation);
  activation.instance = grain;
  activation.beginActivate("incoming-call");
  await flush(); // let the activation turn settle to "valid"
  return activation;
}

describe("registerTimer keepAlive", () => {
  it("resets the idle timer on a keep-alive tick, unlike an ordinary tick", async () => {
    const time = new FakeTimeProvider();
    const keepAliveActivation = await makeActivation(time, 1000);
    const plainActivation = await makeActivation(time, 1000);

    keepAliveActivation.registerTimer(async () => {}, { ms: 800 }, undefined, { keepAlive: true });
    plainActivation.registerTimer(async () => {}, { ms: 800 });

    time.advance(800);
    await flush(); // both ticks fire; only the keep-alive one should touch idle time

    time.advance(900); // total elapsed since construction: 1700ms

    // The keep-alive activation was "touched" at t=800, so only 900ms have
    // passed since — under the 1000ms collection age.
    expect(keepAliveActivation.isStale()).toBe(false);
    // The plain activation was never touched by its tick, so its full 1700ms
    // since construction counts — over the 1000ms collection age.
    expect(plainActivation.isStale()).toBe(true);
  });
});
