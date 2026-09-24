import { describe, expect, it } from "vitest";
import { Grain } from "@thresh/core/grain";
import { GrainId } from "@thresh/core/grain-id";
import { ActivationData } from "@thresh/runtime/activation";
import { FakeTimeProvider } from "@thresh/runtime/test-support/fake-time-provider";

// Mirrors Orleans' `ActivationData.DelayDeactivation` (ActivationData.cs:486-509):
// a non-positive delay cancels an active keep-alive and reverts to normal
// collection; a positive delay REPLACES whatever keep-alive was in effect,
// rather than only ever being able to extend it.

const id = new GrainId("Delayed", "a");
const flush = (): Promise<void> => new Promise((r) => setTimeout(r, 0));

class NoopGrain extends Grain {}

async function makeActivation(
  collectionAgeMs = 1000,
): Promise<{ activation: ActivationData; time: FakeTimeProvider }> {
  const time = new FakeTimeProvider();
  const activation = new ActivationData(id, time, collectionAgeMs, false, "act-1");
  const grain = new NoopGrain();
  grain.setContext(activation);
  activation.instance = grain;
  activation.beginActivate("incoming-call");
  await flush(); // let the activation turn settle to "valid"
  return { activation, time };
}

describe("ActivationData.delayDeactivation", () => {
  it("keeps the activation alive past its normal collection age while the delay is in effect", async () => {
    const { activation, time } = await makeActivation(1000);
    activation.delayDeactivation(5000);
    time.advance(2000); // past the 1000ms collection age, still under the 5000ms keep-alive
    expect(activation.isStale()).toBe(false);
    time.advance(4000); // now past the keep-alive too
    expect(activation.isStale()).toBe(true);
  });

  it("shortens (replaces, not extends) an already-active keep-alive", async () => {
    const { activation, time } = await makeActivation(1000);
    activation.delayDeactivation(10_000);
    // A shorter second call must actually shorten it, not be ignored because
    // it is smaller than the first (the old `Math.max` folding).
    activation.delayDeactivation(2000);
    time.advance(3000);
    expect(activation.isStale()).toBe(true);
  });

  it("cancels an active keep-alive when the delay is zero or negative, reverting to normal collection", async () => {
    const { activation, time } = await makeActivation(1000);
    activation.delayDeactivation(10_000);
    activation.delayDeactivation(0);
    time.advance(1500); // past the normal collection age, well under the cancelled 10s keep-alive
    expect(activation.isStale()).toBe(true);
  });
});
