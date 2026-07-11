import { describe, expect, it } from "vitest";
import { GrainCallTimeoutError } from "@tsva/core/errors";
import { defineGrainInterface } from "@tsva/core/grain-interface";
import type { GrainWithStringKey } from "@tsva/core/key-kinds";
import type { Dispatcher } from "@tsva/runtime/dispatcher";
import { GrainFactory } from "@tsva/runtime/grain-factory";
import { FakeTimeProvider } from "@tsva/runtime/test-support/fake-time-provider";

interface ISlow extends GrainWithStringKey {
  hang(): Promise<string>;
  fireAndForget(): Promise<string>;
}

const ISlow = defineGrainInterface<ISlow>("ISlow.grain-factory-timeout", {
  options: {
    hang: { responseTimeout: { seconds: 5 } },
    // A oneWay call ignores any responseTimeout: the caller never waits for a
    // response in the first place, so no deadline should race it.
    fireAndForget: { oneWay: true, responseTimeout: { seconds: 5 } },
  },
});

function buildFactory(time: FakeTimeProvider, invoke: Dispatcher["invoke"]): GrainFactory {
  const factory = new GrainFactory(() => "ISlow", time);
  factory.setDispatcher({ invoke });
  return factory;
}

describe("GrainFactory response deadline", () => {
  it("rejects with GrainCallTimeoutError when the call outlives its deadline", async () => {
    const time = new FakeTimeProvider();
    // The dispatched call never settles; only the caller's timer can end the race.
    const factory = buildFactory(time, () => new Promise(() => {}));
    const grainRef = factory.getGrain(ISlow, "a");

    const call = grainRef.hang();
    const assertion = expect(call).rejects.toBeInstanceOf(GrainCallTimeoutError);
    time.advance(5_000);
    await assertion;
  });

  it("resolves normally when the call finishes before the deadline", async () => {
    const time = new FakeTimeProvider();
    const factory = buildFactory(time, () => Promise.resolve("ok"));
    const grainRef = factory.getGrain(ISlow, "b");

    await expect(grainRef.hang()).resolves.toBe("ok");
  });

  it("does not race a deadline for a oneWay call", async () => {
    const time = new FakeTimeProvider();
    let settle: (value: string) => void = () => {};
    const pending = new Promise<string>((resolve) => {
      settle = resolve;
    });
    const factory = buildFactory(time, () => pending);
    const grainRef = factory.getGrain(ISlow, "c");

    const call = grainRef.fireAndForget();
    // Well past the 5s deadline configured on the method; a oneWay call must
    // not have been raced against it.
    time.advance(10_000);
    settle("done");

    await expect(call).resolves.toBe("done");
  });
});
