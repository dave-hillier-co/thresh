import { describe, expect, it } from "vitest";
import { grain } from "@thresh/core/decorators";
import { Grain } from "@thresh/core/grain";
import { defineGrainInterface } from "@thresh/core/grain-interface";
import type { GrainWithStringKey } from "@thresh/core/key-kinds";
import { systemTimeProvider } from "@thresh/core/time-provider";
import { Silo } from "@thresh/runtime/silo";
import { FakeTimeProvider } from "@thresh/runtime/test-support/fake-time-provider";

// Orleans exposes the silo's clock on `IGrainRuntime.TimeProvider`
// (src/Orleans.Runtime/Core/GrainRuntime.cs), so a grain holding something
// TTL-based (an `ObserverManager`) drives it from the silo's configured clock
// rather than pinning it to the system one.
interface IClockReader extends GrainWithStringKey {
  readNow(): Promise<number>;
  isSystemClock(): Promise<boolean>;
}
const IClockReader = defineGrainInterface<IClockReader>("IClockReader");

@grain()
class ClockReaderGrain extends Grain implements IClockReader {
  async readNow(): Promise<number> {
    return this.runtime.timeProvider.now();
  }
  async isSystemClock(): Promise<boolean> {
    return this.runtime.timeProvider === systemTimeProvider;
  }
}

function newSilo(time: FakeTimeProvider): Silo {
  const silo = new Silo({
    time,
    defaultCollectionAgeSeconds: 100_000,
    collectionIntervalSeconds: 100_000,
  });
  silo.registerGrain(ClockReaderGrain, { interfaces: [IClockReader] });
  silo.start();
  return silo;
}

describe("GrainRuntime.timeProvider", () => {
  it("hands a grain the silo's configured clock, not the system one", async () => {
    const time = new FakeTimeProvider();
    const reader = newSilo(time).getGrain(IClockReader, "a");

    expect(await reader.isSystemClock()).toBe(false);
    expect(await reader.readNow()).toBe(time.now());
  });

  it("advances with the silo's clock, so a TTL can be driven deterministically", async () => {
    const time = new FakeTimeProvider();
    const reader = newSilo(time).getGrain(IClockReader, "b");

    const before = await reader.readNow();
    time.advance(60_000);
    expect(await reader.readNow()).toBe(before + 60_000);
    // Real time did not move by a minute; only the fake clock did.
    expect(await reader.readNow()).not.toBe(systemTimeProvider.now());
  });
});
