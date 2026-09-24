import { describe, expect, it } from "vitest";
import { grain } from "@thresh/core/decorators";
import { Grain } from "@thresh/core/grain";
import { defineGrainInterface } from "@thresh/core/grain-interface";
import type { GrainKey } from "@thresh/core/key-kinds";
import { SiloAddress } from "@thresh/core/silo-address";
import { onStreamingEvent, type StreamingEvent } from "@thresh/core/streaming-diagnostics";
import { FakeTimeProvider } from "@thresh/core/test-support/fake-time-provider";
import { InProcessNetwork } from "@thresh/messaging/in-process-transport";
import { createSilo } from "@thresh/hosting/silo-builder";

interface IFarewell extends GrainKey<string> {
  touch(): Promise<void>;
}
const IFarewell = defineGrainInterface<IFarewell>("IFarewell.memory-streams-stop");

/** Publishes one last event from `onDeactivate` — a common "I'm going away" pattern. */
@grain()
class FarewellGrain extends Grain implements IFarewell {
  async touch(): Promise<void> {}

  override async onDeactivate(): Promise<void> {
    await this.runtime
      .getStreamProvider()
      .getStream<string>("farewells", this.id.key)
      .publish("bye");
  }
}

const local = new SiloAddress("silo-0", "uid-0", "silo-0:11111");

describe("memory streams on silo stop", () => {
  // The memory provider's `stop()` only cancels per-stream inactivity timers —
  // it doesn't stop delivery — and a publish re-arms them. It must therefore
  // run AFTER activations deactivate, or a publish from an `onDeactivate` hook
  // arms a timer that outlives the silo.
  it("leaves no inactivity timer armed by a publish from an onDeactivate hook", async () => {
    const time = new FakeTimeProvider();
    const silo = createSilo({ clusterId: "c1", local, time })
      .useStaticMembership([local])
      .useInProcessTransport(new InProcessNetwork())
      .useMemoryStreams("default", { streamInactivityPeriodMs: 5_000 })
      .registerGrain(FarewellGrain, { interfaces: [IFarewell] })
      .build();
    await silo.start();
    await silo.getGrain(IFarewell, "f1").touch();
    await silo.stop();

    const inactive: StreamingEvent[] = [];
    const off = onStreamingEvent((event) => {
      if (event.kind === "streamInactive") inactive.push(event);
    });
    try {
      time.advance(5_000);
      expect(inactive).toEqual([]);
    } finally {
      off();
    }
  });
});
