import { describe, expect, it } from "vitest";
import { GrainId } from "@thresh/core/grain-id";
import { SiloAddress } from "@thresh/core/silo-address";
import { FakeTimeProvider } from "@thresh/core/test-support/fake-time-provider";
import { InProcessNetwork } from "@thresh/messaging/in-process-transport";
import { createSilo } from "@thresh/hosting/silo-builder";
import type { SiloHost } from "@thresh/hosting/silo-host";
import { GreeterGrain } from "@thresh/example-greeter/greeter-grain";
import { runGreeterDemo } from "@thresh/example-greeter/demo";

const local = new SiloAddress("silo-0", "uid-0", "silo-0:11111");
const tick = () => new Promise((r) => setTimeout(r, 0));

function buildGreeterSilo(time: FakeTimeProvider): SiloHost {
  return createSilo({
    clusterId: "greeter",
    local,
    time,
    collectionAgeSeconds: 30,
    collectionIntervalSeconds: 10,
  })
    .useStaticMembership([local])
    .useInProcessTransport(new InProcessNetwork())
    .registerGrain(GreeterGrain)
    .build();
}

describe("greeter (core actor model acceptance)", () => {
  it("runs the activate hook before the first message", async () => {
    const silo = buildGreeterSilo(new FakeTimeProvider());
    await silo.start();
    try {
      // The prefix is only set in the activate hook; seeing it proves activation ran first.
      expect(await silo.getGrain(GreeterGrain, "en").greet("Ada")).toBe(
        "[en] Hello, Ada! (greeting #1)",
      );
    } finally {
      await silo.stop();
    }
  });

  it("serializes concurrent calls so no increment is lost", async () => {
    const silo = buildGreeterSilo(new FakeTimeProvider());
    await silo.start();
    try {
      const greeter = silo.getGrain(GreeterGrain, "en");
      await Promise.all([greeter.greet("a"), greeter.greet("b"), greeter.greet("c")]);
      expect(await greeter.greetings()).toBe(3);
    } finally {
      await silo.stop();
    }
  });

  it("deactivates when idle and reactivates fresh on the next call", async () => {
    const time = new FakeTimeProvider();
    const silo = buildGreeterSilo(time);
    await silo.start();
    try {
      const greeter = silo.getGrain(GreeterGrain, "en");
      await greeter.greet("Ada");
      expect(await greeter.greetings()).toBe(1);

      time.advance(60_000);
      await tick();
      expect(silo.isActive(new GrainId("Greeter", "en"))).toBe(false);

      // Reactivated fresh: volatile count restarts at 1.
      expect(await greeter.greet("Alan")).toBe("[en] Hello, Alan! (greeting #1)");
    } finally {
      await silo.stop();
    }
  });

  it("runs the runnable demo end-to-end", async () => {
    const result = await runGreeterDemo();
    expect(result.firstGreeting).toBe("[en] Hello, Ada! (greeting #1)");
    expect(result.countAfterConcurrent).toBe(4);
    expect(result.deactivatedWhenIdle).toBe(true);
    expect(result.countAfterReactivation).toBe(1);
  });
});
