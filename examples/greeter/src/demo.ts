import { GrainId } from "@thresh/core/grain-id";
import { SiloAddress } from "@thresh/core/silo-address";
import { FakeTimeProvider } from "@thresh/core/test-support/fake-time-provider";
import { InProcessNetwork } from "@thresh/messaging/in-process-transport";
import { createSilo } from "@thresh/hosting/silo-builder";
import { GreeterGrain } from "@thresh/example-greeter/greeter-grain";
import { IGreeter } from "@thresh/example-greeter/interfaces";

export interface GreeterDemoResult {
  /** The first greeting — proves onActivate ran before it (carries the prefix). */
  firstGreeting: string;
  /** Count after several concurrent greets — proves serialized turns (no lost ++). */
  countAfterConcurrent: number;
  /** Whether the grain deactivated once idle. */
  deactivatedWhenIdle: boolean;
  /** Count on the first call after reactivation — proves volatile state reset. */
  countAfterReactivation: number;
}

const tick = () => new Promise((r) => setTimeout(r, 0));

/**
 * Runs the greeter end-to-end with no providers — just the core actor model
 * over the in-process transport. Uses a fake clock to force idle deactivation
 * deterministically.
 */
export async function runGreeterDemo(): Promise<GreeterDemoResult> {
  const time = new FakeTimeProvider();
  const local = new SiloAddress("silo-0", "uid-0", "silo-0:11111");

  const silo = createSilo({
    clusterId: "greeter",
    local,
    time,
    collectionAgeSeconds: 30,
    collectionIntervalSeconds: 10,
  })
    .useStaticMembership([local])
    .useInProcessTransport(new InProcessNetwork())
    .registerGrain(GreeterGrain.grain, { interfaces: [IGreeter] })
    .build();

  await silo.start();
  try {
    const greeter = silo.getGrain(IGreeter, "en");

    const firstGreeting = await greeter.greet("Ada");

    // Fire several calls at once; serialized turns mean every ++ is counted.
    await Promise.all([greeter.greet("Grace"), greeter.greet("Linus"), greeter.greet("Edsger")]);
    const countAfterConcurrent = await greeter.greetings();

    // Idle past the collection age; the sweep deactivates the grain.
    time.advance(60_000);
    await tick();
    const deactivatedWhenIdle = !silo.isActive(new GrainId("Greeter", "en"));

    // Next call reactivates fresh — the volatile count restarts at 1.
    await greeter.greet("Alan");
    const countAfterReactivation = await greeter.greetings();

    return {
      firstGreeting,
      countAfterConcurrent,
      deactivatedWhenIdle,
      countAfterReactivation,
    };
  } finally {
    await silo.stop();
  }
}
