import { SiloAddress } from "@thresh/core/silo-address";
import { FakeTimeProvider } from "@thresh/core/test-support/fake-time-provider";
import { InProcessNetwork } from "@thresh/messaging/in-process-transport";
import { createSilo } from "@thresh/hosting/silo-builder";
import { FleetAggregatorGrain } from "@thresh/example-thermostat/fleet-aggregator-grain";
import {
  IFleetAggregator,
  IThermostat,
  IThermostatControl,
  type Command,
} from "@thresh/example-thermostat/interfaces";
import { ThermostatGrain } from "@thresh/example-thermostat/thermostat-grain";

export interface DemoResult {
  commands: Command[][];
  status: { tempC: number };
  telemetrySamples: number;
  telemetryAverage: number;
  pendingAfterSelfCheck: number;
}

const tick = () => new Promise((r) => setTimeout(r, 0));

/**
 * Runs the thermostat end-to-end against in-memory providers and the in-process
 * transport: persistent state, a stream consumer, and a durable reminder. Uses a
 * fake clock so the self-check reminder can be made to fire deterministically.
 */
export async function runThermostatDemo(): Promise<DemoResult> {
  const time = new FakeTimeProvider();
  const local = new SiloAddress("silo-0", "uid-0", "silo-0:11111");

  const silo = createSilo({ clusterId: "demo", local, time })
    .useStaticMembership([local])
    .useInProcessTransport(new InProcessNetwork())
    .useMessagePackSerialization()
    .useMemoryStorage()
    .useReminders()
    .useMemoryStreams()
    .registerGrain(ThermostatGrain, { interfaces: [IThermostat, IThermostatControl] })
    .registerGrain(FleetAggregatorGrain.grain, { interfaces: [IFleetAggregator] })
    .build();

  await silo.start();
  try {
    const device = "kitchen";
    // Start the aggregator so it subscribes to the telemetry stream first.
    await silo.getGrain(IFleetAggregator, device).sampleCount();

    const thermostat = silo.getGrain(IThermostat, device);
    const commands: Command[][] = [];
    commands.push(await thermostat.onUpdate({ tempC: 18 })); // below target -> heat
    commands.push(await thermostat.onUpdate({ tempC: 23 })); // above target -> cool
    await tick(); // let stream delivery turns run

    // Telemetry rolled up from the two updates so far.
    const aggregator = silo.getGrain(IFleetAggregator, device);
    const telemetrySamples = await aggregator.sampleCount();
    const telemetryAverage = await aggregator.averageTemp();

    // Fire the durable self-check reminder; it queues a self-test command.
    time.advance(60_000);
    await tick();

    const status = await silo.getGrain(IThermostatControl, device).getStatus();
    const pending = (await thermostat.onUpdate({ tempC: 21 })).filter(
      (c) => c.kind === "self-test",
    );

    return {
      commands,
      status,
      telemetrySamples,
      telemetryAverage,
      pendingAfterSelfCheck: pending.length,
    };
  } finally {
    await silo.stop();
  }
}
