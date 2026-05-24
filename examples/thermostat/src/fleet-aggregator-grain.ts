import { grain } from "@tsva/core/decorators";
import { Grain } from "@tsva/core/grain";
import type { StreamHandler } from "@tsva/core/stream";
import {
  TELEMETRY,
  type IFleetAggregator,
  type ThermostatStatus,
} from "@tsva/example-thermostat/interfaces";

/** Subscribes to a device's telemetry stream and keeps a rolling average. */
@grain()
export class FleetAggregatorGrain extends Grain implements IFleetAggregator {
  private sum = 0;
  private count = 0;

  override async onActivate(): Promise<void> {
    const stream = this.runtime
      .getStreamProvider()
      .getStream<ThermostatStatus>(TELEMETRY, this.id.key);
    const existing = await stream.getSubscriptions();
    if (existing.length > 0) await existing[0]!.resume(this.handler());
    else await stream.subscribe(this.handler());
  }

  async averageTemp(): Promise<number> {
    return this.count === 0 ? 0 : this.sum / this.count;
  }

  async sampleCount(): Promise<number> {
    return this.count;
  }

  private handler(): StreamHandler<ThermostatStatus> {
    return {
      onNext: async (status) => {
        this.sum += status.tempC;
        this.count++;
      },
    };
  }
}
