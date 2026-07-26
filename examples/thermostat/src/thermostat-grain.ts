import { grain, persistentState } from "@thresh/core/decorators";
import { Grain } from "@thresh/core/grain";
import type { PersistentState } from "@thresh/core/persistent-state";
import type { Remindable, TickStatus } from "@thresh/core/reminder";
import {
  TELEMETRY,
  type Command,
  type IThermostat,
  type IThermostatControl,
  type ThermostatConfiguration,
  type ThermostatStatus,
} from "@thresh/example-thermostat/interfaces";

interface ThermostatState {
  status: ThermostatStatus;
  config: ThermostatConfiguration;
  pendingCommands: Command[];
}

const defaultState = (): ThermostatState => ({
  status: { tempC: 20 },
  config: { targetC: 21 },
  pendingCommands: [],
});

/**
 * The worked Orleans thermostat: two interfaces on one grain, durable state via
 * `@persistentState`, a durable self-check reminder, and telemetry published to
 * a stream — single-threaded, no locks.
 *
 * Retained deliberately as the `@grain()` class form. Functional-first
 * (`defineGrain` + hooks) is the documented default and every other
 * example now uses it — but the class is the runtime substrate both styles
 * compile down to, so one living example keeps that path exercised and shows the
 * two interoperate: the functional `FleetAggregatorGrain` in this same example
 * consumes the telemetry this class publishes.
 */
@grain()
export class ThermostatGrain extends Grain implements IThermostat, IThermostatControl, Remindable {
  @persistentState("thermostat", { defaultValue: defaultState })
  private state!: PersistentState<ThermostatState>;

  override async onActivate(): Promise<void> {
    // A durable self-check that fires even if the grain is idle or the silo restarts.
    await this.runtime.registerReminder("self-check", { seconds: 60 }, { seconds: 60 });
  }

  async onUpdate(status: ThermostatStatus): Promise<Command[]> {
    this.state.value.status = status;
    const commands = this.state.value.pendingCommands;
    this.state.value.pendingCommands = [];
    if (status.tempC < this.state.value.config.targetC) commands.push({ kind: "heat" });
    else if (status.tempC > this.state.value.config.targetC) commands.push({ kind: "cool" });
    await this.state.write();

    await this.runtime
      .getStreamProvider()
      .getStream<ThermostatStatus>(TELEMETRY, this.id.key)
      .publish(status);

    return commands;
  }

  async getStatus(): Promise<ThermostatStatus> {
    return this.state.value.status; // read-only, served from memory
  }

  async updateConfiguration(config: ThermostatConfiguration): Promise<void> {
    this.state.value.config = config;
    await this.state.write();
  }

  async receiveReminder(name: string, _status: TickStatus): Promise<void> {
    if (name === "self-check") {
      this.state.value.pendingCommands.push({ kind: "self-test" });
      await this.state.write();
    }
  }
}
