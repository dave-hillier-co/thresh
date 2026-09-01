import { defineGrainInterface } from "@thresh/core/grain-interface";
import type { GrainKey } from "@thresh/core/key-kinds";

export interface ThermostatStatus {
  tempC: number;
}

export interface ThermostatConfiguration {
  targetC: number;
}

export interface Command {
  kind: "heat" | "cool" | "self-test";
}

/** Called by the device frontend. */
export type Thermostat = GrainKey<string> & {
  onUpdate(status: ThermostatStatus): Promise<Command[]>;
};

/** Called by control systems. */
export type ThermostatControl = GrainKey<string> & {
  getStatus(): Promise<ThermostatStatus>;
  updateConfiguration(config: ThermostatConfiguration): Promise<void>;
};

export const thermostat = defineGrainInterface<Thermostat>("example.thermostat");

export const thermostatControl = defineGrainInterface<ThermostatControl>(
  "example.thermostatControl",
  { options: { getStatus: { readOnly: true } } },
);

export const TELEMETRY = "telemetry";
