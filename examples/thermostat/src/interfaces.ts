import { defineGrainInterface } from "@thresh/core/grain-interface";

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
export interface IThermostat {
  onUpdate(status: ThermostatStatus): Promise<Command[]>;
}

/** Called by control systems. */
export interface IThermostatControl {
  getStatus(): Promise<ThermostatStatus>;
  updateConfiguration(config: ThermostatConfiguration): Promise<void>;
}

export const IThermostat = defineGrainInterface<IThermostat>("example.IThermostat");

export const IThermostatControl = defineGrainInterface<IThermostatControl>(
  "example.IThermostatControl",
  { options: { getStatus: { readOnly: true } } },
);

export const TELEMETRY = "telemetry";
