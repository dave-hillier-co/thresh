import { defineGrainInterface } from "@thresh/core/grain-interface";
import type { GrainWithStringKey } from "@thresh/core/key-kinds";

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
export interface IThermostat extends GrainWithStringKey {
  onUpdate(status: ThermostatStatus): Promise<Command[]>;
}

/** Called by control systems. */
export interface IThermostatControl extends GrainWithStringKey {
  getStatus(): Promise<ThermostatStatus>;
  updateConfiguration(config: ThermostatConfiguration): Promise<void>;
}

/** Rolls up telemetry across the fleet. */
export interface IFleetAggregator extends GrainWithStringKey {
  averageTemp(): Promise<number>;
  sampleCount(): Promise<number>;
}

export const IThermostat = defineGrainInterface<IThermostat>("example.IThermostat");

export const IThermostatControl = defineGrainInterface<IThermostatControl>(
  "example.IThermostatControl",
  { options: { getStatus: { readOnly: true } } },
);

export const IFleetAggregator = defineGrainInterface<IFleetAggregator>("example.IFleetAggregator", {
  options: { averageTemp: { readOnly: true }, sampleCount: { readOnly: true } },
});

export const TELEMETRY = "telemetry";
