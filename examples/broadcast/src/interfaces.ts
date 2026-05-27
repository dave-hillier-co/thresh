import { defineGrainInterface } from "@tsva/core/grain-interface";
import type { GrainWithStringKey } from "@tsva/core/key-kinds";

/** One alert broadcast to a region's channel. */
export interface Alert {
  region: string;
  text: string;
}

/** Publishes alerts to a region's broadcast channel. */
export interface IAlertPublisher extends GrainWithStringKey {
  raise(region: string, text: string): Promise<void>;
}
export const IAlertPublisher = defineGrainInterface<IAlertPublisher>("IAlertPublisher.broadcast");

/** A per-region dashboard that shows the alerts it has received. */
export interface IRegionMonitor extends GrainWithStringKey {
  alerts(): Promise<Alert[]>;
}
export const IRegionMonitor = defineGrainInterface<IRegionMonitor>("IRegionMonitor.broadcast");

/** A per-region audit log that records every alert it has received. */
export interface IAuditLog extends GrainWithStringKey {
  entries(): Promise<string[]>;
}
export const IAuditLog = defineGrainInterface<IAuditLog>("IAuditLog.broadcast");
