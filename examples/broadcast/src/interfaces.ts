import { defineGrainInterface } from "@thresh/core/grain-interface";
import type { GrainKey } from "@thresh/core/key-kinds";

/** One alert broadcast to a region's channel. */
export interface Alert {
  region: string;
  text: string;
}

/** Publishes alerts to a region's broadcast channel. */
export type AlertPublisher = GrainKey<string> & {
  raise(region: string, text: string): Promise<void>;
};
export const alertPublisher = defineGrainInterface<AlertPublisher>("alertPublisher.broadcast");

/** A per-region dashboard that shows the alerts it has received. */
export type RegionMonitor = GrainKey<string> & {
  alerts(): Promise<Alert[]>;
};
export const regionMonitor = defineGrainInterface<RegionMonitor>("regionMonitor.broadcast");

/** A per-region audit log that records every alert it has received. */
export type AuditLog = GrainKey<string> & {
  entries(): Promise<string[]>;
};
export const auditLog = defineGrainInterface<AuditLog>("auditLog.broadcast");
