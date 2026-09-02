import type { Duration } from "@thresh/core/duration";
import type { GrainId } from "@thresh/core/grain-id";

/** The codec-serialized payload of a reminder (etag is stored alongside). */
export interface ReminderData {
  grainId: GrainId;
  name: string;
  startAt: Date;
  period: Duration;
  /** Last-tick instant, if any — see `ReminderEntry.lastFiredAt` (issue: reminder double-fire on rebalance). */
  lastFiredAt?: Date;
}
