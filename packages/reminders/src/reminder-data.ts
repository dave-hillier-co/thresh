import type { Duration } from "@thresh/core/duration";
import type { GrainId } from "@thresh/core/grain-id";

/** The codec-serialized payload of a reminder (etag is stored alongside). */
export interface ReminderData {
  grainId: GrainId;
  name: string;
  startAt: Date;
  period: Duration;
}
