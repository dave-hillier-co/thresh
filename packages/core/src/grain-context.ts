import type { GrainId } from "./grain-id";
import type { GrainRuntime } from "./grain-runtime";

/** The runtime context a grain instance is bound to: its identity and services. */
export interface GrainContext {
  readonly id: GrainId;
  readonly runtime: GrainRuntime;
}
