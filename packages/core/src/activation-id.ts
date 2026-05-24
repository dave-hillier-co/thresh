import { Guid } from "./guid";

/** Unique per activation incarnation; distinguishes a grain's current instance. */
export type ActivationId = string;

export function newActivationId(): ActivationId {
  return Guid.newGuid().toString();
}
