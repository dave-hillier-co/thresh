/** Why an activation is being created. */
export type ActivationReason = "incoming-call" | "reactivation";

export type DeactivationReasonCode =
  | "shutting-down"
  | "idle"
  | "migrating"
  | "application-requested"
  | "runtime-requested";

/** Why an activation is being deactivated, mirroring Orleans' DeactivationReason. */
export interface DeactivationReason {
  code: DeactivationReasonCode;
  description: string;
}
