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

/**
 * Upstream's `DeactivationReasonCode` enum member names (PascalCase), used
 * only to format the reason the same way `DeactivationReason.ToString()`
 * does — `"{ReasonCode}: {Description}"` — for the `orleans.deactivation.reason`
 * tracing span tag (GAP-TRACING, DeactivationTracingTests.cs). Not used for
 * behavior anywhere; `DeactivationReasonCode` itself stays kebab-case, this is
 * purely a display mapping.
 */
const REASON_CODE_LABELS: Record<DeactivationReasonCode, string> = {
  "shutting-down": "ShuttingDown",
  idle: "ActivationIdle",
  migrating: "Migrating",
  "application-requested": "ApplicationRequested",
  "runtime-requested": "RuntimeRequested",
};

/** Format a `DeactivationReason` the way upstream's `DeactivationReason.ToString()` does. */
export function formatDeactivationReason(reason: DeactivationReason): string {
  return `${REASON_CODE_LABELS[reason.code]}: ${reason.description}`;
}
