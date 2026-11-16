import { defineGrainInterface } from "@thresh/core/grain-interface";
import type { ActivationData } from "@thresh/runtime/activation";

/**
 * Orleans' built-in `IGrainManagementExtension` `IGrainExtension`
 * (`Orleans.Core.Internal.IGrainManagementExtension`): a system-provided,
 * always-installed extension every grain reference can `Cast<T>()` onto to
 * ask its OWN activation to go away, without the grain's application
 * interface exposing any such method. `deactivateOnIdle()` mirrors
 * `IGrainManagementExtension.DeactivateOnIdle()`.
 */
export interface IGrainManagementExtension {
  deactivateOnIdle(): Promise<void>;
}

// `deactivateOnIdle` must be admitted even while another turn is in flight on
// the SAME activation — e.g. called right after the triggering call returns,
// or (per Orleans) from within a grain's own method via
// `this.DeactivateOnIdle()` while that very turn is still running — so it
// is `alwaysInterleave: true`, exactly like `cancelRemoteToken`
// (cancellation-extension.ts) and Orleans' `[AlwaysInterleave]` on
// `IGrainManagementExtension.DeactivateOnIdle`. It returns at once:
// `ActivationData.requestDeactivation` moves the activation to deactivating
// immediately (Orleans `Deactivate`), so every call that arrives from then on
// is held and rerouted to a fresh activation, and the deactivation pipeline
// runs as a lifecycle operation once the running turns — this one included —
// have drained. Awaiting that pipeline here would wait on this very turn.
export const IGrainManagementExtension = defineGrainInterface<IGrainManagementExtension>(
  "$management",
  { extension: true, options: { deactivateOnIdle: { alwaysInterleave: true } } },
);

/**
 * The extension instance bound per-activation (`ActivationData.extensions`).
 * Holds no state of its own beyond the activation it was created for.
 */
export class GrainManagementExtension implements IGrainManagementExtension {
  constructor(private readonly activation: ActivationData) {}

  async deactivateOnIdle(): Promise<void> {
    this.activation.requestDeactivation();
  }
}

/** Auto-install factory, always registered by the catalog (see `catalog.ts`). */
export const grainManagementExtensionFactory = (
  activation: ActivationData,
): GrainManagementExtension => new GrainManagementExtension(activation);
