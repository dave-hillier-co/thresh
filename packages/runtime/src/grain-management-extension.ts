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
// needs `alwaysInterleave: true`, exactly like `cancelRemoteToken`
// (cancellation-extension.ts). It must also stay fast: it only flags the
// activation (`ActivationData.requestDeactivation`) and returns immediately
// rather than awaiting the full deactivate hook here. Awaiting the hook
// in-turn would deadlock — `onDeactivate` runs as an ordinary EXCLUSIVE turn,
// and `TurnScheduler.mayAdmit` only admits an exclusive turn when nothing is
// running, but THIS turn (the alwaysInterleave extension call) would still be
// running. Instead, `Catalog`'s lookup-create-store path
// (`getOrActivate`) observes the pending request lazily, on the very next
// call that resolves this activation, and finalizes the old activation there
// before creating the fresh one — which is race-free because by the time a
// caller's next call arrives, `deactivateOnIdle()`'s own turn has already
// completed and the scheduler is idle, so running `onDeactivate` inline in
// `getOrActivate` cannot deadlock the way doing it here would.
export const IGrainManagementExtension = defineGrainInterface<IGrainManagementExtension>(
  "$management",
  { extension: true, options: { deactivateOnIdle: { alwaysInterleave: true } } },
);

/**
 * The extension instance bound per-activation (`ActivationData.extensions`).
 * Holds no state of its own beyond the activation it was created for — it
 * only flags that activation for lazy, next-call finalization.
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
