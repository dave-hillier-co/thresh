import { type Logger, defaultLogger } from "@thresh/core/logger";
import type { InvocationRequest } from "@thresh/core/request";
import { mayResend } from "@thresh/messaging/resend-policy";
import { admit } from "@thresh/runtime/admission";
import type { Catalog } from "@thresh/runtime/catalog";
import {
  withCallDeadline,
  type Dispatcher,
  type InvokeCallOptions,
} from "@thresh/runtime/dispatcher";
import { dispatchDetachingOneWay } from "@thresh/runtime/one-way-dispatch";

/**
 * Phase-1 dispatcher: every grain lives in this one process, so a call is
 * delivered straight to its (possibly newly created) local activation. Phase 2
 * replaces this with directory + transport routing.
 */
export class LocalDispatcher implements Dispatcher {
  constructor(
    private readonly catalog: Catalog,
    /** Where a detached `oneWay` failure is reported; see `dispatchDetachingOneWay`. */
    private readonly logger: Logger = defaultLogger,
  ) {}

  invoke(req: InvocationRequest, opts?: InvokeCallOptions): Promise<unknown> {
    // A `oneWay` call resolves to the caller as soon as delivery is under way,
    // exactly as it does when the callee lives on another silo — the delivery
    // below still runs, detached, to completion.
    return dispatchDetachingOneWay(req, this.logger, () => this.deliver(req, opts));
  }

  private async deliver(req: InvocationRequest, opts?: InvokeCallOptions): Promise<unknown> {
    const withDeadline = withCallDeadline(req, opts);
    // [StatelessWorker] grains scale to several local activations per id
    // instead of the ordinary exactly-one; see `Catalog.pickOrScaleWorker`.
    if (this.catalog.isStatelessWorkerType(withDeadline.target.type)) {
      return admit(this.catalog.pickOrScaleWorker(withDeadline.target), withDeadline, opts);
    }
    const activation = await this.catalog.getOrCreate(withDeadline.target);
    try {
      return await admit(activation, withDeadline, opts);
    } catch (error) {
      // The call never ran here: the activation was deactivated as stuck
      // (MaxRequestProcessingTime), or it was deactivating/migrating and held
      // the call until it settled (`ActivationData.invoke`'s hold-and-reroute),
      // which `admit` marks as a refusal before execution. Re-deliver it to a
      // fresh activation, as Orleans' `RerouteAllQueuedMessages` does. The one
      // resend rule (`mayResend`) admits only such refusals, so a call that did
      // run -- even one whose own body threw a "noActivation" rejection from a
      // nested call -- is never executed twice.
      if (!mayResend(error)) throw error;
      return this.deliver(req, opts);
    }
  }
}
