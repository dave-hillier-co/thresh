import { type Logger, noopLogger } from "@thresh/core/logger";
import type { InvocationRequest } from "@thresh/core/request";
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
    private readonly logger: Logger = noopLogger,
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
      return this.catalog.pickOrScaleWorker(withDeadline.target).invoke(withDeadline, opts);
    }
    const activation = await this.catalog.getOrCreate(withDeadline.target);
    return activation.invoke(withDeadline, opts);
  }
}
