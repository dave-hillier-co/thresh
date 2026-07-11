import type { InvocationRequest } from "@tsva/core/request";
import type { Catalog } from "@tsva/runtime/catalog";
import type { Dispatcher } from "@tsva/runtime/dispatcher";

/**
 * Phase-1 dispatcher: every grain lives in this one process, so a call is
 * delivered straight to its (possibly newly created) local activation. Phase 2
 * replaces this with directory + transport routing.
 */
export class LocalDispatcher implements Dispatcher {
  constructor(private readonly catalog: Catalog) {}

  async invoke(req: InvocationRequest): Promise<unknown> {
    const activation = await this.catalog.getOrCreate(req.target);
    return activation.invoke(req);
  }
}
