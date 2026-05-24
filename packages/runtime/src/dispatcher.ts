import type { InvocationRequest } from "@tsva/core/request";

/**
 * Routes a grain call to its target. Phase 1 dispatches locally; Phase 2
 * resolves the owning silo via the directory and forwards over the transport.
 */
export interface Dispatcher {
  invoke(req: InvocationRequest): Promise<unknown>;
}
