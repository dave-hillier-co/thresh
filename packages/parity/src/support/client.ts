import type { GrainRegistrationSpec } from "@thresh/core/grain-registration-spec";
import type {
  IncomingGrainCallFilter,
  OutgoingGrainCallFilter,
} from "@thresh/core/grain-call-filter";
import { SiloAddress } from "@thresh/core/silo-address";
import { InProcessTransport } from "@thresh/messaging/in-process-transport";
import { createClient, type ClientNode } from "@thresh/client/client-node";
import type { TestCluster } from "@thresh/testing/test-cluster";

/**
 * Build a `ClientNode` joined to `cluster` through its primary silo as
 * gateway, mirroring the wiring in
 * `packages/client/src/observer-callback.cluster.test.ts`. Registers `grains`
 * so the client can address them via `getGrain`/`createObjectReference`.
 * `incomingCallFilters` wraps calls into this client's hosted objects
 * (Orleans' client-builder `AddIncomingGrainCallFilter` — the
 * `Observer_GrainCallFilter_Incoming_*` tests use this to mirror the silo's
 * own filter wiring on the client side). `outgoingCallFilters` wraps calls
 * this client issues (Orleans' `AddOutgoingGrainCallFilter` — the
 * `GrainCallTraceContextPropagationTests` port uses this to inject W3C trace
 * context on client-originated calls).
 */
export async function createClusterClient(
  cluster: TestCluster,
  grains: readonly GrainRegistrationSpec[],
  incomingCallFilters?: readonly IncomingGrainCallFilter[],
  outgoingCallFilters?: readonly OutgoingGrainCallFilter[],
): Promise<ClientNode> {
  const local = new SiloAddress("test-client", "uid-client", "test-client:22222");
  const client = createClient({
    clusterId: cluster.clusterId,
    local,
    transport: new InProcessTransport(cluster.network, cluster.clusterId),
    gateway: cluster.primary.address,
    ...(incomingCallFilters !== undefined ? { incomingCallFilters } : {}),
    ...(outgoingCallFilters !== undefined ? { outgoingCallFilters } : {}),
  }).registerGrains(grains);
  await client.connect();
  return client;
}
