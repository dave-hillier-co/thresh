import type { Grain } from "@tsva/core/grain";
import type { GrainInterface } from "@tsva/core/grain-interface";
import { SiloAddress } from "@tsva/core/silo-address";
import { InProcessTransport } from "@tsva/messaging/in-process-transport";
import { createClient, type ClientNode } from "@tsva/client/client-node";
import type { TestCluster } from "@tsva/testing/test-cluster";

/**
 * Build a `ClientNode` joined to `cluster` through its primary silo as
 * gateway, mirroring the wiring in
 * `packages/client/src/observer-callback.cluster.test.ts`. Registers `grains`
 * so the client can address them via `getGrain`/`createObjectReference`.
 */
export async function createClusterClient(
  cluster: TestCluster,
  grains: ReadonlyArray<{ ctor: new () => Grain; interfaces: GrainInterface<unknown>[] }>,
): Promise<ClientNode> {
  const local = new SiloAddress("test-client", "uid-client", "test-client:22222");
  const client = createClient({
    clusterId: cluster.clusterId,
    local,
    transport: new InProcessTransport(cluster.network, cluster.clusterId),
    gateway: cluster.primary.address,
  }).registerGrains([...grains]);
  await client.connect();
  return client;
}
