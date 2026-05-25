import { once } from "node:events";
import { KubernetesEndpointWatch } from "@tsva/clustering-k8s/kubernetes-endpoint-watch";
import { createKubernetesClientSource } from "@tsva/clustering-k8s/kubernetes-client-source";
import { readPodEnvironment, siloAddressFromPodEnv } from "@tsva/clustering-k8s/pod-environment";
import { createSilo } from "@tsva/hosting/silo-builder";
import { CounterGrain } from "@tsva/example-k8s-silo/counter-grain";
import { ICounter } from "@tsva/example-k8s-silo/interfaces";
import { createCounterApi } from "@tsva/example-k8s-silo/http-api";

const SILO_PORT = Number(process.env.SILO_PORT ?? 11111);
const HEALTH_PORT = Number(process.env.HEALTH_PORT ?? 8080);
const API_PORT = Number(process.env.API_PORT ?? 8081);
const CLUSTER_ID = process.env.CLUSTER_ID ?? "k8s-silo";
const SERVICE_NAME = process.env.SILO_SERVICE ?? "silo-headless";
const REDIS_URL = process.env.REDIS_URL ?? "redis://redis:6379";

/**
 * Runnable silo for Kubernetes: membership from the headless Service's
 * EndpointSlices, WebSocket transport over per-pod IPs, durable state in Redis,
 * health probes, and a small HTTP API in front of the counter grain. Mirrors the
 * topology in docs/10.
 */
async function main(): Promise<void> {
  const pod = readPodEnvironment();
  const local = siloAddressFromPodEnv(SILO_PORT);
  const source = createKubernetesClientSource({
    namespace: pod.namespace,
    serviceName: SERVICE_NAME,
  });
  const watch = new KubernetesEndpointWatch(source);

  const host = createSilo({ clusterId: CLUSTER_ID, local })
    .useKubernetesMembership(watch, { portName: "silo" })
    .useWebSocketTransport()
    .addRedisStorage("default", { url: REDIS_URL })
    .useHealthEndpoints({ port: HEALTH_PORT })
    .registerGrain(CounterGrain, { interfaces: [ICounter] })
    .build();

  await host.start();

  const api = createCounterApi(host);
  api.listen(API_PORT);
  await once(api, "listening");
  console.log(`silo ${pod.podName} ready: silo=${local.endpoint} api=:${API_PORT}`);

  // SIGTERM: flip readiness (drains via the host), close the API, then exit.
  process.on("SIGTERM", () => {
    void (async () => {
      api.close();
      await host.stop();
      process.exit(0);
    })();
  });
}

main().catch((err: unknown) => {
  console.error(err);
  process.exitCode = 1;
});
