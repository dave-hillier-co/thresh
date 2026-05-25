import { DiscoveryV1Api, KubeConfig, Watch } from "@kubernetes/client-node";
import type {
  EndpointSliceSource,
  RawEndpointSlice,
} from "@tsva/clustering-k8s/kubernetes-endpoint-watch";
import type { WatchEventType } from "@tsva/clustering-k8s/watched-endpoints";

export interface KubernetesClientSourceOptions {
  /** Namespace the silo runs in (from the downward API, `POD_NAMESPACE`). */
  namespace: string;
  /** Name of the headless Service whose EndpointSlices define membership. */
  serviceName: string;
  /**
   * Kube config to use. Defaults to in-cluster config (the pod's mounted service
   * account); pass an explicit config for out-of-cluster use.
   */
  kubeConfig?: KubeConfig;
}

/** Load the in-cluster service-account config (silos run inside the cluster). */
export function inClusterConfig(): KubeConfig {
  const kc = new KubeConfig();
  kc.loadFromCluster();
  return kc;
}

/**
 * The production {@link EndpointSliceSource}: lists and watches the EndpointSlices
 * Kubernetes maintains for the headless Service, selected by the well-known
 * `kubernetes.io/service-name` label. This is the only place that touches
 * `@kubernetes/client-node`; everything above it is plain data, so the membership
 * logic stays testable without a cluster.
 */
export function createKubernetesClientSource(
  options: KubernetesClientSourceOptions,
): EndpointSliceSource {
  const kc = options.kubeConfig ?? inClusterConfig();
  const api = kc.makeApiClient(DiscoveryV1Api);
  const labelSelector = `kubernetes.io/service-name=${options.serviceName}`;
  const watchPath = `/apis/discovery.k8s.io/v1/namespaces/${options.namespace}/endpointslices`;

  return {
    async list(): Promise<RawEndpointSlice[]> {
      const result = await api.listNamespacedEndpointSlice({
        namespace: options.namespace,
        labelSelector,
      });
      return (result.items ?? []) as RawEndpointSlice[];
    },

    watch(onEvent, onClose): () => void {
      const watch = new Watch(kc);
      let aborted = false;
      let controller: AbortController | undefined;
      void watch
        .watch(
          watchPath,
          { labelSelector },
          (phase: string, apiObj: unknown) =>
            onEvent(phase as WatchEventType, apiObj as RawEndpointSlice),
          (err: unknown) => onClose(err ?? undefined),
        )
        .then((c) => {
          controller = c;
          if (aborted) c.abort();
        })
        .catch((err: unknown) => onClose(err));
      return () => {
        aborted = true;
        controller?.abort();
      };
    },
  };
}
