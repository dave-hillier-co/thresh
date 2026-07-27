import { CoreV1Api, DiscoveryV1Api, KubeConfig, Watch } from "@kubernetes/client-node";
import type {
  EndpointSliceSource,
  RawEndpointSlice,
} from "@thresh/clustering-k8s/kubernetes-endpoint-watch";
import type { WatchEventType } from "@thresh/clustering-k8s/watched-endpoints";

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
  /**
   * List and watch Pods in `namespace` too, joining each endpoint's pod labels
   * onto it (by `targetRef.uid`) so `KubernetesMembershipOptions.metadataLabelPrefix`
   * has labels to read. EndpointSlice endpoints don't carry pod labels
   * themselves, hence the extra API calls — opt in only when metadata-aware
   * placement needs them. A label added or changed on a running pod is picked
   * up on the next EndpointSlice list/watch event (the pod watch alone doesn't
   * trigger a re-emit), not immediately.
   */
  fetchPodLabels?: boolean;
}

interface RawPod {
  metadata?: { uid?: string; labels?: Record<string, string> };
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
  const podLabels =
    options.fetchPodLabels === true ? watchPodLabels(kc, options.namespace) : undefined;

  /** Join each endpoint's pod labels in by `targetRef.uid`, when resolved. */
  function withPodLabels(raw: RawEndpointSlice): RawEndpointSlice {
    if (podLabels === undefined || raw.endpoints === undefined) return raw;
    return {
      ...raw,
      endpoints: raw.endpoints.map((endpoint) => {
        const uid = endpoint.targetRef?.uid;
        const labels = uid !== undefined ? podLabels.labels.get(uid) : undefined;
        return labels !== undefined ? { ...endpoint, metadata: labels } : endpoint;
      }),
    };
  }

  return {
    async list(): Promise<RawEndpointSlice[]> {
      const result = await api.listNamespacedEndpointSlice({
        namespace: options.namespace,
        labelSelector,
      });
      return ((result.items ?? []) as RawEndpointSlice[]).map(withPodLabels);
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
            onEvent(phase as WatchEventType, withPodLabels(apiObj as RawEndpointSlice)),
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
        podLabels?.stop();
      };
    },
  };
}

interface PodLabelCache {
  readonly labels: ReadonlyMap<string, Record<string, string>>;
  stop(): void;
}

/**
 * Lists then watches Pods in `namespace`, maintaining a live `uid -> labels`
 * map. Kept separate from the EndpointSlice list/watch so a pod-only label
 * change doesn't need to reconcile against the slice cache: the next
 * EndpointSlice event (or reconnect) simply reads the latest map when it joins
 * labels in.
 */
function watchPodLabels(kc: KubeConfig, namespace: string): PodLabelCache {
  const api = kc.makeApiClient(CoreV1Api);
  const labels = new Map<string, Record<string, string>>();
  const watchPath = `/api/v1/namespaces/${namespace}/pods`;
  let controller: AbortController | undefined;
  let aborted = false;

  function apply(pod: RawPod): void {
    const uid = pod.metadata?.uid;
    if (uid === undefined) return;
    if (pod.metadata?.labels !== undefined) labels.set(uid, pod.metadata.labels);
  }

  void api
    .listNamespacedPod({ namespace })
    .then((result) => {
      for (const pod of result.items ?? []) apply(pod as RawPod);
      const watch = new Watch(kc);
      return watch.watch(
        watchPath,
        {},
        (phase: string, apiObj: unknown) => {
          const pod = apiObj as RawPod;
          if (phase === "DELETED") {
            const uid = pod.metadata?.uid;
            if (uid !== undefined) labels.delete(uid);
          } else {
            apply(pod);
          }
        },
        () => {
          /* Pod labels are read fresh on the next EndpointSlice event; no reconnect loop here. */
        },
      );
    })
    .then((c) => {
      controller = c;
      if (aborted) c.abort();
    })
    .catch(() => {
      /* Best-effort: metadata stays absent if the pod watch can't be established. */
    });

  return {
    labels,
    stop(): void {
      aborted = true;
      controller?.abort();
    },
  };
}
