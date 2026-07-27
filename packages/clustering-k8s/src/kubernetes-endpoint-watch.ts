import type { EndpointSlice, EndpointSliceEndpoint } from "@thresh/clustering-k8s/endpoint-slice";
import type { EndpointWatch } from "@thresh/clustering-k8s/kubernetes-membership";
import { WatchedEndpoints, type WatchEventType } from "@thresh/clustering-k8s/watched-endpoints";

/** An EndpointSlice as returned by the Kubernetes API (the fields we read). */
export interface RawEndpointSlice {
  metadata?: { name?: string };
  endpoints?: Array<{
    addresses?: string[];
    conditions?: { ready?: boolean };
    targetRef?: { name?: string; uid?: string };
    /** Pod labels, when the source resolves them (see `EndpointSliceEndpoint.metadata`). */
    metadata?: Record<string, string>;
  }>;
  ports?: Array<{ name?: string; port?: number }>;
}

/**
 * The true boundary: a source of EndpointSlice list+watch events. The production
 * implementation wraps `@kubernetes/client-node` (see `kubernetes-client-source`);
 * tests supply a fake so the list→watch→reconnect logic here is sociable and runs
 * without a cluster.
 */
export interface EndpointSliceSource {
  /** The current full set of slices for the service. */
  list(): Promise<RawEndpointSlice[]>;
  /**
   * Start a watch. `onEvent` receives each change; `onClose` is called when the
   * watch ends (cleanly or on error). Returns a function that stops the watch.
   */
  watch(
    onEvent: (type: WatchEventType, slice: RawEndpointSlice) => void,
    onClose: (err?: unknown) => void,
  ): () => void;
}

/** Strip the API metadata, keeping the fields the failure detector reads. */
export function toEndpointSlice(raw: RawEndpointSlice): EndpointSlice {
  const slice: EndpointSlice = {};
  if (raw.ports !== undefined) slice.ports = raw.ports;
  if (raw.endpoints !== undefined) {
    slice.endpoints = raw.endpoints.map((endpoint) => {
      const mapped: EndpointSliceEndpoint = { addresses: endpoint.addresses ?? [] };
      if (endpoint.conditions !== undefined) mapped.conditions = endpoint.conditions;
      if (endpoint.targetRef !== undefined) mapped.targetRef = endpoint.targetRef;
      if (endpoint.metadata !== undefined) mapped.metadata = endpoint.metadata;
      return mapped;
    });
  }
  return slice;
}

export interface KubernetesEndpointWatchOptions {
  /** Delay before re-listing and re-watching after the watch drops (defaults to 1s). */
  reconnectMs?: number;
}

/**
 * Drives a {@link WatchedEndpoints} from a Kubernetes EndpointSlice list+watch:
 * lists once to seed the full set, then applies each watch event. A Kubernetes
 * watch is finite — the API server ends it periodically and on error — so on
 * close it re-lists (recovering any change missed while disconnected) and
 * re-watches. Exposes `subscribe` so it plugs straight into `KubernetesMembership`
 * as the `EndpointWatch`.
 */
export class KubernetesEndpointWatch implements EndpointWatch {
  private readonly endpoints = new WatchedEndpoints();
  private readonly reconnectMs: number;
  private stopWatch: (() => void) | undefined;
  private running = false;

  constructor(
    private readonly source: EndpointSliceSource,
    options: KubernetesEndpointWatchOptions = {},
  ) {
    this.reconnectMs = options.reconnectMs ?? 1000;
  }

  subscribe(onSlices: (slices: EndpointSlice[]) => void): () => void {
    return this.endpoints.subscribe(onSlices);
  }

  async start(): Promise<void> {
    this.running = true;
    await this.relistAndWatch();
  }

  stop(): void {
    this.running = false;
    this.stopWatch?.();
    this.stopWatch = undefined;
  }

  private async relistAndWatch(): Promise<void> {
    const slices = new Map<string, EndpointSlice>();
    for (const raw of await this.source.list()) {
      const name = raw.metadata?.name;
      if (name !== undefined) slices.set(name, toEndpointSlice(raw));
    }
    this.endpoints.reset(slices);
    if (!this.running) return;

    this.stopWatch = this.source.watch(
      (type, raw) => {
        const name = raw.metadata?.name;
        if (name === undefined) return;
        this.endpoints.apply(type, name, type === "DELETED" ? undefined : toEndpointSlice(raw));
      },
      () => {
        this.stopWatch = undefined;
        if (this.running) setTimeout(() => void this.relistAndWatch(), this.reconnectMs);
      },
    );
  }
}
