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

/** A `list()` snapshot: the current slices plus the resourceVersion it was read at. */
export interface EndpointSliceListResult {
  readonly items: RawEndpointSlice[];
  /**
   * The list's resourceVersion (the Kubernetes API's `metadata.resourceVersion`), when the
   * source can report one. Pass it to `watch()` so the watch picks up from EXACTLY this
   * snapshot — otherwise a watch that starts from "now" misses any change the API server
   * applied between the list call returning and the watch call being established.
   */
  readonly resourceVersion?: string;
}

/**
 * The true boundary: a source of EndpointSlice list+watch events. The production
 * implementation wraps `@kubernetes/client-node` (see `kubernetes-client-source`);
 * tests supply a fake so the list→watch→reconnect logic here is sociable and runs
 * without a cluster.
 */
export interface EndpointSliceSource {
  /** The current full set of slices for the service, and the resourceVersion it was read at. */
  list(): Promise<EndpointSliceListResult>;
  /**
   * Start a watch from `resourceVersion` (typically a preceding `list()`'s, so no event in
   * between is missed; `undefined` starts from "now"). `onEvent` receives each change;
   * `onClose` is called when the watch ends (cleanly, on error, or on a 410 Gone once
   * `resourceVersion` has aged out of the API server's history — the caller reacts to any of
   * these the same way: re-list for a fresh snapshot and resourceVersion, then re-watch).
   * Returns a function that stops the watch.
   */
  watch(
    resourceVersion: string | undefined,
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
    const { items, resourceVersion } = await this.source.list();
    for (const raw of items) {
      const name = raw.metadata?.name;
      if (name !== undefined) slices.set(name, toEndpointSlice(raw));
    }
    this.endpoints.reset(slices);
    if (!this.running) return;

    // Watch from this list's resourceVersion, not "now" — otherwise any change the API server
    // applies between the list returning and the watch being established is silently missed.
    this.stopWatch = this.source.watch(
      resourceVersion,
      (type, raw) => {
        const name = raw.metadata?.name;
        if (name === undefined) return;
        this.endpoints.apply(type, name, type === "DELETED" ? undefined : toEndpointSlice(raw));
      },
      () => {
        this.stopWatch = undefined;
        // Any close — clean, an error, or a 410 Gone once `resourceVersion` aged out of the API
        // server's history — is handled the same way: relist for a fresh snapshot and
        // resourceVersion, then rewatch from there.
        if (this.running) setTimeout(() => void this.relistAndWatch(), this.reconnectMs);
      },
    );
  }
}
