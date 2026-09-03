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
  /**
   * Base delay before re-listing and re-watching after the watch drops (defaults to 1s).
   * Doubles on each consecutive failed close, up to {@link maxReconnectMs}; see the class
   * doc comment for why.
   */
  reconnectMs?: number;
  /** Ceiling for the exponential backoff delay (defaults to 30s). */
  maxReconnectMs?: number;
  /**
   * Source of randomness for jitter, injected so tests can make the delay deterministic
   * (the codebase pattern — see e.g. `RandomPlacement`). Defaults to `Math.random`.
   */
  random?: () => number;
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
  private readonly maxReconnectMs: number;
  private readonly random: () => number;
  private stopWatch: (() => void) | undefined;
  private running = false;
  /**
   * Consecutive failed closes (a truthy `onClose(err)`) since the last clean close. Drives
   * the exponential backoff below — a clean close (the API server ending the watch normally,
   * not a failure) resets it, so steady-state reconnects stay at the base delay.
   */
  private consecutiveFailures = 0;

  constructor(
    private readonly source: EndpointSliceSource,
    options: KubernetesEndpointWatchOptions = {},
  ) {
    this.reconnectMs = options.reconnectMs ?? 1000;
    this.maxReconnectMs = options.maxReconnectMs ?? 30_000;
    this.random = options.random ?? Math.random;
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
    try {
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
        (err) => {
          this.stopWatch = undefined;
          // Any close — clean, an error, or a 410 Gone once `resourceVersion` aged out of the
          // API server's history — is handled the same way: relist for a fresh snapshot and
          // resourceVersion, then rewatch from there. The delay before doing so backs off
          // exponentially across consecutive *failed* closes (an error or 410 Gone), with
          // jitter: when the API server itself restarts, every silo's watch drops at once, and
          // a flat delay would have them all relist and rewatch in lockstep, hammering the API
          // server the moment it comes back. A clean close — the API server ending the watch
          // normally, which it does periodically by design — resets the backoff, so
          // steady-state reconnects stay at the base delay.
          this.consecutiveFailures = err !== undefined ? this.consecutiveFailures + 1 : 0;
          if (this.running) setTimeout(() => void this.relistAndWatch(), this.nextReconnectDelay());
        },
      );

      // We got here because the relist and rewatch above both succeeded — a healthy
      // (re)connection, not just a clean close of the *previous* watch. Reset the backoff here
      // too (in addition to the clean-close reset above) so a failure followed by a long,
      // healthy run doesn't have its next unrelated failure inherit an old incident's doubled
      // delay: the exponent is meant to track one continuous outage, not accumulate forever.
      this.consecutiveFailures = 0;
    } catch {
      // The relist itself failed — most likely the same outage that dropped the watch (e.g. the
      // API server is still restarting) rather than a fresh, unrelated problem. Left unhandled,
      // this rejection would be silently swallowed by the `void this.relistAndWatch()` caller
      // and the watch loop would die permanently on the very outage the backoff exists to
      // survive. Count it as a failure and reschedule through the same backoff as a dropped
      // watch, so relist retries also spread out instead of hammering the API server.
      this.stopWatch = undefined;
      this.consecutiveFailures += 1;
      if (this.running) setTimeout(() => void this.relistAndWatch(), this.nextReconnectDelay());
    }
  }

  /**
   * Base delay doubled once per consecutive failure beyond the first (capped at
   * `maxReconnectMs`), plus full jitter — a random extra 0x-1x of that delay — so silos that
   * failed in the same tick don't all retry at the exact same instant either.
   */
  private nextReconnectDelay(): number {
    const exponent = Math.max(0, this.consecutiveFailures - 1);
    const delay = Math.min(this.reconnectMs * 2 ** exponent, this.maxReconnectMs);
    return delay + this.random() * delay;
  }
}
