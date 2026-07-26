import type { EndpointSlice } from "@thresh/clustering-k8s/endpoint-slice";
import type { EndpointWatch } from "@thresh/clustering-k8s/kubernetes-membership";

export type WatchEventType = "ADDED" | "MODIFIED" | "DELETED";

/**
 * Aggregates the per-resource events a Kubernetes watch emits into the full set
 * of EndpointSlices for the service, notifying subscribers on every change.
 * The real `@kubernetes/client-node` watch adapter feeds events in via `apply`
 * (and `reset` on a re-list); the aggregation here is what `KubernetesMembership`
 * consumes.
 */
export class WatchedEndpoints implements EndpointWatch {
  private readonly slices = new Map<string, EndpointSlice>();
  private readonly subscribers = new Set<(slices: EndpointSlice[]) => void>();

  subscribe(onSlices: (slices: EndpointSlice[]) => void): () => void {
    this.subscribers.add(onSlices);
    if (this.slices.size > 0) onSlices(this.snapshot());
    return () => {
      this.subscribers.delete(onSlices);
    };
  }

  /** Apply one watch event, keyed by the slice's resource name. */
  apply(type: WatchEventType, name: string, slice?: EndpointSlice): void {
    if (type === "DELETED") this.slices.delete(name);
    else if (slice !== undefined) this.slices.set(name, slice);
    this.emit();
  }

  /** Replace the whole set, e.g. after the watch re-lists on reconnect. */
  reset(slices: ReadonlyMap<string, EndpointSlice>): void {
    this.slices.clear();
    for (const [name, slice] of slices) this.slices.set(name, slice);
    this.emit();
  }

  private snapshot(): EndpointSlice[] {
    return [...this.slices.values()];
  }

  private emit(): void {
    const snapshot = this.snapshot();
    for (const subscriber of this.subscribers) subscriber(snapshot);
  }
}
