import type { EndpointSlice } from "@thresh/clustering-k8s/endpoint-slice";
import type { EndpointWatch } from "@thresh/clustering-k8s/kubernetes-membership";

export type WatchEventType = "ADDED" | "MODIFIED" | "DELETED";

/**
 * Aggregates the per-resource events a Kubernetes watch emits into the full set
 * of EndpointSlices for the service, notifying subscribers on every change.
 * The real `@kubernetes/client-node` watch adapter feeds events in via `apply`
 * (and `reset` on a re-list); the aggregation here is what `KubernetesMembership`
 * consumes.
 *
 * Alongside the slices it reports the **observed revision**: the highest
 * EndpointSlice `resourceVersion` this watch has seen — each listed slice's, and
 * each watch event's (a DELETED event carries the revision of the deletion). For an
 * etcd-backed resource a `resourceVersion` is the etcd modification revision, one
 * cluster-wide increasing counter, so two watchers that have observed the same
 * writes to the service's slices report the same revision, and a watcher that has
 * missed a write reports a lower one. `KubernetesMembership` uses it as the view
 * version (issue #72).
 *
 * The list's own `resourceVersion` is deliberately NOT folded in on every re-list:
 * it is the cluster's global revision at the moment of the list, which moves with
 * writes to every other object, so a silo that listed later would carry a higher
 * number for an identical view — and a peer then waits for a version it can never
 * reach (`ClusterNode.awaitView`). It is used for exactly one case: a re-list whose
 * slices changed although no listed slice is newer than what was already observed
 * — a slice deleted while the watch was down, whose deletion revision this watcher
 * never saw. The list's revision is the only bound on that change, and taking it
 * keeps the revision moving forward whenever the view does.
 */
export class WatchedEndpoints implements EndpointWatch {
  private readonly slices = new Map<string, EndpointSlice>();
  private readonly subscribers = new Set<
    (slices: EndpointSlice[], revision: number | undefined) => void
  >();
  private revision: number | undefined;

  subscribe(onSlices: (slices: EndpointSlice[], revision: number | undefined) => void): () => void {
    this.subscribers.add(onSlices);
    if (this.slices.size > 0) onSlices(this.snapshot(), this.revision);
    return () => {
      this.subscribers.delete(onSlices);
    };
  }

  /** The highest slice revision observed so far (see the class doc); `undefined` before any. */
  observedRevision(): number | undefined {
    return this.revision;
  }

  /**
   * Apply one watch event, keyed by the slice's resource name. For a DELETED
   * event `slice`, when given, only contributes its `resourceVersion`.
   */
  apply(type: WatchEventType, name: string, slice?: EndpointSlice): void {
    if (type === "DELETED") this.slices.delete(name);
    else if (slice !== undefined) this.slices.set(name, slice);
    const advanced = this.observe(parseRevision(slice?.resourceVersion));
    this.emit(advanced ? this.revision : undefined);
  }

  /**
   * Replace the whole set, e.g. after the watch re-lists on reconnect.
   * `listResourceVersion` is the list's own `resourceVersion` (see the class doc for
   * the one case it is used).
   */
  reset(slices: ReadonlyMap<string, EndpointSlice>, listResourceVersion?: string): void {
    const changed = !sameSlices(this.slices, slices);
    let newest: number | undefined;
    for (const slice of slices.values()) {
      const revision = parseRevision(slice.resourceVersion);
      if (revision !== undefined && (newest === undefined || revision > newest)) newest = revision;
    }
    const listRevision = parseRevision(listResourceVersion);
    const before = this.revision;
    if (newest === undefined && this.revision === undefined) {
      // Nothing listed carries a revision of its own (an empty service): the list's is all
      // there is to start from.
      this.observe(listRevision);
    } else if (changed && (newest === undefined || (this.revision ?? -1) >= newest)) {
      this.observe(listRevision ?? newest);
    } else {
      this.observe(newest);
    }
    this.slices.clear();
    for (const [name, slice] of slices) this.slices.set(name, slice);
    this.emit(this.revision !== before || !changed ? this.revision : undefined);
  }

  /** Fold `revision` in; whether it moved the observed revision forward. */
  private observe(revision: number | undefined): boolean {
    if (revision === undefined) return false;
    if (this.revision !== undefined && revision <= this.revision) return false;
    this.revision = revision;
    return true;
  }

  private snapshot(): EndpointSlice[] {
    return [...this.slices.values()];
  }

  /**
   * Notify subscribers. `revision` is `undefined` for a change the watch could not
   * date — one that did not move the observed revision forward, which only a source
   * reporting no (or stale) resourceVersions produces — so the consumer does not
   * mistake a changed view for the one it already holds at that revision.
   */
  private emit(revision: number | undefined): void {
    const snapshot = this.snapshot();
    for (const subscriber of this.subscribers) subscriber(snapshot, revision);
  }
}

/**
 * A `resourceVersion` as a revision number. Kubernetes documents the string as opaque;
 * for etcd-backed resources it is the decimal etcd revision. Anything else is treated
 * as carrying no revision.
 */
function parseRevision(resourceVersion: string | undefined): number | undefined {
  if (resourceVersion === undefined || !/^\d+$/.test(resourceVersion)) return undefined;
  const revision = Number(resourceVersion);
  return Number.isSafeInteger(revision) ? revision : undefined;
}

/** Whether two slice sets hold the same slices at the same revisions (or, lacking them, the same objects). */
function sameSlices(
  a: ReadonlyMap<string, EndpointSlice>,
  b: ReadonlyMap<string, EndpointSlice>,
): boolean {
  if (a.size !== b.size) return false;
  for (const [name, slice] of a) {
    const other = b.get(name);
    if (other === undefined) return false;
    if (slice.resourceVersion === undefined || other.resourceVersion === undefined) {
      if (JSON.stringify(slice) !== JSON.stringify(other)) return false;
    } else if (slice.resourceVersion !== other.resourceVersion) {
      return false;
    }
  }
  return true;
}
