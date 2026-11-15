import type { SiloMember, SiloStatus } from "@thresh/core/membership";
import { SiloAddress } from "@thresh/core/silo-address";

// The subset of the Kubernetes EndpointSlice shape the membership watch reads.
export interface EndpointSliceEndpoint {
  addresses: string[];
  /**
   * The endpoint's conditions: `ready` (the pod's readiness probe passes) and `terminating`
   * (the pod is being deleted). Together they give the silo's status (see `siloStatusOf`).
   */
  conditions?: { ready?: boolean; terminating?: boolean };
  targetRef?: { name?: string; uid?: string };
  /**
   * The endpoint's pod labels, when the watch source resolves them (EndpointSlice
   * endpoints don't carry pod labels themselves; `createKubernetesClientSource`
   * joins them in from the Pod object by `targetRef.uid`). Raw, unfiltered labels;
   * `KubernetesMembershipOptions.metadataLabelPrefix` picks which ones become
   * `SiloMember.metadata`.
   */
  metadata?: Readonly<Record<string, string>>;
}

export interface EndpointSlicePort {
  name?: string;
  port?: number;
}

export interface EndpointSlice {
  /**
   * The slice's `metadata.resourceVersion`: its last-modified revision, which the watch
   * folds into the view version (see `WatchedEndpoints`).
   */
  resourceVersion?: string;
  endpoints?: EndpointSliceEndpoint[];
  ports?: EndpointSlicePort[];
}

function pickPort(ports: EndpointSlicePort[] | undefined, portName?: string): number | undefined {
  if (ports === undefined || ports.length === 0) return undefined;
  if (portName !== undefined) {
    const named = ports.find((p) => p.name === portName);
    if (named?.port !== undefined) return named.port;
  }
  return ports[0]?.port;
}

/**
 * A listed endpoint's status, from its conditions: `ready` is `active`; not ready and
 * `terminating` is `draining` (a pod being deleted, still serving what it hosts); not ready
 * otherwise is `joining` (a pod still starting, or one whose readiness probe is failing). An
 * endpoint that is not listed at all is absent from the view. The same rule applies to every
 * endpoint, the local silo's own included, so silos that observed the same slices hold the same
 * view.
 */
export function siloStatusOf(endpoint: EndpointSliceEndpoint): SiloStatus {
  if (endpoint.conditions?.ready === true) return "active";
  return endpoint.conditions?.terminating === true ? "draining" : "joining";
}

/**
 * Derive the silo view from EndpointSlices: every endpoint becomes a member, with
 * the pod name and UID from `targetRef` (the UID distinguishes a fresh
 * incarnation from a previous one at the same name) and its status from its
 * conditions (`siloStatusOf`).
 *
 * Readiness alone is not a failure detector, and the two are not the same
 * question. Whether a silo is `active` decides who may take new placements and
 * own new ranges; whether it is *present at all* decides whether the directory
 * entries pointing at it are still valid. Kubernetes marks a terminating
 * endpoint not-ready (during a graceful drain, which then keeps serving for its
 * grace period) and only removes it once the pod is gone — so a not-ready
 * endpoint is a silo that is leaving or still coming up, not one that has
 * departed. Reporting it as a member is what lets a peer keep, rather than
 * delete, the pointer to a grain that has not stopped running.
 */
export function siloMembersFromSlices(
  slices: readonly EndpointSlice[],
  portName?: string,
): SiloMember[] {
  const silos: SiloMember[] = [];
  for (const slice of slices) {
    const port = pickPort(slice.ports, portName);
    if (port === undefined) continue;
    for (const endpoint of slice.endpoints ?? []) {
      const address = endpoint.addresses[0];
      const podName = endpoint.targetRef?.name;
      const podUid = endpoint.targetRef?.uid;
      if (address === undefined || podName === undefined || podUid === undefined) continue;
      silos.push({
        address: new SiloAddress(podName, podUid, `${address}:${port}`),
        status: siloStatusOf(endpoint),
      });
    }
  }
  return silos;
}

/**
 * Derive `SiloMember.metadata` from EndpointSlice endpoints, keyed by pod uid.
 * Only labels under `labelPrefix` are surfaced, with the prefix stripped from the
 * key (e.g. label `thresh.io/role=worker` with prefix `"thresh.io/"` becomes
 * metadata `{ role: "worker" }`). Omitting `labelPrefix` surfaces no metadata —
 * Kubernetes membership is opt-in for metadata, unlike static membership, which
 * always carries whatever its resolver returns. A pod with no matching labels is
 * absent from the map rather than present with an empty object.
 */
export function metadataFromSlices(
  slices: readonly EndpointSlice[],
  labelPrefix?: string,
): Map<string, Readonly<Record<string, string>>> {
  const metadataByUid = new Map<string, Readonly<Record<string, string>>>();
  if (labelPrefix === undefined) return metadataByUid;
  for (const slice of slices) {
    for (const endpoint of slice.endpoints ?? []) {
      const uid = endpoint.targetRef?.uid;
      if (uid === undefined || endpoint.metadata === undefined) continue;
      const matched: Record<string, string> = {};
      for (const [key, value] of Object.entries(endpoint.metadata)) {
        if (key.startsWith(labelPrefix)) matched[key.slice(labelPrefix.length)] = value;
      }
      if (Object.keys(matched).length > 0) metadataByUid.set(uid, matched);
    }
  }
  return metadataByUid;
}
