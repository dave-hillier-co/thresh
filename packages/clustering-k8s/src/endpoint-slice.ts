import { SiloAddress } from "@thresh/core/silo-address";

// The subset of the Kubernetes EndpointSlice shape the membership watch reads.
export interface EndpointSliceEndpoint {
  addresses: string[];
  conditions?: { ready?: boolean };
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
 * Derive the live silo set from EndpointSlices: ready endpoints become silos,
 * with the pod name and UID from `targetRef` (the UID distinguishes a fresh
 * incarnation from a previous one at the same name). Endpoints not marked ready
 * are excluded — this is the failure detector (docs/05).
 */
export function readySilosFromSlices(
  slices: readonly EndpointSlice[],
  portName?: string,
): SiloAddress[] {
  const silos: SiloAddress[] = [];
  for (const slice of slices) {
    const port = pickPort(slice.ports, portName);
    if (port === undefined) continue;
    for (const endpoint of slice.endpoints ?? []) {
      if (endpoint.conditions?.ready !== true) continue;
      const address = endpoint.addresses[0];
      const podName = endpoint.targetRef?.name;
      const podUid = endpoint.targetRef?.uid;
      if (address === undefined || podName === undefined || podUid === undefined) continue;
      silos.push(new SiloAddress(podName, podUid, `${address}:${port}`));
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
