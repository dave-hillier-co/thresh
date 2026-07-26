import { SiloAddress } from "@thresh/core/silo-address";

// The subset of the Kubernetes EndpointSlice shape the membership watch reads.
export interface EndpointSliceEndpoint {
  addresses: string[];
  conditions?: { ready?: boolean };
  targetRef?: { name?: string; uid?: string };
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
