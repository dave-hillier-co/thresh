import { SiloAddress } from "@tsva/core/silo-address";

/** The pod identity a silo reads from the Kubernetes downward API. */
export interface PodEnvironment {
  podName: string;
  podUid: string;
  podIp: string;
  namespace: string;
}

type EnvSource = Record<string, string | undefined>;

function required(env: EnvSource, name: string): string {
  const value = env[name];
  if (value === undefined || value === "") {
    throw new Error(`pod-environment: ${name} is not set (inject it via the downward API)`);
  }
  return value;
}

/**
 * Read the pod identity from the downward-API environment variables the
 * StatefulSet injects (see docs/10): `POD_NAME`, `POD_UID`, `POD_IP`,
 * `POD_NAMESPACE`.
 */
export function readPodEnvironment(env: EnvSource = process.env): PodEnvironment {
  return {
    podName: required(env, "POD_NAME"),
    podUid: required(env, "POD_UID"),
    podIp: required(env, "POD_IP"),
    namespace: required(env, "POD_NAMESPACE"),
  };
}

/**
 * The silo's {@link SiloAddress} for this pod: the stable pod name keys the
 * directory ring, the UID marks the incarnation, and `podIp:siloPort` is the
 * address peers connect to over the headless Service's per-pod DNS.
 */
export function siloAddressFromPodEnv(siloPort: number, env: EnvSource = process.env): SiloAddress {
  const pod = readPodEnvironment(env);
  return new SiloAddress(pod.podName, pod.podUid, `${pod.podIp}:${siloPort}`);
}
