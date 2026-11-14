import {
  FakeEndpointSliceApi,
  type FakeEndpointSliceSource,
  type FakePod,
} from "@thresh/clustering-k8s/fake-endpoint-slice-api";
import { KubernetesEndpointWatch } from "@thresh/clustering-k8s/kubernetes-endpoint-watch";
import { SiloAddress } from "@thresh/core/silo-address";

/** Where a silo's advertised metadata travels as pod labels (stripped again by membership). */
export const SILO_METADATA_LABEL_PREFIX = "thresh.io/";

/** Names a silo the controls act on: a `TestCluster` handle, or anything with its address. */
export interface PodOwner {
  readonly address: SiloAddress;
}

/**
 * The Kubernetes side of a `TestCluster` built with `membership: "kubernetes"`: one fake
 * EndpointSlice API server that every silo watches through its own source, as every pod watches
 * the real one. The test drives it the way Kubernetes would — readiness flips, a pod removed,
 * one silo's watch lagging, a watch that ends and re-lists — and each control resolves once the
 * silos have had the chance to apply the change.
 */
export class KubernetesControls {
  /** The fake API server itself, for anything the controls below do not cover. */
  readonly api = new FakeEndpointSliceApi();
  private readonly sources = new Map<string, FakeEndpointSliceSource>();

  /** @internal Built by `TestCluster`, which reports whether every live silo has applied its own view. */
  constructor(private readonly converged: () => boolean) {}

  /** Flip a pod's readiness condition. */
  async setReady(silo: PodOwner, ready: boolean): Promise<void> {
    this.api.setReady(silo.address.podName, ready);
    await this.settle();
  }

  /** The first step of a pod going away: its endpoint turns not ready. */
  markNotReady(silo: PodOwner): Promise<void> {
    return this.setReady(silo, false);
  }

  /** The second step: the endpoint leaves the slice. */
  async removePod(silo: PodOwner): Promise<void> {
    this.api.removePod(silo.address.podName);
    await this.settle();
  }

  /** Both steps, in order, letting the silos see each one. */
  async depart(silo: PodOwner): Promise<void> {
    await this.markNotReady(silo);
    await this.removePod(silo);
  }

  /** Queue `silo`'s watch events instead of delivering them, so its view falls behind. */
  hold(silo: PodOwner): void {
    this.source(silo).hold();
  }

  /** Deliver `silo`'s held events, in order, and resume live delivery. */
  async release(silo: PodOwner): Promise<void> {
    this.source(silo).release();
    await this.settle();
  }

  /** End `silo`'s watch as the API server does on a timeout: it re-lists, then watches again. */
  async relist(silo: PodOwner): Promise<void> {
    this.source(silo).endWatch();
    await this.settle();
  }

  /**
   * Let pending membership work run: watch reconnects, view reconciliation, directory traffic.
   * Returns once every live silo has applied the view its own membership holds (a silo whose
   * watch is held counts as settled on the view it has).
   */
  async settle(): Promise<void> {
    for (let round = 0; round < 500; round++) {
      await new Promise((resolve) => setTimeout(resolve, 0));
      if (round >= 5 && this.converged()) return;
    }
  }

  /**
   * @internal Publish a pod (not ready) for a new silo, labelled with the metadata it
   * advertises, and give it its own watch.
   */
  addPod(
    index: number,
    metadataFor: (address: SiloAddress) => Readonly<Record<string, string>> | undefined,
  ): {
    address: SiloAddress;
    metadata: Readonly<Record<string, string>> | undefined;
    watch: KubernetesEndpointWatch;
  } {
    const unlabelled: FakePod = {
      name: `test-silo-${index}`,
      uid: `uid-${index}`,
      ip: `10.0.${Math.floor(index / 250)}.${(index % 250) + 1}`,
      ready: false,
    };
    const address = new SiloAddress(
      unlabelled.name,
      unlabelled.uid,
      this.api.endpointOf(unlabelled),
    );
    const metadata = metadataFor(address);
    const labels = metadataLabels(metadata);
    const pod: FakePod = labels !== undefined ? { ...unlabelled, labels } : unlabelled;
    this.api.setPod(pod);
    const source = this.api.source();
    this.sources.set(pod.name, source);
    // Reconnect at once, with no jitter: a forced relist is a step the test takes, not a wait.
    const watch = new KubernetesEndpointWatch(source, { reconnectMs: 0, random: () => 0 });
    return { address, metadata, watch };
  }

  /** Whether the slice still lists `silo`'s pod. */
  listed(silo: PodOwner): boolean {
    return this.api.pods().some((p) => p.name === silo.address.podName);
  }

  /** @internal Sever `silo`'s watch for good, as a crashed pod's is. */
  disconnect(silo: PodOwner): void {
    this.source(silo).disconnect();
  }

  private source(silo: PodOwner): FakeEndpointSliceSource {
    const source = this.sources.get(silo.address.podName);
    if (source === undefined) throw new Error(`no pod for ${silo.address.podName}`);
    return source;
  }
}

/** Pod labels carrying a silo's advertised metadata. */
function metadataLabels(
  metadata: Readonly<Record<string, string>> | undefined,
): Readonly<Record<string, string>> | undefined {
  if (metadata === undefined) return undefined;
  return Object.fromEntries(
    Object.entries(metadata).map(([k, v]) => [`${SILO_METADATA_LABEL_PREFIX}${k}`, v]),
  );
}
