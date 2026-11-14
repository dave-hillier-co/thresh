import type {
  EndpointSliceListResult,
  EndpointSliceSource,
  RawEndpointSlice,
} from "@thresh/clustering-k8s/kubernetes-endpoint-watch";
import type { WatchEventType } from "@thresh/clustering-k8s/watched-endpoints";

/**
 * Test support: an in-memory stand-in for the Kubernetes API server's EndpointSlice
 * list+watch — the true boundary under `KubernetesMembership`. A test hands each
 * silo its own `source()` and wraps it in the production `KubernetesEndpointWatch`,
 * so the whole membership path (watch → `KubernetesMembership` → host → node) runs
 * exactly as it does against a cluster.
 *
 * It models the parts of the API server that membership depends on:
 *
 * - one EndpointSlice for the headless service, holding every pod's endpoint;
 * - an etcd-like global revision: every write to any object bumps it, and the
 *   slice's `metadata.resourceVersion` is the revision of its last write, while a
 *   list reports the global revision at the time of the list (which also moves on
 *   writes to unrelated objects — see `unrelatedWrite`);
 * - per-watcher delivery that can be held back (`hold`/`release`), standing in for a
 *   silo whose watch connection lags its peers';
 * - a watch the API server ends (`FakeEndpointSliceSource.endWatch`), after which the
 *   watcher re-lists and re-watches, as it does when a real watch times out;
 * - pod labels, carried on each endpoint as the label-resolving client source does.
 */
export interface FakePod {
  readonly name: string;
  readonly uid: string;
  readonly ip: string;
  readonly ready: boolean;
  /** The pod's labels, surfaced on its endpoint (see `KubernetesMembershipOptions.metadataLabelPrefix`). */
  readonly labels?: Readonly<Record<string, string>>;
}

interface Watcher {
  onEvent: (type: WatchEventType, slice: RawEndpointSlice) => void;
  onClose: ((err?: unknown) => void) | undefined;
  held: Array<() => void> | undefined;
}

const SLICE_NAME = "silo-svc-abcde";

export class FakeEndpointSliceApi {
  private revision = 1;
  private sliceRevision = 1;
  private readonly podsByName = new Map<string, FakePod>();
  private readonly watchers = new Set<Watcher>();

  constructor(private readonly port = 11111) {}

  /** The SiloAddress endpoint `KubernetesMembership` derives for `pod`. */
  endpointOf(pod: Pick<FakePod, "ip">): string {
    return `${pod.ip}:${this.port}`;
  }

  /** Add or replace a pod's endpoint (keyed by pod name), publishing a MODIFIED event. */
  setPod(pod: FakePod): void {
    this.podsByName.set(pod.name, pod);
    this.writeSlice();
  }

  /** Flip a pod's readiness condition. */
  setReady(name: string, ready: boolean): void {
    const pod = this.podsByName.get(name);
    if (pod === undefined) throw new Error(`no pod ${name}`);
    this.setPod({ ...pod, ready });
  }

  /** Remove a pod's endpoint from the slice. */
  removePod(name: string): void {
    this.podsByName.delete(name);
    this.writeSlice();
  }

  /** The pods whose endpoints the slice currently lists. */
  pods(): FakePod[] {
    return [...this.podsByName.values()];
  }

  /** A write to some other object in the cluster: the global revision moves, the slice does not. */
  unrelatedWrite(): void {
    this.revision += 1;
  }

  /** A fresh list+watch source, one per silo (each silo watches for itself). */
  source(): FakeEndpointSliceSource {
    return new FakeEndpointSliceSource(this);
  }

  /** @internal */
  list(): EndpointSliceListResult {
    return { items: [this.slice()], resourceVersion: String(this.revision) };
  }

  /** @internal */
  addWatcher(watcher: Watcher): void {
    this.watchers.add(watcher);
  }

  /** @internal */
  removeWatcher(watcher: Watcher): void {
    this.watchers.delete(watcher);
  }

  private writeSlice(): void {
    this.revision += 1;
    this.sliceRevision = this.revision;
    const slice = this.slice();
    for (const watcher of this.watchers) {
      const deliver = (): void => watcher.onEvent("MODIFIED", slice);
      if (watcher.held !== undefined) watcher.held.push(deliver);
      else deliver();
    }
  }

  private slice(): RawEndpointSlice {
    return {
      metadata: { name: SLICE_NAME, resourceVersion: String(this.sliceRevision) },
      ports: [{ name: "silo", port: this.port }],
      endpoints: [...this.podsByName.values()].map((pod) => ({
        addresses: [pod.ip],
        conditions: { ready: pod.ready },
        targetRef: { name: pod.name, uid: pod.uid },
        ...(pod.labels !== undefined ? { metadata: { ...pod.labels } } : {}),
      })),
    };
  }
}

/** One silo's connection to the fake API server. */
export class FakeEndpointSliceSource implements EndpointSliceSource {
  private watcher: Watcher | undefined;
  private holding = false;

  constructor(private readonly api: FakeEndpointSliceApi) {}

  async list(): Promise<EndpointSliceListResult> {
    return this.api.list();
  }

  watch(
    _resourceVersion: string | undefined,
    onEvent: (type: WatchEventType, slice: RawEndpointSlice) => void,
    onClose?: (err?: unknown) => void,
  ): () => void {
    const watcher: Watcher = { onEvent, onClose, held: this.holding ? [] : undefined };
    this.watcher = watcher;
    this.api.addWatcher(watcher);
    return () => {
      this.api.removeWatcher(watcher);
      if (this.watcher === watcher) this.watcher = undefined;
    };
  }

  /** Queue this silo's watch events instead of delivering them, until `release`. */
  hold(): void {
    this.holding = true;
    if (this.watcher !== undefined) this.watcher.held ??= [];
  }

  /** Deliver every held event, in order, and resume live delivery. */
  release(): void {
    this.holding = false;
    const held = this.watcher?.held ?? [];
    if (this.watcher !== undefined) this.watcher.held = undefined;
    for (const deliver of held) deliver();
  }

  /**
   * End the current watch cleanly, as the API server does when a watch times out: the watcher
   * re-lists for a fresh snapshot (which no hold delays) and watches again. Events held for the
   * ended watch are discarded with it; the re-list covers them.
   */
  endWatch(): void {
    const watcher = this.watcher;
    if (watcher === undefined) return;
    this.api.removeWatcher(watcher);
    this.watcher = undefined;
    watcher.onClose?.();
  }

  /** Sever the watch for good, as a crashed pod's watch is (its process is gone). */
  disconnect(): void {
    if (this.watcher !== undefined) this.api.removeWatcher(this.watcher);
    this.watcher = undefined;
  }
}
