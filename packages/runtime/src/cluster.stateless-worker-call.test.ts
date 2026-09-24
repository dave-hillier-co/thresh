import { afterEach, describe, expect, it } from "vitest";
import { grain, mayInterleave } from "@thresh/core/decorators";
import { Grain } from "@thresh/core/grain";
import { GrainId } from "@thresh/core/grain-id";
import { defineGrainInterface } from "@thresh/core/grain-interface";
import type { GrainKey } from "@thresh/core/key-kinds";
import { TestCluster } from "@thresh/testing/test-cluster";

interface IWorkerPool extends GrainKey<string> {
  /**
   * Hold this call until `release`, then answer with the id of the activation
   * that served it — so a caller can tell two concurrent calls served by two
   * local activations from two calls served by the same one.
   */
  park(): Promise<string>;
  release(): Promise<void>;
  ping(): Promise<string>;
}

/** An ordinary grain, for contrast with the stateless worker above. */
interface IPlain extends GrainKey<string> {
  ping(): Promise<string>;
}

const IWorkerPool = defineGrainInterface<IWorkerPool>("IWorkerPool.wire");
const IPlain = defineGrainInterface<IPlain>("IPlain.wire");

/**
 * A gate every activation of `WorkerPoolGrain` shares (upstream
 * `StatelessWorkerScalingGrain`'s `SemaphoreSlim(0)`), so a test can hold
 * several activations busy at once and then free them all.
 */
class CountingSemaphore {
  private count = 0;
  private readonly waiters: Array<() => void> = [];

  park(): Promise<void> {
    if (this.count > 0) {
      this.count -= 1;
      return Promise.resolve();
    }
    return new Promise<void>((resolve) => this.waiters.push(resolve));
  }

  release(): void {
    const next = this.waiters.shift();
    if (next !== undefined) next();
    else this.count += 1;
  }
}

const gate = new CountingSemaphore();
let nextActivationUid = 0;

/**
 * A stateless worker: several interchangeable activations per id on one silo,
 * scaled on demand up to `maxLocalWorkers`. Each activation carries its own
 * uid, the role upstream's per-activation `Guid` plays. `release` is
 * `@mayInterleave`-eligible so it always runs while a `park` call holds its
 * activation busy (upstream's `[AlwaysInterleave]` on the same grain).
 */
@grain({ stateless: true, maxLocalWorkers: 4 })
@mayInterleave((methodName) => methodName !== "park")
class WorkerPoolGrain extends Grain implements IWorkerPool {
  private readonly activationUid = `w${nextActivationUid++}`;

  async park(): Promise<string> {
    await gate.park();
    return this.activationUid;
  }

  async release(): Promise<void> {
    gate.release();
  }

  async ping(): Promise<string> {
    return "pong";
  }
}

@grain()
class PlainGrain extends Grain implements IPlain {
  async ping(): Promise<string> {
    return "pong";
  }
}

let cluster: TestCluster | undefined;

afterEach(async () => {
  await cluster?.dispose();
  cluster = undefined;
});

/**
 * One silo, so the cluster client has exactly one gateway to dial: every call
 * below therefore takes the inbound-request path (`ClusterNode.receiveRequest`
 * → `Dispatcher.deliverLocal`) rather than the originator's own funnel, which
 * is the path these tests are about. The client is not a silo — its calls
 * cross the transport as wire messages exactly as another silo's would.
 */
async function buildCluster(): Promise<TestCluster> {
  cluster = await TestCluster.start({
    clusterId: "swc",
    initialSilos: 1,
    grains: [
      { ctor: WorkerPoolGrain, interfaces: [IWorkerPool] },
      { ctor: PlainGrain, interfaces: [IPlain] },
    ],
  });
  return cluster;
}

describe("a grain call arriving from outside the cluster", () => {
  it("scales the target silo's local worker pool for a stateless-worker grain", async () => {
    const c = await buildCluster();
    const client = await c.client;
    const grain = client.getGrain(IWorkerPool, "k");

    // Two calls in flight at once: each must be served by its own local
    // activation (the catalog scales up while the first is still busy), rather
    // than the second queueing behind the first on a single activation.
    const parked = [grain.park(), grain.park()];
    await grain.release();
    await grain.release();

    expect(new Set(await Promise.all(parked)).size).toBe(2);
  });

  it("does not directory-register a stateless-worker activation", async () => {
    const c = await buildCluster();
    const client = await c.client;
    await client.getGrain(IWorkerPool, "k").ping();

    // A stateless worker's activations are interchangeable, which the
    // single-winner directory CAS cannot express — so they are never
    // registered, whichever silo's pool the call joined.
    const id = new GrainId("WorkerPool", "k");
    for (const silo of c.silos) expect(await silo.host.directory.lookup(id)).toBeUndefined();
  });

  it("still directory-registers an ordinary grain's activation", async () => {
    const c = await buildCluster();
    const client = await c.client;
    await client.getGrain(IPlain, "p").ping();

    // The stateless short-circuit must not swallow the ordinary path: this
    // activation is the single one for its id, and is resolvable through the
    // directory.
    const id = new GrainId("Plain", "p");
    const registered = await c.silos[0]!.host.directory.lookup(id);
    expect(registered?.silo).toEqual(c.silos[0]!.address);
  });
});
