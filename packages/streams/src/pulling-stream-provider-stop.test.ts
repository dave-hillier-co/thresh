import { describe, expect, it } from "vitest";
import type { GrainId } from "@thresh/core/grain-id";
import { GeneratorPullingStreamProvider } from "@thresh/streams/generator-pulling-stream-provider";
import {
  PullingStreamProviderCore,
  type AppendableQueue,
  type SubscriptionRegistry,
} from "@thresh/streams/pulling-stream-provider-core";
import type { QueueEntry } from "@thresh/streams/redis-stream-queue";

/** An always-empty queue that records every poll a pulling agent makes of it. */
class RecordingQueue implements AppendableQueue {
  polls = 0;
  async getCursor(): Promise<number> {
    this.polls++;
    return 0;
  }
  async readAfter(): Promise<QueueEntry[]> {
    this.polls++;
    return [];
  }
  async commit(): Promise<void> {}
  async seek(): Promise<void> {}
  async append(): Promise<number> {
    return 0;
  }
}

const noSubscribers: SubscriptionRegistry = {
  subscribe: async () => {},
  unsubscribe: async () => {},
  subscribers: async (): Promise<GrainId[]> => [],
};

const flush = async (times = 5): Promise<void> => {
  for (let i = 0; i < times; i++) await new Promise((r) => setTimeout(r, 0));
};

/**
 * `SiloHost.stop` stops pulling providers BEFORE deactivating activations
 * (Orleans stops persistent stream providers at `ServiceLifecycleStage.Active`),
 * so a membership view change already mid-flight — or a Kafka partition
 * acquisition resolving late — can still ask the provider to adopt queues
 * afterwards. A stopped provider must not start agents again: nothing would
 * stop them, and they would keep delivering into a silo that is going away.
 */
describe("a stopped pulling stream provider", () => {
  it("PullingStreamProviderCore starts no agents after stop()", async () => {
    const queue = new RecordingQueue();
    const core = new PullingStreamProviderCore("p", [queue], noSubscribers, {
      pollIntervalMs: 1,
    });
    await core.stop();

    core.startAgentsFor([0]);
    await flush();

    expect(queue.polls).toBe(0);
    await core.stop();
  });

  it("GeneratorPullingStreamProvider starts no agents after stop()", async () => {
    const provider = new GeneratorPullingStreamProvider(
      "gen",
      { streamNamespace: "ns", eventsInStream: 5 },
      { queueCount: 1, pollIntervalMs: 1 },
    );
    const delivered: unknown[] = [];
    provider.setImplicitSubscribers((namespace) => (namespace === "ns" ? ["Sub"] : []));
    provider.setDeliver(async (_grainId, _streamKey, event) => {
      delivered.push(event);
    });
    await provider.stop();

    provider.startAgentsFor([0]);
    await flush();

    // The generator's queue mints events on first pull; an agent started
    // here would have delivered them to the implicit subscriber.
    expect(delivered).toEqual([]);
    await provider.stop();
  });
});
