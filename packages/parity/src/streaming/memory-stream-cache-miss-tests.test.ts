// Ported from dotnet/orleans test/Orleans.Streaming.Tests/StreamingTests/MemoryStreamCacheMissTests.cs @ v10.1.0 (MIT).
// Base class: test/Orleans.Streaming.Tests/StreamingTests/StreamingCacheMissTests.cs @ v10.1.0 (MIT).
// Grain: test/Grains/TestGrainInterfaces/IImplicitSubscriptionCounterGrain.cs,
// test/Grains/TestGrains/ImplicitSubscriptionCounterGrain.cs @ v10.1.0 (MIT).
//
// Upstream's scenario is "an event survives cache eviction": send an event,
// wait past `DataMaxAgeInCache`, push enough other-stream traffic through the
// shared pulling agent to force its cache to evict the first stream's entry,
// then send a second event and confirm both were delivered. This provider
// (`packages/streams/src/memory-stream-provider.ts`) delivers by direct push,
// in-process, over an unbounded per-stream event log — there is no pulling
// agent and no bounded cache to evict from, so `DataMaxAgeInCache`/
// `DataMinTimeInCache` are accepted-but-inert (see
// `SiloBuilder.useMemoryStreams`'s doc) and the "wait for eviction, then hammer
// other streams" steps have nothing to exercise; they're dropped here. What's
// left, and what's actually ported, is the delivery guarantee upstream is
// really testing: every published event reaches its subscriber, synchronized
// via the streaming diagnostic bus (`@thresh/core/streaming-diagnostics`,
// `StreamingDiagnosticObserver`) instead of racing `publish()`.
import { describe, expect } from "vitest";
import { grain, implicitStreamSubscription, persistentState } from "@thresh/core/decorators";
import { Grain } from "@thresh/core/grain";
import { defineGrainInterface } from "@thresh/core/grain-interface";
import type { GrainKey } from "@thresh/core/key-kinds";
import type { PersistentState } from "@thresh/core/persistent-state";
import {
  STREAM_SUBSCRIPTION_OBSERVER,
  type StreamFilter,
  type StreamHandler,
  type StreamId,
} from "@thresh/core/stream";
import { orleansTest } from "@thresh/testing/orleans-test";
import { TestCluster } from "@thresh/testing/test-cluster";
import { randomGuidKey } from "@thresh/parity/support/keys";
import { StreamingDiagnosticObserver } from "@thresh/parity/support/streaming-diagnostics";
import type { Guid } from "@thresh/core/guid";

const StreamProviderName = "StreamingCacheMissTests";
const NAMESPACE = "IImplicitSubscriptionCounterGrain";

interface IImplicitSubscriptionCounterGrain extends GrainKey<Guid> {
  getEventCounter(): Promise<number>;
  getErrorCounter(): Promise<number>;
  deactivate(): Promise<void>;
  deactivateOnEvent(flag: boolean): Promise<void>;
}
const IImplicitSubscriptionCounterGrain = defineGrainInterface<IImplicitSubscriptionCounterGrain>(
  "IImplicitSubscriptionCounterGrain",
  {
    options: {
      getEventCounter: { readOnly: true },
      getErrorCounter: { readOnly: true },
    },
  },
);

interface CounterState {
  eventCounter: number;
  errorCounter: number;
}

// Orleans `ImplicitSubscriptionCounterGrain`: implicitly subscribed via
// `[ImplicitStreamSubscription(nameof(IImplicitSubscriptionCounterGrain))]`,
// persists its counters (`Grain<MyState>`) so they survive deactivation, and
// declares `IStreamSubscriptionObserver.OnSubscribed` — this framework's
// equivalent is `STREAM_SUBSCRIPTION_OBSERVER`.
@grain({ name: "UnitTests.Grains.ImplicitSubscriptionCounterGrain" })
@implicitStreamSubscription(NAMESPACE)
export class ImplicitSubscriptionCounterGrain
  extends Grain
  implements IImplicitSubscriptionCounterGrain
{
  @persistentState("counter", {
    defaultValue: (): CounterState => ({ eventCounter: 0, errorCounter: 0 }),
  })
  private state!: PersistentState<CounterState>;

  private deactivateOnEventFlag = false;

  async getEventCounter(): Promise<number> {
    return this.state.value.eventCounter;
  }

  async getErrorCounter(): Promise<number> {
    return this.state.value.errorCounter;
  }

  async deactivate(): Promise<void> {
    this.runtime.deactivateOnIdle();
  }

  async deactivateOnEvent(flag: boolean): Promise<void> {
    this.deactivateOnEventFlag = flag;
  }

  [STREAM_SUBSCRIPTION_OBSERVER](_namespace: string, _key: string): StreamHandler<unknown> {
    return {
      onNext: async () => {
        this.state.value.eventCounter += 1;
        await this.state.write();
        if (this.deactivateOnEventFlag) this.runtime.deactivateOnIdle();
      },
      onError: async () => {
        this.state.value.errorCounter += 1;
        await this.state.write();
      },
    };
  }
}

// Orleans `CustomStreamFilter`: only delivers a byte-array item whose first
// byte is `1`; an item that isn't a byte array (`data === default`) always
// passes.
export class CustomStreamFilter implements StreamFilter {
  shouldDeliver(_streamId: StreamId, item: unknown): boolean {
    const data = item instanceof Uint8Array ? item : undefined;
    return data === undefined || data[0] === 1;
  }
}

function makeInterestingData(firstByte: number): Uint8Array {
  const data = new Uint8Array(1024);
  data[0] = firstByte;
  return data;
}

describe("Tester.StreamingTests.MemoryStreamCacheMissTests", () => {
  orleansTest(
    "Tester.StreamingTests.MemoryStreamCacheMissTests.PreviousEventEvictedFromCacheTest",
    async () => {
      const cluster = await TestCluster.start({
        grains: [
          {
            ctor: ImplicitSubscriptionCounterGrain,
            interfaces: [IImplicitSubscriptionCounterGrain],
          },
        ],
        configureSilo: (builder) => builder.useMemoryStreams(StreamProviderName),
      });
      const observer = StreamingDiagnosticObserver.create();
      try {
        const streamProvider = cluster.getStreamProvider(StreamProviderName);
        if (streamProvider === undefined)
          throw new Error(`no "${StreamProviderName}" stream provider`);
        const key = randomGuidKey();
        const stream = streamProvider.getStream<Uint8Array>(NAMESPACE, key);
        const grainRef = cluster.getGrain(IImplicitSubscriptionCounterGrain, key);
        const streamId: StreamId = {
          provider: StreamProviderName,
          namespace: NAMESPACE,
          key: key.toString(),
        };
        const interestingData = makeInterestingData(1);

        await stream.publish(interestingData);
        await stream.publish(interestingData);

        await observer.waitForItemDeliveryCount(streamId, 2, StreamProviderName);
        expect(await grainRef.getEventCounter()).toBe(2);
        expect(await grainRef.getErrorCounter()).toBe(0);
      } finally {
        observer.dispose();
        await cluster.dispose();
      }
    },
  );

  orleansTest(
    "Tester.StreamingTests.MemoryStreamCacheMissTests.PreviousEventEvictedFromCacheWithFilterTest",
    async () => {
      const cluster = await TestCluster.start({
        grains: [
          {
            ctor: ImplicitSubscriptionCounterGrain,
            interfaces: [IImplicitSubscriptionCounterGrain],
          },
        ],
        configureSilo: (builder) =>
          builder
            .useMemoryStreams(StreamProviderName)
            .addStreamFilter(StreamProviderName, new CustomStreamFilter()),
      });
      const observer = StreamingDiagnosticObserver.create();
      try {
        const streamProvider = cluster.getStreamProvider(StreamProviderName);
        if (streamProvider === undefined)
          throw new Error(`no "${StreamProviderName}" stream provider`);
        const key = randomGuidKey();
        const stream = streamProvider.getStream<Uint8Array>(NAMESPACE, key);
        const grainRef = cluster.getGrain(IImplicitSubscriptionCounterGrain, key);
        const streamId: StreamId = {
          provider: StreamProviderName,
          namespace: NAMESPACE,
          key: key.toString(),
        };
        const skippedData = makeInterestingData(2);
        const interestingData = makeInterestingData(1);

        // Filtered out before it reaches the grain: no delivery observed for it.
        await stream.publish(skippedData);
        // Delivered.
        await stream.publish(interestingData);

        await observer.waitForItemDeliveryCount(streamId, 1, StreamProviderName);
        expect(await grainRef.getEventCounter()).toBe(1);
        expect(await grainRef.getErrorCounter()).toBe(0);
      } finally {
        observer.dispose();
        await cluster.dispose();
      }
    },
  );
});
