// Ported from dotnet/orleans test/Orleans.Streaming.Tests/StreamingTests/MemoryStreamResumeTests.cs @ v10.1.0 (MIT).
// Base class: test/Orleans.Streaming.Tests/StreamingTests/StreamingResumeTests.cs @ v10.1.0 (MIT).
// Grain: test/Grains/TestGrainInterfaces/IImplicitSubscriptionCounterGrain.cs,
// test/Grains/TestGrains/ImplicitSubscriptionCounterGrain.cs @ v10.1.0 (MIT).
//
// Upstream drives these off the pulling agent's `StreamInactivityPeriod`
// (when to report a stream inactive) and cache-eviction options
// (`MetadataMinTimeInCache`/`DataMaxAgeInCache`/`DataMinTimeInCache`, when a
// consumer's position falls out of the cache and it must resume from
// storage). This provider (`packages/streams/src/memory-stream-provider.ts`)
// pushes directly to subscribers over an unbounded per-stream event log — no
// pulling agent, no bounded cache — so the cache-eviction options are
// accepted-but-inert (see `SiloBuilder.useMemoryStreams`'s doc) and there is
// no "falls out of cache, resumes from storage" transition to observe. What
// IS ported faithfully: `StreamInactivityPeriod` itself (a real per-stream
// timer, see `MemoryStreamProvider`/`MemoryStream`) and the actual behavior
// upstream's name promises — a stream "resumes" (keeps delivering correctly)
// across an inactivity gap or a grain deactivation/reactivation, because the
// counter grain's state is durable across both. Synchronization uses the
// streaming diagnostic bus (`@tsva/core/streaming-diagnostics`,
// `StreamingDiagnosticObserver`) exactly as the cache-miss tests do, plus a
// `FakeTimeProvider` to fire the inactivity timer deterministically instead
// of upstream's real `Task.Delay`.
import { afterAll, beforeAll, describe, expect } from "vitest";
import { grain, implicitStreamSubscription, persistentState } from "@tsva/core/decorators";
import { Grain } from "@tsva/core/grain";
import { defineGrainInterface } from "@tsva/core/grain-interface";
import type { GrainWithGuidKey } from "@tsva/core/key-kinds";
import type { PersistentState } from "@tsva/core/persistent-state";
import { STREAM_SUBSCRIPTION_OBSERVER, type StreamHandler, type StreamId } from "@tsva/core/stream";
import { FakeTimeProvider } from "@tsva/core/test-support/fake-time-provider";
import { orleansTest } from "@tsva/testing/orleans-test";
import { TestCluster } from "@tsva/testing/test-cluster";
import { randomGuidKey } from "@tsva/parity/support/keys";
import { StreamingDiagnosticObserver } from "@tsva/parity/support/streaming-diagnostics";

const StreamProviderName = "StreamingCacheMissTests";
const NAMESPACE = "IImplicitSubscriptionCounterGrain";
const streamInactivityPeriodMs = 5_000;

interface IImplicitSubscriptionCounterGrain extends GrainWithGuidKey {
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

// Same fixture as memory-stream-cache-miss-tests.test.ts (upstream shares one
// `ImplicitSubscriptionCounterGrain` across both suites; ported per-file, like
// the rest of this suite's fixtures).
@grain({ name: "UnitTests.Grains.ImplicitSubscriptionCounterGrain" })
@implicitStreamSubscription(NAMESPACE)
class ImplicitSubscriptionCounterGrain extends Grain implements IImplicitSubscriptionCounterGrain {
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

function makeInterestingData(): Uint8Array {
  return new Uint8Array([1]);
}

describe("Tester.StreamingTests.MemoryStreamResumeTests", () => {
  let cluster: TestCluster;
  let time: FakeTimeProvider;

  beforeAll(async () => {
    time = new FakeTimeProvider();
    cluster = await TestCluster.start({
      time,
      grains: [
        {
          ctor: ImplicitSubscriptionCounterGrain,
          interfaces: [IImplicitSubscriptionCounterGrain],
        },
      ],
      configureSilo: (builder) =>
        builder.useMemoryStreams(StreamProviderName, { streamInactivityPeriodMs }),
    });
  });

  afterAll(async () => {
    await cluster.dispose();
  });

  async function resumeAfterInactivityImpl(waitForCacheToFlush: boolean): Promise<void> {
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
      const interestingData = makeInterestingData();

      await stream.publish(interestingData);
      await observer.waitForItemDeliveryCount(streamId, 1, StreamProviderName);
      expect(await grainRef.getEventCounter()).toBe(1);
      time.advance(streamInactivityPeriodMs);
      await observer.waitForStreamInactive(streamId, StreamProviderName);

      if (waitForCacheToFlush) {
        // Upstream forces cache eviction here by pushing enough other-stream
        // traffic through the shared pulling agent. There's no cache to
        // evict in this provider (see file header); what's left to exercise
        // is that unrelated streams' own inactivity timers fire independently
        // of the tested stream's.
        let lastOtherStreamId: StreamId | undefined;
        for (let i = 0; i < 5; i++) {
          const otherKey = randomGuidKey();
          const otherStream = streamProvider.getStream<Uint8Array>(NAMESPACE, otherKey);
          await otherStream.publish(interestingData);
          lastOtherStreamId = {
            provider: StreamProviderName,
            namespace: NAMESPACE,
            key: otherKey.toString(),
          };
        }
        time.advance(streamInactivityPeriodMs);
        await observer.waitForStreamInactive(lastOtherStreamId!, StreamProviderName);

        for (let i = 0; i < 5; i++) {
          const otherKey = randomGuidKey();
          const otherStream = streamProvider.getStream<Uint8Array>(NAMESPACE, otherKey);
          await otherStream.publish(interestingData);
        }
      }

      await stream.publish(interestingData);
      await observer.waitForItemDeliveryCount(streamId, 2, StreamProviderName);
      expect(await grainRef.getEventCounter()).toBe(2);
      expect(await grainRef.getErrorCounter()).toBe(0);
    } finally {
      observer.dispose();
    }
  }

  orleansTest("Tester.StreamingTests.MemoryStreamResumeTests.ResumeAfterInactivity", async () =>
    resumeAfterInactivityImpl(false),
  );

  orleansTest(
    "Tester.StreamingTests.MemoryStreamResumeTests.ResumeAfterInactivityNotInCache",
    async () => resumeAfterInactivityImpl(true),
  );

  orleansTest("Tester.StreamingTests.MemoryStreamResumeTests.ResumeAfterDeactivation", async () => {
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
      const interestingData = makeInterestingData();

      await stream.publish(interestingData);
      await observer.waitForItemDeliveryCount(streamId, 1, StreamProviderName);
      expect(await grainRef.getEventCounter()).toBe(1);
      time.advance(streamInactivityPeriodMs);
      await observer.waitForStreamInactive(streamId, StreamProviderName);
      await grainRef.deactivate();

      await stream.publish(interestingData);
      await observer.waitForItemDeliveryCount(streamId, 2, StreamProviderName);
      expect(await grainRef.getEventCounter()).toBe(2);
      expect(await grainRef.getErrorCounter()).toBe(0);
    } finally {
      observer.dispose();
    }
  });

  orleansTest(
    "Tester.StreamingTests.MemoryStreamResumeTests.ResumeAfterDeactivationActiveStream",
    async () => {
      const observer = StreamingDiagnosticObserver.create();
      try {
        const streamProvider = cluster.getStreamProvider(StreamProviderName);
        if (streamProvider === undefined)
          throw new Error(`no "${StreamProviderName}" stream provider`);
        const key = randomGuidKey();
        const stream = streamProvider.getStream<Uint8Array>(NAMESPACE, key);
        const otherStream = streamProvider.getStream<Uint8Array>(NAMESPACE, randomGuidKey());
        const grainRef = cluster.getGrain(IImplicitSubscriptionCounterGrain, key);
        await grainRef.deactivateOnEvent(true);
        const streamId: StreamId = {
          provider: StreamProviderName,
          namespace: NAMESPACE,
          key: key.toString(),
        };
        const interestingData = makeInterestingData();

        await stream.publish(interestingData);
        await otherStream.publish(interestingData);
        await otherStream.publish(interestingData);
        await otherStream.publish(interestingData);
        await stream.publish(interestingData);
        await observer.waitForItemDeliveryCount(streamId, 2, StreamProviderName);
        expect(await grainRef.getEventCounter()).toBe(2);
        time.advance(streamInactivityPeriodMs);
        await observer.waitForStreamInactive(streamId, StreamProviderName);
        await grainRef.deactivate();

        await stream.publish(interestingData);
        await observer.waitForItemDeliveryCount(streamId, 3, StreamProviderName);
        expect(await grainRef.getEventCounter()).toBe(3);
        expect(await grainRef.getErrorCounter()).toBe(0);
      } finally {
        observer.dispose();
      }
    },
  );

  // Needs publish/delivery decoupling so a slow subscriber's activation does
  // not block a fast subscriber on the same stream — deferred to a
  // follow-up slice. This provider's implicit fan-out (`MemoryStream.publishBatch`
  // → `MemoryStreamProvider.fanOut`) awaits each subscriber's delivery in
  // turn before `publish()` resolves (see `implicit-subscription-key-type-grain-tests.test.ts`'s
  // header for why), so a slow-activating subscriber on a shared stream
  // blocks every other subscriber's delivery too — there is no independent
  // per-consumer cursor pump for the implicit path the way `MemoryStream.deliverTo`
  // gives explicit subscribers.
  orleansTest.gap(
    "GAP-STREAM-CACHE-DIAGNOSTICS",
    "Tester.StreamingTests.MemoryStreamResumeTests.ResumeAfterSlowSubscriber",
  );
});
