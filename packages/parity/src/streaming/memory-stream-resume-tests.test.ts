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
// streaming diagnostic bus (`@thresh/core/streaming-diagnostics`,
// `StreamingDiagnosticObserver`) exactly as the cache-miss tests do, plus a
// `FakeTimeProvider` to fire the inactivity timer deterministically instead
// of upstream's real `Task.Delay`.
import { afterAll, beforeAll, describe, expect } from "vitest";
import { grain, implicitStreamSubscription, persistentState } from "@thresh/core/decorators";
import { Grain } from "@thresh/core/grain";
import { defineGrainInterface } from "@thresh/core/grain-interface";
import type { GrainKey } from "@thresh/core/key-kinds";
import type { PersistentState } from "@thresh/core/persistent-state";
import {
  STREAM_SUBSCRIPTION_OBSERVER,
  type StreamHandler,
  type StreamId,
} from "@thresh/core/stream";
import { FakeTimeProvider } from "@thresh/core/test-support/fake-time-provider";
import { orleansTest } from "@thresh/testing/orleans-test";
import { TestCluster } from "@thresh/testing/test-cluster";
import { waitFor } from "@thresh/testing/wait";
import { randomGuidKey } from "@thresh/parity/support/keys";
import { StreamingDiagnosticObserver } from "@thresh/parity/support/streaming-diagnostics";
import type { Guid } from "@thresh/core/guid";

const StreamProviderName = "StreamingCacheMissTests";
const NAMESPACE = "IImplicitSubscriptionCounterGrain";
const streamInactivityPeriodMs = 5_000;

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

// Fixtures for ResumeAfterSlowSubscriber (Orleans `IFastImplicitSubscriptionCounterGrain` /
// `ISlowImplicitSubscriptionCounterGrain`, both `[ImplicitStreamSubscription("FastSlowImplicitSubscriptionCounterGrain")]`
// on the same shared counter grain base). The slow grain delays its own
// activation; the test asserts the fast grain's delivery isn't serialized
// behind it.
const FAST_SLOW_NAMESPACE = "FastSlowImplicitSubscriptionCounterGrain";
const SLOW_ACTIVATION_DELAY_MS = 2_000;

interface IFastSlowCounterGrain extends GrainKey<Guid> {
  getEventCounter(): Promise<number>;
}
const IFastImplicitSubscriptionCounterGrain = defineGrainInterface<IFastSlowCounterGrain>(
  "IFastImplicitSubscriptionCounterGrain",
  { options: { getEventCounter: { readOnly: true } } },
);
const ISlowImplicitSubscriptionCounterGrain = defineGrainInterface<IFastSlowCounterGrain>(
  "ISlowImplicitSubscriptionCounterGrain",
  { options: { getEventCounter: { readOnly: true } } },
);

abstract class FastSlowCounterGrainBase extends Grain implements IFastSlowCounterGrain {
  private eventCounter = 0;

  async getEventCounter(): Promise<number> {
    return this.eventCounter;
  }

  [STREAM_SUBSCRIPTION_OBSERVER](_namespace: string, _key: string): StreamHandler<unknown> {
    return {
      onNext: async () => {
        this.eventCounter += 1;
      },
    };
  }
}

@grain({ name: "UnitTests.Grains.FastImplicitSubscriptionCounterGrain" })
@implicitStreamSubscription(FAST_SLOW_NAMESPACE)
class FastImplicitSubscriptionCounterGrain extends FastSlowCounterGrainBase {}

@grain({ name: "UnitTests.Grains.SlowImplicitSubscriptionCounterGrain" })
@implicitStreamSubscription(FAST_SLOW_NAMESPACE)
class SlowImplicitSubscriptionCounterGrain extends FastSlowCounterGrainBase {
  override async onActivate(reason: Parameters<Grain["onActivate"]>[0]): Promise<void> {
    await new Promise((resolve) => setTimeout(resolve, SLOW_ACTIVATION_DELAY_MS));
    await super.onActivate(reason);
  }
}

async function waitUntilFastCounter(
  grain: IFastSlowCounterGrain,
  expected: number,
  timeoutMs: number,
): Promise<void> {
  await waitFor(async () => (await grain.getEventCounter()) === expected, {
    timeoutMs,
    intervalMs: 10,
  });
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

  // `MemoryStreamProvider.fanOut` now dispatches to every implicit/admin
  // subscriber concurrently (`Promise.allSettled`) instead of awaiting each
  // in turn, so a slow-activating subscriber sharing a stream with a fast
  // one no longer stalls the fast one's own delivery — see
  // `memory-stream-provider.ts`'s `fanOut`. `publish()` itself still only
  // resolves once every subscriber's delivery settles (the invariant
  // `implicit-subscription-key-type-grain-tests.test.ts` documents), so this
  // test — like upstream's real async pulling-agent delivery — doesn't await
  // `publish()` before checking the fast grain; it polls concurrently with
  // the in-flight publish, then drains the publish promises once the
  // assertions are done.
  orleansTest(
    "Tester.StreamingTests.MemoryStreamResumeTests.ResumeAfterSlowSubscriber",
    async () => {
      const fastSlowCluster = await TestCluster.start({
        grains: [
          {
            ctor: FastImplicitSubscriptionCounterGrain,
            interfaces: [IFastImplicitSubscriptionCounterGrain],
          },
          {
            ctor: SlowImplicitSubscriptionCounterGrain,
            interfaces: [ISlowImplicitSubscriptionCounterGrain],
          },
        ],
        configureSilo: (builder) => builder.useMemoryStreams(StreamProviderName),
      });
      const pendingPublishes: Promise<void>[] = [];
      try {
        const key = randomGuidKey();
        const streamProvider = fastSlowCluster.getStreamProvider(StreamProviderName);
        if (streamProvider === undefined)
          throw new Error(`no "${StreamProviderName}" stream provider`);
        const stream = streamProvider.getStream<Uint8Array>(
          "FastSlowImplicitSubscriptionCounterGrain",
          key,
        );
        const fastGrain = fastSlowCluster.getGrain(IFastImplicitSubscriptionCounterGrain, key);

        const start = Date.now();
        pendingPublishes.push(stream.publish(new Uint8Array([1])));
        // The fast grain's counter must reach 1 well before the slow
        // grain's activation delay elapses — proving delivery to the fast
        // subscriber isn't serialized behind the slow one's activation.
        await waitUntilFastCounter(fastGrain, 1, SLOW_ACTIVATION_DELAY_MS / 2);
        expect(Date.now() - start).toBeLessThan(SLOW_ACTIVATION_DELAY_MS);

        pendingPublishes.push(stream.publish(new Uint8Array([2])));
        await waitUntilFastCounter(fastGrain, 2, SLOW_ACTIVATION_DELAY_MS / 2);
      } finally {
        await Promise.allSettled(pendingPublishes);
        await fastSlowCluster.dispose();
      }
    },
    35_000,
  );
});
