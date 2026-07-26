// Ported from dotnet/orleans test/Orleans.Streaming.Tests/StreamingTests/MemoryProgrammaticSubcribeTests.cs @ v10.1.0 (MIT).
// Base class: test/Orleans.Streaming.Tests/StreamingTests/ProgrammaticSubscribeTests/ProgrammaticSubscribeTestsRunner.cs @ v10.1.0 (MIT).
// Grains: test/Grains/TestGrains/ProgrammaticSubscribe/{SubscribeGrain,Passive_ConsumerGrain,TypedProducerGrain}.cs @ v10.1.0 (MIT).
//
// Every test method here is inherited from ProgrammaticSubscribeTestsRunner
// and runs, upstream, against the concrete MemoryProgrammaticSubcribeTests
// class — so ids use that concrete class's namespace, per this repo's
// convention for runner-derived suites (see memory-stream-batching-tests.test.ts).
//
// Upstream's `SubscriptionManager` test helper wraps `IStreamSubscriptionManagerAdmin`
// / `IStreamSubscriptionManager` (`StreamSubscriptionManagerType.ExplicitSubscribeOnly`):
// it lets a test add/remove/list a subscription for an arbitrary grain reference
// *administratively*, without that grain ever calling `subscribe()` itself, and
// independently of any implicit-subscription declaration. This is now ported as
// `StreamSubscriptionManager` (`@thresh/core/stream`), backed per-provider by
// `MemorySubscriptionManager` (`@thresh/streams/memory-subscription-manager`) and
// retrieved via `provider.getStreamSubscriptionManager()`. Unlike upstream (one
// provider-agnostic manager, parameterized per call by `streamProviderName`),
// each memory provider owns its own manager — a test targeting a second
// provider fetches that provider's manager directly instead of passing a
// provider name into a shared one; see `SubscribeToStreamsHandledByDifferentStreamProvider`
// below. Every memory provider exposes the manager (this framework has no
// "explicit-subscribe-only" provider config to gate it behind, unlike the
// `pubSub`-mode knob a fuller port might add — see `memory-stream-provider.ts`).
//
// A subscriber grain reacts to an administrative subscription the same way an
// implicit one does: `STREAM_SUBSCRIPTION_OBSERVER` (`@thresh/core/stream`),
// resolved the first time delivery is attempted. Returning `undefined` there
// is this port's analogue of upstream's `OnSubscribed` calling
// `handle.UnsubscribeAsync()` without ever resuming — see `JerkConsumerGrain`
// below, used by `ConsumerUnsubscribeOnAdd`.
//
// Upstream also synchronizes several of these tests on `StreamingDiagnosticObserver`,
// an internal pulling-agent diagnostic hook. This framework now has a
// test-side equivalent (`@thresh/parity/support/streaming-diagnostics`, used by
// `memory-stream-resume-tests.test.ts`/`memory-stream-cache-miss-tests.test.ts`),
// but it observes item-delivery/stream-inactive events, not the
// subscription-count events these programmatic-subscribe tests need. Every
// closed test below instead substitutes a deterministic poll (`waitUntil`)
// for the delivery/removal counts upstream reads off the observer — the
// memory provider's fan-out is synchronous-enough in-process that a short
// poll is reliably non-flaky.
import { expect } from "vitest";
import { grain } from "@thresh/core/decorators";
import { Grain } from "@thresh/core/grain";
import type { GrainKey } from "@thresh/core/grain-key";
import { defineGrainInterface } from "@thresh/core/grain-interface";
import { grainReferenceIdentity } from "@thresh/core/grain-reference";
import type { GrainWithGuidKey } from "@thresh/core/key-kinds";
import {
  STREAM_SUBSCRIPTION_OBSERVER,
  tryGetStreamSubscriptionManager,
  type StreamHandler,
  type StreamId,
} from "@thresh/core/stream";
import { orleansTest } from "@thresh/testing/orleans-test";
import { TestCluster } from "@thresh/testing/test-cluster";
import { randomGuidKey } from "@thresh/parity/support/keys";

const StreamProviderName = "StreamProvider1";
const StreamProviderName2 = "StreamProvider2";

interface ISubscribeGrain extends GrainWithGuidKey {
  canGetSubscriptionManager(providerName: string): Promise<boolean>;
}
const ISubscribeGrain = defineGrainInterface<ISubscribeGrain>("ISubscribeGrain", {
  options: { canGetSubscriptionManager: { readOnly: true } },
});

@grain()
class SubscribeGrain extends Grain implements ISubscribeGrain {
  async canGetSubscriptionManager(providerName: string): Promise<boolean> {
    const provider = this.runtime.getStreamProvider(providerName);
    return tryGetStreamSubscriptionManager(provider) !== undefined;
  }
}

interface IPassiveConsumerGrain extends GrainWithGuidKey {
  getNumberConsumed(): Promise<number>;
}
const IPassiveConsumerGrain = defineGrainInterface<IPassiveConsumerGrain>("IPassiveConsumerGrain", {
  options: { getNumberConsumed: { readOnly: true } },
});

/** Consumer grain which passively reacts to a subscription added administratively. */
@grain()
class PassiveConsumerGrain extends Grain implements IPassiveConsumerGrain {
  private consumed = 0;

  async getNumberConsumed(): Promise<number> {
    return this.consumed;
  }

  [STREAM_SUBSCRIPTION_OBSERVER](): StreamHandler<unknown> {
    return {
      onNext: async () => {
        this.consumed++;
      },
    };
  }
}

type IJerkConsumerGrain = GrainWithGuidKey;
const IJerkConsumerGrain = defineGrainInterface<IJerkConsumerGrain>("IJerkConsumerGrain");

/** Unsubscribes on any subscription added to it (Orleans `Jerk_ConsumerGrain`). */
@grain()
class JerkConsumerGrain extends Grain {
  [STREAM_SUBSCRIPTION_OBSERVER](): StreamHandler<unknown> | undefined {
    return undefined;
  }
}

interface ITypedProducerGrain extends GrainWithGuidKey {
  becomeProducer(streamKey: GrainKey, streamNamespace: string, providerName: string): Promise<void>;
  produce(): Promise<void>;
  getNumberProduced(): Promise<number>;
}

function typedProducerInterface(name: string) {
  return defineGrainInterface<ITypedProducerGrain>(name, {
    options: { getNumberProduced: { readOnly: true } },
  });
}

/** Produces a monotonically increasing counter to whatever stream `becomeProducer` names. */
class TypedProducerGrain extends Grain implements ITypedProducerGrain {
  private stream: { publish(event: number): Promise<void> } | undefined;
  private numProduced = 0;

  async becomeProducer(
    streamKey: GrainKey,
    streamNamespace: string,
    providerName: string,
  ): Promise<void> {
    const provider = this.runtime.getStreamProvider(providerName);
    this.stream = provider.getStream<number>(streamNamespace, streamKey);
  }

  async produce(): Promise<void> {
    if (this.stream === undefined) throw new Error("becomeProducer was not called");
    this.numProduced++;
    await this.stream.publish(this.numProduced);
  }

  async getNumberProduced(): Promise<number> {
    return this.numProduced;
  }
}

const ITypedProducerGrainProducingApple = typedProducerInterface(
  "ITypedProducerGrainProducingApple",
);
@grain()
class TypedProducerGrainProducingApple extends TypedProducerGrain {}

const ITypedProducerGrainProducingInt = typedProducerInterface("ITypedProducerGrainProducingInt");
@grain()
class TypedProducerGrainProducingInt extends TypedProducerGrain {}

/** Polls `check` until it returns `true`, or throws once `timeoutMs` elapses. */
async function waitUntil(check: () => Promise<boolean>, timeoutMs = 5000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    if (await check()) return;
    if (Date.now() >= deadline) throw new Error("waitUntil: condition never became true");
    await new Promise((resolve) => setTimeout(resolve, 10));
  }
}

// `TestCluster.streamProvider(name)` shares one `MemoryStreamProvider`
// instance cluster-wide per name (the way `storage`/`reminderTable` already
// are), so a producer grain and an admin-attached consumer grain landing on
// *different* silos of a multi-silo cluster still publish into, and register
// against, the same provider instance.
async function startCluster(): Promise<TestCluster> {
  return TestCluster.start({
    grains: [
      { ctor: SubscribeGrain, interfaces: [ISubscribeGrain] },
      { ctor: PassiveConsumerGrain, interfaces: [IPassiveConsumerGrain] },
      { ctor: JerkConsumerGrain, interfaces: [IJerkConsumerGrain] },
      { ctor: TypedProducerGrainProducingApple, interfaces: [ITypedProducerGrainProducingApple] },
      { ctor: TypedProducerGrainProducingInt, interfaces: [ITypedProducerGrainProducingInt] },
    ],
    configureSilo: (builder, _silo, cluster) => {
      builder.useMemoryStreams(
        StreamProviderName,
        undefined,
        cluster.streamProvider(StreamProviderName),
      );
      builder.useMemoryStreams(
        StreamProviderName2,
        undefined,
        cluster.streamProvider(StreamProviderName2),
      );
    },
  });
}

orleansTest(
  "UnitTests.StreamingTests.MemoryProgrammaticSubcribeTests.Programmatic_Subscribe_Provider_WithExplicitPubsub_TryGetStreamSubscrptionManager",
  async () => {
    const cluster = await startCluster();
    try {
      const subGrain = cluster.getGrain(ISubscribeGrain, randomGuidKey());
      expect(await subGrain.canGetSubscriptionManager(StreamProviderName)).toBe(true);
    } finally {
      await cluster.dispose();
    }
  },
);

orleansTest(
  "UnitTests.StreamingTests.MemoryProgrammaticSubcribeTests.Programmatic_Subscribe_CanUseNullNamespace",
  async () => {
    const cluster = await startCluster();
    try {
      const manager = tryGetStreamSubscriptionManager(
        cluster.getStreamProvider(StreamProviderName)!,
      )!;
      const streamId: StreamId = {
        provider: StreamProviderName,
        namespace: "",
        key: randomGuidKey().toString(),
      };
      const consumerRef = cluster.getGrain(IPassiveConsumerGrain, randomGuidKey());
      const grainId = grainReferenceIdentity(consumerRef)!.grainId;

      await manager.addSubscription(streamId, grainId);
      const subscriptions = await manager.getSubscriptions(streamId);
      expect(subscriptions).toHaveLength(1);
      await manager.removeSubscription(streamId, subscriptions[0]!.subscriptionId);
      expect(await manager.getSubscriptions(streamId)).toHaveLength(0);
    } finally {
      await cluster.dispose();
    }
  },
);

orleansTest(
  "UnitTests.StreamingTests.MemoryProgrammaticSubcribeTests.StreamingTests_Consumer_Producer_Subscribe",
  async () => {
    const cluster = await startCluster();
    try {
      const manager = tryGetStreamSubscriptionManager(
        cluster.getStreamProvider(StreamProviderName)!,
      )!;
      const streamKey = randomGuidKey();
      const streamId: StreamId = {
        provider: StreamProviderName,
        namespace: "EmptySpace",
        key: streamKey.toString(),
      };

      const consumerCount = 10;
      const consumers = Array.from({ length: consumerCount }, () =>
        cluster.getGrain(IPassiveConsumerGrain, randomGuidKey()),
      );
      for (const consumer of consumers) {
        await manager.addSubscription(streamId, grainReferenceIdentity(consumer)!.grainId);
      }

      const producer = cluster.getGrain(ITypedProducerGrainProducingApple, randomGuidKey());
      await producer.becomeProducer(streamKey, streamId.namespace, StreamProviderName);

      const eventCount = 10;
      for (let i = 0; i < eventCount; i++) await producer.produce();

      await waitUntil(async () => {
        for (const consumer of consumers) {
          if ((await consumer.getNumberConsumed()) < eventCount) return false;
        }
        return true;
      });

      const numProduced = await producer.getNumberProduced();
      for (const consumer of consumers) {
        expect(await consumer.getNumberConsumed()).toBe(numProduced);
      }
    } finally {
      await cluster.dispose();
    }
  },
);

orleansTest.excluded(
  "skipped upstream (dotnet/orleans#5635)",
  "UnitTests.StreamingTests.MemoryProgrammaticSubcribeTests.StreamingTests_Consumer_Producer_UnSubscribe",
);

orleansTest(
  "UnitTests.StreamingTests.MemoryProgrammaticSubcribeTests.StreamingTests_Consumer_Producer_GetSubscriptions",
  async () => {
    const cluster = await startCluster();
    try {
      const manager = tryGetStreamSubscriptionManager(
        cluster.getStreamProvider(StreamProviderName)!,
      )!;
      const streamId: StreamId = {
        provider: StreamProviderName,
        namespace: "EmptySpace",
        key: randomGuidKey().toString(),
      };

      const consumers = Array.from({ length: 2 }, () =>
        cluster.getGrain(IPassiveConsumerGrain, randomGuidKey()),
      );
      const expected = [];
      for (const consumer of consumers) {
        expected.push(
          await manager.addSubscription(streamId, grainReferenceIdentity(consumer)!.grainId),
        );
      }
      let subscriptionIds = (await manager.getSubscriptions(streamId))
        .map((s) => s.subscriptionId)
        .sort();
      expect(subscriptionIds).toEqual(expected.map((s) => s.subscriptionId).sort());

      await manager.removeSubscription(streamId, expected[0]!.subscriptionId);
      subscriptionIds = (await manager.getSubscriptions(streamId))
        .map((s) => s.subscriptionId)
        .sort();
      expect(subscriptionIds).toEqual([expected[1]!.subscriptionId]);
    } finally {
      await cluster.dispose();
    }
  },
);

orleansTest(
  "UnitTests.StreamingTests.MemoryProgrammaticSubcribeTests.StreamingTests_Consumer_Producer_ConsumerUnsubscribeOnAdd",
  async () => {
    const cluster = await startCluster();
    try {
      const manager = tryGetStreamSubscriptionManager(
        cluster.getStreamProvider(StreamProviderName)!,
      )!;
      const streamKey = randomGuidKey();
      const streamId: StreamId = {
        provider: StreamProviderName,
        namespace: "EmptySpace",
        key: streamKey.toString(),
      };

      const jerkCount = 10;
      for (let i = 0; i < jerkCount; i++) {
        const jerk = cluster.getGrain(IJerkConsumerGrain, randomGuidKey());
        await manager.addSubscription(streamId, grainReferenceIdentity(jerk)!.grainId);
      }

      const producer = cluster.getGrain(ITypedProducerGrainProducingInt, randomGuidKey());
      await producer.becomeProducer(streamKey, streamId.namespace, StreamProviderName);
      await producer.produce();

      await waitUntil(async () => (await manager.getSubscriptions(streamId)).length === 0);
      expect(await manager.getSubscriptions(streamId)).toHaveLength(0);
    } finally {
      await cluster.dispose();
    }
  },
);

orleansTest.excluded(
  "skipped upstream (dotnet/orleans#5650)",
  "UnitTests.StreamingTests.MemoryProgrammaticSubcribeTests.StreamingTests_Consumer_Producer_SubscribeToTwoStream_MessageWithPolymorphism",
);

orleansTest(
  "UnitTests.StreamingTests.MemoryProgrammaticSubcribeTests.StreamingTests_Consumer_Producer_SubscribeToStreamsHandledByDifferentStreamProvider",
  async () => {
    const cluster = await startCluster();
    try {
      const manager1 = tryGetStreamSubscriptionManager(
        cluster.getStreamProvider(StreamProviderName)!,
      )!;
      const manager2 = tryGetStreamSubscriptionManager(
        cluster.getStreamProvider(StreamProviderName2)!,
      )!;
      const streamKey = randomGuidKey();
      const streamId: StreamId = {
        provider: StreamProviderName,
        namespace: "EmptySpace",
        key: streamKey.toString(),
      };
      const streamKey2 = randomGuidKey();
      const streamId2: StreamId = {
        provider: StreamProviderName2,
        namespace: "EmptySpace2",
        key: streamKey2.toString(),
      };

      const consumerCount = 10;
      const consumers = Array.from({ length: consumerCount }, () =>
        cluster.getGrain(IPassiveConsumerGrain, randomGuidKey()),
      );
      for (const consumer of consumers) {
        await manager1.addSubscription(streamId, grainReferenceIdentity(consumer)!.grainId);
      }

      const producer = cluster.getGrain(ITypedProducerGrainProducingApple, randomGuidKey());
      await producer.becomeProducer(streamKey, streamId.namespace, StreamProviderName);
      const producer2 = cluster.getGrain(ITypedProducerGrainProducingApple, randomGuidKey());
      await producer2.becomeProducer(streamKey2, streamId2.namespace, StreamProviderName2);

      // Register the same consumers on the second provider's stream too.
      for (const consumer of consumers) {
        await manager2.addSubscription(streamId2, grainReferenceIdentity(consumer)!.grainId);
      }

      const eventCount = 10;
      const secondStreamEventCount = eventCount - 2;
      for (let i = 0; i < eventCount; i++) {
        await producer.produce();
        if (i < secondStreamEventCount) await producer2.produce();
      }

      const expectedPerConsumer = eventCount + secondStreamEventCount;
      await waitUntil(async () => {
        for (const consumer of consumers) {
          if ((await consumer.getNumberConsumed()) < expectedPerConsumer) return false;
        }
        return true;
      });

      const numProduced =
        (await producer.getNumberProduced()) + (await producer2.getNumberProduced());
      for (const consumer of consumers) {
        expect(await consumer.getNumberConsumed()).toBe(numProduced);
      }
    } finally {
      await cluster.dispose();
    }
  },
);
