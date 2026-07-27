// Ported from dotnet/orleans test/Orleans.Streaming.Tests/StreamingTests/ImplicitSubscriptionKeyTypeGrainTests.cs @ v10.1.0 (MIT).
// Grain: test/Grains/TestGrainInterfaces/IImplicitSubscriptionKeyTypeGrain.cs,
// test/Grains/TestGrains/ImplicitSubscriptionWithKeyTypeGrain.cs @ v10.1.0 (MIT).
//
// Upstream publishes onto the stream via `fixture.Client.GetStreamProvider(...)`
// from test code, outside any grain. This framework has no separate
// client-process gateway a test reaches a stream provider through, so the test
// reaches the memory provider via `TestCluster.getStreamProvider` (which
// forwards to the primary silo — see that method's doc) instead.
//
// The consumer grain (`ImplicitSubscriptionWithLongKeyGrain` upstream) declares
// `[ImplicitStreamSubscription]` and subscribes itself from `OnActivateAsync`;
// this framework's implicit-subscription model instead has the grain expose a
// `STREAM_SUBSCRIPTION_OBSERVER` handler and never call `subscribe` at all
// (see `packages/runtime/src/implicit-stream-delivery.test.ts`) — ported that
// way, with equivalent behaviour: the grain receives the stream's events with
// no explicit subscribe call.
import { expect } from "vitest";
import { grain, implicitStreamSubscription } from "@thresh/core/decorators";
import { Grain } from "@thresh/core/grain";
import { defineGrainInterface } from "@thresh/core/grain-interface";
import type { GrainWithIntegerKey } from "@thresh/core/key-kinds";
import { STREAM_SUBSCRIPTION_OBSERVER, type StreamHandler } from "@thresh/core/stream";
import { orleansTest } from "@thresh/testing/orleans-test";
import { TestCluster } from "@thresh/testing/test-cluster";

const STREAM_NAMESPACE = "IImplicitSubscriptionLongKeyGrain";

interface IImplicitSubscriptionLongKeyGrain extends GrainWithIntegerKey {
  getValue(): Promise<number>;
}
const IImplicitSubscriptionLongKeyGrain = defineGrainInterface<IImplicitSubscriptionLongKeyGrain>(
  "IImplicitSubscriptionLongKeyGrain",
  { options: { getValue: { readOnly: true } } },
);

@grain()
@implicitStreamSubscription(STREAM_NAMESPACE)
class ImplicitSubscriptionWithLongKeyGrain
  extends Grain
  implements IImplicitSubscriptionLongKeyGrain
{
  private value = 0;

  async getValue(): Promise<number> {
    return this.value;
  }

  [STREAM_SUBSCRIPTION_OBSERVER](_namespace: string, _key: string): StreamHandler<number> {
    return {
      onNext: async (data) => {
        this.value = data;
      },
    };
  }
}

orleansTest("UnitTests.StreamingTests.ImplicitSubscriptionKeyTypeGrainTests.LongKey", async () => {
  const cluster = await TestCluster.start({
    grains: [
      {
        ctor: ImplicitSubscriptionWithLongKeyGrain,
        interfaces: [IImplicitSubscriptionLongKeyGrain],
      },
    ],
  });
  try {
    const grainKey = 13n;
    const value = 87;
    const streamProvider = cluster.getStreamProvider();
    if (streamProvider === undefined) throw new Error("no default stream provider configured");
    const stream = streamProvider.getStream<number>(STREAM_NAMESPACE, grainKey);

    // The memory provider's implicit-subscriber fan-out is awaited inside
    // `publish` itself (see `MemoryStream.publishBatch`), so — unlike
    // upstream's poll loop against a real cluster — the delivery has
    // already happened by the time `publish` resolves.
    await stream.publish(value);

    const consumer = cluster.getGrain(IImplicitSubscriptionLongKeyGrain, grainKey);
    expect(await consumer.getValue()).toBe(value);
  } finally {
    await cluster.dispose();
  }
});
