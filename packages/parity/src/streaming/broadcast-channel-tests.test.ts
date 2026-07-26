// Ported from dotnet/orleans test/Orleans.Streaming.Tests/StreamingTests/BroadcastChannels/BroadcastChannelTests.cs @ v10.1.0 (MIT).
// Grains: test/Grains/TestGrainInterfaces + TestGrains/BroadcastChannel/SimpleSubscriberGrain.cs @ v10.1.0 (MIT).
//
// Upstream publishes from `_fixture.Client.GetBroadcastChannelProvider(name)`
// (outside any grain), synchronizes on a `BroadcastChannelDiagnosticObserver`,
// and configures two named providers with different `FireAndForgetDelivery`
// settings plus a regex-namespace subscriber grain. This port adds:
//  - `ClientNode.getBroadcastChannelProvider` (`packages/client/src/client-node.ts`),
//    whose writers reach the gateway silo via a `BroadcastChannelPublisherInterface`
//    call the silo handles directly (`ClusterNode.dispatchBroadcastPublish`,
//    `packages/runtime/src/cluster-node.ts`) rather than through placement —
//    there's no real grain activation to address.
//  - `BroadcastChannelOptions.fireAndForgetDelivery` per named provider
//    (`SiloBuilder.useBroadcastChannels`), driving `ClusterNode.
//    publishToBroadcastChannel`'s per-subscriber failure semantics: fire-and-
//    forget swallows a subscriber's throw; non-fire-and-forget awaits every
//    subscriber and aggregates every failure into one thrown `AggregateError`
//    once all have been tried.
//  - `@thresh/core/broadcast-channel-diagnostics`, a delivery-event bus
//    `publishToBroadcastChannel` emits on, and `BroadcastChannelDiagnosticObserver`
//    (`packages/parity/src/support/broadcast-channel-diagnostic-observer.ts`),
//    the test-side counter that lets these tests await "N deliveries" without
//    racing the fan-out.
//  - predicate-based subscriber matching (`packages/core/src/
//    channel-namespace-predicate.ts`, already merged) wired into `ClusterNode.
//    broadcastGrainTypes`, so `RegexNamespaceSubscriberGrain` below (declared
//    with `@regexImplicitChannelSubscription`) matches alongside the
//    exact-namespace path. `SimpleSubscriberGrain` (upstream: bare
//    `[ImplicitChannelSubscription]`, matching every namespace) is ported as
//    `@regexImplicitChannelSubscription(".*")` — this framework has no
//    dedicated "all namespaces" predicate, and a match-everything regex is
//    exactly equivalent.
import { expect } from "vitest";
import {
  BROADCAST_CHANNEL_OBSERVER,
  channelKey,
  type BroadcastChannelHandler,
  type BroadcastChannelProvider,
  type ChannelId,
} from "@thresh/core/broadcast-channel";
import { grain, regexImplicitChannelSubscription } from "@thresh/core/decorators";
import { Grain } from "@thresh/core/grain";
import { defineGrainInterface } from "@thresh/core/grain-interface";
import { Guid } from "@thresh/core/guid";
import type { GrainWithStringKey } from "@thresh/core/key-kinds";
import type { ClientNode } from "@thresh/client/client-node";
import { orleansTest } from "@thresh/testing/orleans-test";
import { TestCluster } from "@thresh/testing/test-cluster";
import { BroadcastChannelDiagnosticObserver } from "../support/broadcast-channel-diagnostic-observer";
import { createClusterClient } from "../support/client";

const NS = "Tester.StreamingTests.BroadcastChannel.BroadcastChannelTests";

const PROVIDER_NAME = "BroadcastChannel";
const PROVIDER_NAME_NON_FIRE_AND_FORGET = "BroadcastChannelNonFireAndForget";
const CALL_TIMEOUT_MS = 500;

interface ISubscriberGrain extends GrainWithStringKey {
  getValues(channelId: ChannelId): Promise<number[]>;
  getOnPublishedCounter(): Promise<number>;
  throwsOnReceive(throwsOnReceive: boolean): Promise<void>;
}

type ISimpleSubscriberGrain = ISubscriberGrain;
const ISimpleSubscriberGrain = defineGrainInterface<ISimpleSubscriberGrain>(
  `${NS}.ISimpleSubscriberGrain`,
);

type IRegexNamespaceSubscriberGrain = ISubscriberGrain;
const IRegexNamespaceSubscriberGrain = defineGrainInterface<IRegexNamespaceSubscriberGrain>(
  `${NS}.IRegexNamespaceSubscriberGrain`,
);

abstract class SubscriberGrainBase extends Grain implements ISubscriberGrain {
  private readonly values = new Map<string, number[]>();
  private onPublishedCounter = 0;
  private throwsOnReceiveFlag = false;

  async getValues(channelId: ChannelId): Promise<number[]> {
    return this.values.get(channelKey(channelId)) ?? [];
  }

  async getOnPublishedCounter(): Promise<number> {
    return this.onPublishedCounter;
  }

  async throwsOnReceive(throwsOnReceive: boolean): Promise<void> {
    this.throwsOnReceiveFlag = throwsOnReceive;
  }

  [BROADCAST_CHANNEL_OBSERVER](namespace: string, key: string): BroadcastChannelHandler<unknown> {
    const storeKey = channelKey({ namespace, key });
    return {
      onPublished: (item) => {
        this.onPublishedCounter += 1;
        if (this.throwsOnReceiveFlag) throw new Error("Some error message here");
        const values = this.values.get(storeKey) ?? [];
        values.push(item as number);
        this.values.set(storeKey, values);
      },
    };
  }
}

// Upstream: `[ImplicitChannelSubscription]` with no namespace argument matches
// every namespace (`AllStreamNamespacesPredicate`); this framework has no
// dedicated "all namespaces" predicate, so a match-everything regex is used
// instead — equivalent, since every published namespace in these tests
// matches `.*`.
@grain()
@regexImplicitChannelSubscription(".*")
class SimpleSubscriberGrain extends SubscriberGrainBase implements ISimpleSubscriberGrain {}

@grain()
@regexImplicitChannelSubscription("multiple-namespaces-(.)+")
class RegexNamespaceSubscriberGrain
  extends SubscriberGrainBase
  implements IRegexNamespaceSubscriberGrain
{}

const grainRegistrations = [
  { ctor: SimpleSubscriberGrain, interfaces: [ISimpleSubscriberGrain] },
  { ctor: RegexNamespaceSubscriberGrain, interfaces: [IRegexNamespaceSubscriberGrain] },
];

function newGrainKey(): string {
  return Guid.newGuid().toString().replace(/-/g, "");
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

interface Fixture {
  client: ClientNode;
}

/**
 * Builds the cluster + client for one test (Orleans' shared `IClassFixture`,
 * rebuilt per test here rather than shared — cheap in-process silos, and it
 * keeps each test's channels/diagnostic counts isolated). Pinned to a single
 * silo: broadcast fan-out is dispatcher-routed cluster-wide regardless of
 * silo count, but a single gateway keeps the client's publish path and every
 * subscriber activation on one silo, matching how the rest of this framework's
 * client-facing ported suites avoid incidental cross-silo timing.
 */
async function withFixture(run: (fixture: Fixture) => Promise<void>): Promise<void> {
  const cluster = await TestCluster.start({
    initialSilos: 1,
    grains: grainRegistrations,
    configureSilo: (builder) => {
      builder
        // Orleans' default (`FireAndForgetDelivery = true`) — must be passed
        // explicitly here since this framework's own default is `false`
        // (non-fire-and-forget), unlike Orleans (see `ClusterNode`'s
        // constructor).
        .useBroadcastChannels(PROVIDER_NAME, { fireAndForgetDelivery: true })
        .useBroadcastChannels(PROVIDER_NAME_NON_FIRE_AND_FORGET, { fireAndForgetDelivery: false });
    },
  });
  try {
    const client = await createClusterClient(cluster, grainRegistrations);
    try {
      await run({ client });
    } finally {
      await client.close();
    }
  } finally {
    await cluster.dispose();
  }
}

async function clientPublishSingleChannelTestImpl(
  fixture: Fixture,
  provider: BroadcastChannelProvider,
  fireAndForget = true,
): Promise<void> {
  const grainKey = newGrainKey();
  const channelId: ChannelId = { namespace: "some-namespace", key: grainKey };

  const observer = BroadcastChannelDiagnosticObserver.create();
  try {
    const writer = provider.getChannelWriter<number>(channelId);

    await writer.publish(1);
    await writer.publish(2);
    await writer.publish(3);

    await observer.waitForDeliveryCount(channelId, 3, { timeoutMs: CALL_TIMEOUT_MS });

    const grain = fixture.client.getGrain(ISimpleSubscriberGrain, grainKey);
    const values = await grain.getValues(channelId);

    expect(values).toHaveLength(3);
    if (fireAndForget) {
      expect(values).toContain(1);
      expect(values).toContain(2);
      expect(values).toContain(3);
    } else {
      expect(values).toEqual([1, 2, 3]);
    }
  } finally {
    observer.dispose();
  }
}

async function clientPublishMultipleChannelTestImpl(
  fixture: Fixture,
  provider: BroadcastChannelProvider,
): Promise<void> {
  const grainKey = newGrainKey();
  const observer = BroadcastChannelDiagnosticObserver.create();
  try {
    const channels: Array<{ channelId: ChannelId; expectedValue: number }> = [];
    for (let i = 0; i < 10; i++) {
      const channelId: ChannelId = { namespace: `some-namespace${i}`, key: grainKey };
      const expectedValue = i + 50;
      channels.push({ channelId, expectedValue });
      await provider.getChannelWriter<number>(channelId).publish(expectedValue);
    }

    const grain = fixture.client.getGrain(ISimpleSubscriberGrain, grainKey);

    for (const channel of channels) {
      await observer.waitForDeliveryCount(channel.channelId, 1, { timeoutMs: CALL_TIMEOUT_MS });
      const values = await grain.getValues(channel.channelId);
      expect(values).toHaveLength(1);
      expect(values[0]).toBe(channel.expectedValue);
    }
  } finally {
    observer.dispose();
  }
}

async function multipleSubscribersChannelTestImpl(
  fixture: Fixture,
  provider: BroadcastChannelProvider,
  fireAndForget = true,
): Promise<void> {
  const grainKey = newGrainKey();
  const channelId: ChannelId = { namespace: "multiple-namespaces-0", key: grainKey };

  const observer = BroadcastChannelDiagnosticObserver.create();
  try {
    const writer = provider.getChannelWriter<number>(channelId);

    await writer.publish(1);
    await writer.publish(2);
    await writer.publish(3);

    // 3 items x 2 subscribers = 6 deliveries.
    await observer.waitForDeliveryCount(channelId, 6, { timeoutMs: CALL_TIMEOUT_MS });

    const grains = [
      fixture.client.getGrain(ISimpleSubscriberGrain, grainKey),
      fixture.client.getGrain(IRegexNamespaceSubscriberGrain, grainKey),
    ];

    for (const grain of grains) {
      const values = await grain.getValues(channelId);
      expect(values).toHaveLength(3);
      if (fireAndForget) {
        expect(values).toContain(1);
        expect(values).toContain(2);
        expect(values).toContain(3);
      } else {
        expect(values).toEqual([1, 2, 3]);
      }
    }
  } finally {
    observer.dispose();
  }
}

async function multipleSubscribersOneBadActorChannelTestImpl(
  fixture: Fixture,
  provider: BroadcastChannelProvider,
  fireAndForget = true,
): Promise<void> {
  const grainKey = newGrainKey();
  const channelId: ChannelId = { namespace: "multiple-namespaces-0", key: grainKey };

  const observer = BroadcastChannelDiagnosticObserver.create();
  try {
    const writer = provider.getChannelWriter<number>(channelId);

    const badGrain = fixture.client.getGrain(ISimpleSubscriberGrain, grainKey);
    const goodGrain = fixture.client.getGrain(IRegexNamespaceSubscriberGrain, grainKey);

    await writer.publish(1);
    if (fireAndForget) {
      // 1 item x 2 subscribers = 2 deliveries.
      await observer.waitForDeliveryCount(channelId, 2, { timeoutMs: CALL_TIMEOUT_MS });
      const values = await badGrain.getValues(channelId);
      expect(values).toHaveLength(1);
    }

    await badGrain.throwsOnReceive(true);
    if (fireAndForget) {
      await writer.publish(2);
      // Wait for the good grain's delivery (total: 3 successful deliveries).
      await observer.waitForDeliveryCount(channelId, 3, { timeoutMs: CALL_TIMEOUT_MS });

      // The bad grain's callback is still invoked (but throws), so the
      // delivery event doesn't fire for it. Poll its own counter instead.
      const deadline = Date.now() + CALL_TIMEOUT_MS;
      let counter = 0;
      for (;;) {
        counter = await badGrain.getOnPublishedCounter();
        if (counter === 2 || Date.now() >= deadline) break;
        await sleep(10);
      }
      expect(counter).toBe(2);
    } else {
      let thrown: unknown;
      try {
        await writer.publish(2);
      } catch (err) {
        thrown = err;
      }
      expect(thrown).toBeInstanceOf(AggregateError);
      expect((thrown as AggregateError).errors).toHaveLength(1);
    }

    await badGrain.throwsOnReceive(false);
    await writer.publish(3);

    // All remaining deliveries: 5 total (good: 3, bad: 2).
    await observer.waitForDeliveryCount(channelId, 5, { timeoutMs: CALL_TIMEOUT_MS });

    const goodValues = await goodGrain.getValues(channelId);
    expect(goodValues).toHaveLength(3);
    if (fireAndForget) {
      expect(goodValues).toContain(1);
      expect(goodValues).toContain(2);
      expect(goodValues).toContain(3);
    } else {
      expect(goodValues).toEqual([1, 2, 3]);
    }

    const badValues = await badGrain.getValues(channelId);
    expect(badValues).toHaveLength(2);
    if (fireAndForget) {
      expect(badValues).toContain(1);
      expect(badValues).toContain(3);
    } else {
      expect(badValues).toEqual([1, 3]);
    }
  } finally {
    observer.dispose();
  }
}

orleansTest("Tester.StreamingTests.BroadcastChannel.BroadcastChannelTests.ClientPublishSingleChannelTest", async () => {
  await withFixture((fixture) =>
    clientPublishSingleChannelTestImpl(
      fixture,
      fixture.client.getBroadcastChannelProvider(PROVIDER_NAME),
    ),
  );
});

orleansTest("Tester.StreamingTests.BroadcastChannel.BroadcastChannelTests.ClientPublishSingleChannelMultipleConsumersTest", async () => {
  await withFixture((fixture) =>
    multipleSubscribersChannelTestImpl(
      fixture,
      fixture.client.getBroadcastChannelProvider(PROVIDER_NAME),
    ),
  );
});

orleansTest("Tester.StreamingTests.BroadcastChannel.BroadcastChannelTests.ClientPublishMultipleChannelTest", async () => {
  await withFixture((fixture) =>
    clientPublishMultipleChannelTestImpl(
      fixture,
      fixture.client.getBroadcastChannelProvider(PROVIDER_NAME),
    ),
  );
});

orleansTest("Tester.StreamingTests.BroadcastChannel.BroadcastChannelTests.MultipleSubscribersOneBadActorChannelTest", async () => {
  await withFixture((fixture) =>
    multipleSubscribersOneBadActorChannelTestImpl(
      fixture,
      fixture.client.getBroadcastChannelProvider(PROVIDER_NAME),
    ),
  );
});

orleansTest("Tester.StreamingTests.BroadcastChannel.BroadcastChannelTests.NonFireAndForgetClientPublishSingleChannelTest", async () => {
  await withFixture((fixture) =>
    clientPublishSingleChannelTestImpl(
      fixture,
      fixture.client.getBroadcastChannelProvider(PROVIDER_NAME_NON_FIRE_AND_FORGET),
      false,
    ),
  );
});

orleansTest("Tester.StreamingTests.BroadcastChannel.BroadcastChannelTests.NonFireAndForgetClientPublishMultipleChannelTest", async () => {
  await withFixture((fixture) =>
    clientPublishMultipleChannelTestImpl(
      fixture,
      fixture.client.getBroadcastChannelProvider(PROVIDER_NAME_NON_FIRE_AND_FORGET),
    ),
  );
});

orleansTest("Tester.StreamingTests.BroadcastChannel.BroadcastChannelTests.NonFireAndForgetClientPublishSingleChannelMultipleConsumersTest", async () => {
  await withFixture((fixture) =>
    multipleSubscribersChannelTestImpl(
      fixture,
      fixture.client.getBroadcastChannelProvider(PROVIDER_NAME_NON_FIRE_AND_FORGET),
      false,
    ),
  );
});

orleansTest("Tester.StreamingTests.BroadcastChannel.BroadcastChannelTests.NonFireAndForgetMultipleSubscribersOneBadActorChannelTest", async () => {
  await withFixture((fixture) =>
    multipleSubscribersOneBadActorChannelTestImpl(
      fixture,
      fixture.client.getBroadcastChannelProvider(PROVIDER_NAME_NON_FIRE_AND_FORGET),
      false,
    ),
  );
});
