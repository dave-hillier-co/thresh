import { describe, expect, it } from "vitest";
import { grain, implicitStreamSubscription } from "@thresh/core/decorators";
import { Grain } from "@thresh/core/grain";
import { defineGrainInterface } from "@thresh/core/grain-interface";
import type { GrainKey } from "@thresh/core/key-kinds";
import { FakeTimeProvider } from "@thresh/core/test-support/fake-time-provider";
import { onStreamingEvent, type StreamingEvent } from "@thresh/core/streaming-diagnostics";
import { STREAM_SUBSCRIPTION_OBSERVER, type StreamHandler } from "@thresh/core/stream";
import { TestCluster } from "@thresh/testing/test-cluster";
import { waitFor } from "@thresh/testing/wait";

const seen: Record<string, string[]> = {};

interface IShared extends GrainKey<string> {
  touch(): Promise<void>;
}
const IShared = defineGrainInterface<IShared>("IShared.test-cluster-streams");

@grain({ name: "IShared.test-cluster-streams" })
@implicitStreamSubscription("shared")
class SharedWatcherGrain extends Grain implements IShared {
  async touch(): Promise<void> {}
  [STREAM_SUBSCRIPTION_OBSERVER](_namespace: string, key: string): StreamHandler<string> {
    return { onNext: async (text) => void (seen[key] ??= []).push(text) };
  }
}

async function start(): Promise<TestCluster> {
  return TestCluster.start({
    initialSilos: 3,
    grains: [{ ctor: SharedWatcherGrain, interfaces: [IShared] }],
  });
}

/**
 * Publish until some publish reaches its subscriber. The survivors apply the
 * membership change asynchronously, so a delivery attempted in that window can
 * fail; each attempt uses a fresh stream key so a late arrival is not counted twice.
 */
async function publishUntilDelivered(cluster: TestCluster, label: string): Promise<void> {
  let attempt = 0;
  await waitFor(async () => {
    const key = `${label}-${attempt++}`;
    await cluster.streamProvider().getStream<string>("shared", key).publish("hello");
    await new Promise((resolve) => setTimeout(resolve, 20));
    return seen[key]?.[0] === "hello";
  });
}

describe("TestCluster silos sharing one memory stream provider", () => {
  it("delivers an implicit subscription after the last-built silo is stopped", async () => {
    const cluster = await start();
    try {
      await cluster.stopSilo(cluster.silos[2]!);
      await publishUntilDelivered(cluster, "stopped");
    } finally {
      await cluster.dispose();
    }
  });

  it("delivers an implicit subscription after the last-built silo is killed", async () => {
    const cluster = await start();
    try {
      await cluster.killSilo(cluster.silos[2]!);
      await publishUntilDelivered(cluster, "killed");
    } finally {
      await cluster.dispose();
    }
  });

  it("keeps streams other silos use alive when one silo stops, and disposes them when all stop", async () => {
    const time = new FakeTimeProvider();
    const cluster = await TestCluster.start({
      initialSilos: 3,
      grains: [{ ctor: SharedWatcherGrain, interfaces: [IShared] }],
      time,
    });
    const inactive: StreamingEvent[] = [];
    const off = onStreamingEvent((event) => {
      if (event.kind === "streamInactive") inactive.push(event);
    });
    try {
      const provider = cluster.streamProvider();
      provider.setStreamInactivityPeriod(5_000);
      expect(provider.boundSilos).toBe(3);

      await provider.getStream<string>("shared", "alive").publish("armed");
      await cluster.stopSilo(cluster.silos[2]!);
      expect(provider.boundSilos).toBe(2);
      await cluster.stopSilo(cluster.silos[1]!);
      // Two silos have stopped; the third still uses the stream, so its timer is armed.
      time.advance(5_000);
      expect(inactive).toHaveLength(1);

      await provider.getStream<string>("shared", "alive").publish("re-armed");
      await cluster.stopSilo(cluster.silos[0]!);
      expect(provider.boundSilos).toBe(0);
      time.advance(5_000);
      expect(inactive).toHaveLength(1);
    } finally {
      off();
      await cluster.dispose();
    }
  });
});
