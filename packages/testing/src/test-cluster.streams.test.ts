import { describe, expect, it } from "vitest";
import { grain, implicitStreamSubscription } from "@thresh/core/decorators";
import { Grain } from "@thresh/core/grain";
import { defineGrainInterface } from "@thresh/core/grain-interface";
import type { GrainKey } from "@thresh/core/key-kinds";
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

describe("TestCluster silos sharing one memory stream provider", () => {
  it("delivers an implicit subscription after the last-built silo is stopped", async () => {
    const cluster = await start();
    try {
      await cluster.silos[0]!.host.getGrain(IShared, "stopped").touch();
      await cluster.stopSilo(cluster.silos[2]!);
      await cluster.streamProvider().getStream<string>("shared", "stopped").publish("hello");
      await waitFor(async () => (seen["stopped"] ?? []).length === 1);
      expect(seen["stopped"]).toEqual(["hello"]);
    } finally {
      await cluster.dispose();
    }
  });

  it("delivers an implicit subscription after the last-built silo is killed", async () => {
    const cluster = await start();
    try {
      await cluster.silos[0]!.host.getGrain(IShared, "killed").touch();
      await cluster.killSilo(cluster.silos[2]!);
      await cluster.streamProvider().getStream<string>("shared", "killed").publish("hello");
      await waitFor(async () => (seen["killed"] ?? []).length === 1);
      expect(seen["killed"]).toEqual(["hello"]);
    } finally {
      await cluster.dispose();
    }
  });

  it("keeps streams other silos use alive when one silo stops, and disposes them when all stop", async () => {
    const cluster = await start();
    try {
      const provider = cluster.streamProvider();
      expect(provider.boundSilos).toBe(3);
      await cluster.stopSilo(cluster.silos[2]!);
      expect(provider.boundSilos).toBe(2);
      expect(provider.disposed).toBe(false);
      await cluster.stopSilo(cluster.silos[1]!);
      expect(provider.disposed).toBe(false);
      await cluster.stopSilo(cluster.silos[0]!);
      expect(provider.boundSilos).toBe(0);
      expect(provider.disposed).toBe(true);
    } finally {
      await cluster.dispose();
    }
  });
});
