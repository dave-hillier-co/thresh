import { randomUUID } from "node:crypto";
import { afterAll, describe, expect, it } from "vitest";
import { createClient } from "redis";
import { grain } from "@thresh/core/decorators";
import { Grain } from "@thresh/core/grain";
import { defineGrainInterface } from "@thresh/core/grain-interface";
import type { GrainKey } from "@thresh/core/key-kinds";
import { SiloAddress } from "@thresh/core/silo-address";
import type { StreamHandler } from "@thresh/core/stream";
import { InProcessNetwork } from "@thresh/messaging/in-process-transport";
import { createSilo } from "@thresh/hosting/silo-builder";

const REDIS_URL = process.env.REDIS_URL ?? "redis://localhost:6379";
type Client = ReturnType<typeof createClient>;

async function reachable(url: string): Promise<Client | undefined> {
  // `reconnectStrategy: false` is load-bearing, not tidiness: node-redis retries a refused
  // connection forever by default, so `connect()` below never settles when no Redis is
  // listening and this module-load probe hangs the whole suite instead of skipping it.
  const probe = createClient({ url, socket: { reconnectStrategy: false, connectTimeout: 500 } });
  probe.on("error", () => {});
  try {
    await probe.connect();
    await probe.ping();
    return probe;
  } catch {
    try {
      await probe.destroy();
    } catch {
      /* never connected */
    }
    return undefined;
  }
}

const admin = await reachable(REDIS_URL);
const keyPrefix = `thresh-test:stream-e2e:${randomUUID()}`;

afterAll(async () => {
  if (admin === undefined) return;
  for await (const keys of admin.scanIterator({ MATCH: `${keyPrefix}*` })) {
    if (keys.length > 0) await admin.del(keys);
  }
  await admin.destroy();
});

interface IDevice extends GrainKey<string> {
  report(reading: number): Promise<void>;
}
const IDevice = defineGrainInterface<IDevice>("IDevice.redis");

interface IAggregator extends GrainKey<string> {
  readings(): Promise<number[]>;
}
const IAggregator = defineGrainInterface<IAggregator>("IAggregator.redis");

@grain()
class DeviceGrain extends Grain implements IDevice {
  async report(reading: number): Promise<void> {
    await this.runtime
      .getStreamProvider()
      .getStream<number>("telemetry", this.id.key)
      .publish(reading);
  }
}

@grain()
class AggregatorGrain extends Grain implements IAggregator {
  private received: number[] = [];

  override async onActivate(): Promise<void> {
    const stream = this.runtime.getStreamProvider().getStream<number>("telemetry", this.id.key);
    const existing = await stream.getSubscriptions();
    if (existing.length > 0) await existing[0]!.resume(this.handler());
    else await stream.subscribe(this.handler());
  }

  async readings(): Promise<number[]> {
    return this.received;
  }

  private handler(): StreamHandler<number> {
    return { onNext: async (reading) => void this.received.push(reading) };
  }
}

const local = new SiloAddress("silo-0", "uid-0", "silo-0:11111");

async function waitFor(predicate: () => Promise<boolean>, timeoutMs = 2000): Promise<void> {
  const start = Date.now();
  while (!(await predicate())) {
    if (Date.now() - start > timeoutMs) throw new Error("waitFor: timed out");
    await new Promise((r) => setTimeout(r, 10));
  }
}

describe.skipIf(admin === undefined)("Redis streams end-to-end", () => {
  it("delivers published events to a subscribing grain over Redis, as turns", async () => {
    const silo = createSilo({ clusterId: "c1", local })
      .useStaticMembership([local])
      .useInProcessTransport(new InProcessNetwork())
      .addRedisStreams("default", { url: REDIS_URL, keyPrefix })
      .registerGrain(DeviceGrain, { interfaces: [IDevice] })
      .registerGrain(AggregatorGrain, { interfaces: [IAggregator] })
      .build();
    await silo.start();
    try {
      await silo.getGrain(IAggregator, "dev-1").readings(); // activate -> subscribe
      await silo.getGrain(IDevice, "dev-1").report(10);
      await silo.getGrain(IDevice, "dev-1").report(20);
      await waitFor(
        async () => (await silo.getGrain(IAggregator, "dev-1").readings()).length === 2,
      );
      expect(await silo.getGrain(IAggregator, "dev-1").readings()).toEqual([10, 20]);
    } finally {
      await silo.stop();
    }
  });

  // Issue #64: `addRedisStreams` must thread the silo's `serviceId` into the
  // queue/registry/cursor keys `RedisPullingStreamProvider` builds.
  it("threads the silo's serviceId into the stream queue keys (issue #64)", async () => {
    const address = new SiloAddress("silo-svc", "uid-svc", "silo-svc:11111");
    const silo = createSilo({ clusterId: "c1", serviceId: "svc-a", local: address })
      .useStaticMembership([address])
      .useInProcessTransport(new InProcessNetwork())
      .addRedisStreams("default", { url: REDIS_URL, keyPrefix })
      .registerGrain(DeviceGrain, { interfaces: [IDevice] })
      .registerGrain(AggregatorGrain, { interfaces: [IAggregator] })
      .build();
    await silo.start();
    try {
      await silo.getGrain(IAggregator, "dev-svc").readings();
      await silo.getGrain(IDevice, "dev-svc").report(1);
      const keys: string[] = [];
      for await (const batch of admin!.scanIterator({
        MATCH: `${keyPrefix}:svc-a:streamq:default:*`,
      })) {
        keys.push(...(Array.isArray(batch) ? batch : [batch]));
      }
      expect(keys.length).toBeGreaterThan(0);
    } finally {
      await silo.stop();
    }
  });
});
