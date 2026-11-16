import { afterEach, describe, expect, it } from "vitest";
import { grain } from "@thresh/core/decorators";
import { Grain } from "@thresh/core/grain";
import { defineGrainInterface } from "@thresh/core/grain-interface";
import type { GrainKey } from "@thresh/core/key-kinds";
import { noopLogger } from "@thresh/core/logger";
import { SiloAddress } from "@thresh/core/silo-address";
import { FakeEndpointSliceApi, type FakePod } from "@thresh/clustering-k8s/fake-endpoint-slice-api";
import {
  KubernetesEndpointWatch,
  type EndpointSliceSource,
  type RawEndpointSlice,
} from "@thresh/clustering-k8s/kubernetes-endpoint-watch";
import type { WatchEventType } from "@thresh/clustering-k8s/watched-endpoints";
import { InProcessNetwork } from "@thresh/messaging/in-process-transport";
import { createSilo, type SiloBuilder } from "@thresh/hosting/silo-builder";
import type { SiloHost } from "@thresh/hosting/silo-host";
import { waitFor } from "@thresh/testing/wait";

/** Health signals measure what they name (docs/project-review-2026-11-14.md, M8). */

interface Echo extends GrainKey<string> {
  ping(): Promise<string>;
}
const Echo = defineGrainInterface<Echo>("SiloHealthEcho");

@grain({ name: "SiloHealthEcho" })
class EchoGrain extends Grain implements Echo {
  async ping(): Promise<string> {
    return "pong";
  }
}

const local = new SiloAddress("silo-0", "uid-0", "silo-0:11111");
const started: SiloHost[] = [];

function silo(configure: (builder: SiloBuilder) => SiloBuilder, config = {}): SiloHost {
  const builder = createSilo({
    clusterId: "health",
    local,
    logger: noopLogger,
    gracefulShutdownMs: 0,
    ...config,
  }).useInProcessTransport(configureNetwork.current);
  return configure(builder)
    .registerGrain(EchoGrain, { interfaces: [Echo] })
    .build();
}
const configureNetwork = { current: new InProcessNetwork() };

async function boot(host: SiloHost): Promise<SiloHost> {
  started.push(host);
  await host.start();
  return host;
}

afterEach(async () => {
  for (const host of started.splice(0)) await host.stop().catch(() => undefined);
  configureNetwork.current = new InProcessNetwork();
});

describe("overloaded", () => {
  it("follows the load-shedding detector, in both directions", async () => {
    const host = await boot(
      silo((b) => b.useStaticMembership([local]), {
        loadShedding: { loadSheddingEnabled: true },
        // The default source reads this process's own event loop, which a busy test runner
        // can push past the threshold by itself. A fixed, idle reading leaves the latch as
        // the only thing that moves the detector.
        environmentStatistics: {
          getEnvironmentStatistics: () => ({
            cpuUsagePercentage: 5,
            memoryUsageBytes: 1,
            availableMemoryBytes: 1,
            maximumAvailableMemoryBytes: 2,
          }),
        },
        // Latching the reading is a test hook; a production silo refuses it.
        testHooks: true,
      }),
    );
    expect(host.health.ready().checks.notOverloaded).toBe(true);
    expect(host.health.ready().ok).toBe(true);

    await host.loadShedding.latchOverloaded();
    expect(host.health.ready().checks.notOverloaded).toBe(false);
    expect(host.health.ready().ok).toBe(false);

    await host.loadShedding.unlatchOverloaded();
    expect(host.health.ready().ok).toBe(true);
  });
});

describe("transportReady", () => {
  it("turns false when the listener closes underneath the silo", async () => {
    const host = await boot(silo((b) => b.useStaticMembership([local])));
    expect(host.health.ready().checks.transportReady).toBe(true);

    configureNetwork.current.unregister(local); // the listener is gone
    expect(host.health.ready().checks.transportReady).toBe(false);
    expect(host.health.ready().ok).toBe(false);
    // Liveness is not the transport's business: the process itself is fine.
    expect(host.health.live().ok).toBe(true);
  });
});

describe("liveness", () => {
  const wedgeProbe = (b: SiloBuilder): SiloBuilder =>
    b.useStaticMembership([local]).addIncomingCallFilter(async (ctx) => {
      if (ctx.interfaceName === "Thresh.Runtime.ISiloProbe") throw new Error("wedged");
      await ctx.invoke();
    });

  it("fails once the self-probe has failed continuously for the configured period", async () => {
    const host = await boot(
      silo(wedgeProbe, {
        selfProbe: { intervalMs: 10, timeoutMs: 10, missedThreshold: 1, livenessFailureMs: 60 },
      }),
    );
    await waitFor(() => !host.health.ready().checks.dispatcherResponsive);
    expect(host.health.ready().ok).toBe(false);
    await waitFor(() => !host.health.live().ok);
  });

  it("stays live, though not ready, when no period is configured", async () => {
    const host = await boot(
      silo(wedgeProbe, { selfProbe: { intervalMs: 10, timeoutMs: 10, missedThreshold: 1 } }),
    );
    await waitFor(() => !host.health.ready().checks.dispatcherResponsive);
    await new Promise((resolve) => setTimeout(resolve, 100));
    expect(host.health.live().ok).toBe(true);
  });
});

describe("a Kubernetes API outage", () => {
  /** The API server's list+watch with an outage switch: while down every list fails and the watch drops. */
  class OutageSource implements EndpointSliceSource {
    down = false;
    private onClose: ((err?: unknown) => void) | undefined;
    constructor(private readonly inner: EndpointSliceSource) {}
    list(): ReturnType<EndpointSliceSource["list"]> {
      return this.down ? Promise.reject(new Error("api server down")) : this.inner.list();
    }
    watch(
      resourceVersion: string | undefined,
      onEvent: (type: WatchEventType, slice: RawEndpointSlice) => void,
      onClose: (err?: unknown) => void,
    ): () => void {
      this.onClose = onClose;
      return this.inner.watch(resourceVersion, onEvent, onClose);
    }
    sever(): void {
      this.down = true;
      this.onClose?.(new Error("connection reset"));
    }
  }

  it("is exposed as watch state, and changes neither readiness nor liveness", async () => {
    const api = new FakeEndpointSliceApi();
    const pod: FakePod = { name: "silo-0", uid: "uid-0", ip: "10.0.0.1", ready: true };
    api.setPod(pod);
    const address = new SiloAddress(pod.name, pod.uid, api.endpointOf(pod));
    const source = new OutageSource(api.source());
    const watch = new KubernetesEndpointWatch(source, { reconnectMs: 10, random: () => 0 });
    const host = createSilo({
      clusterId: "health-k8s",
      local: address,
      logger: noopLogger,
      gracefulShutdownMs: 0,
      selfProbe: { enabled: false },
    })
      .useKubernetesMembership(watch, { portName: "silo" })
      .useInProcessTransport(new InProcessNetwork())
      .registerGrain(EchoGrain, { interfaces: [Echo] })
      .build();
    await boot(host);
    await waitFor(() => host.health.ready().ok);
    expect(watch.status().connected).toBe(true);

    source.sever();
    await waitFor(() => watch.status().reconnectFailures >= 2);

    expect(watch.status().connected).toBe(false);
    expect(host.health.ready().ok).toBe(true);
    expect(host.health.live().ok).toBe(true);
    expect(await host.getGrain(Echo, "k").ping()).toBe("pong");
  });
});
