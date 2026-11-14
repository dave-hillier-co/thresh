import { afterAll, afterEach, beforeAll, describe, expect, it } from "vitest";
import { metrics } from "@opentelemetry/api";
import {
  AggregationTemporality,
  InMemoryMetricExporter,
  MeterProvider,
  PeriodicExportingMetricReader,
} from "@opentelemetry/sdk-metrics";
import { GrainId } from "@thresh/core/grain-id";
import type { Logger } from "@thresh/core/logger";
import type { MembershipService } from "@thresh/core/membership";
import { SiloAddress } from "@thresh/core/silo-address";
import { InProcessNetwork, InProcessTransport } from "@thresh/messaging/in-process-transport";
import { MessagePackSerializer } from "@thresh/messaging/msgpack-serializer";
import { nextCorrelationId, type Message } from "@thresh/messaging/message";
import { PROTOCOL_VERSION } from "@thresh/messaging/transport";
import { ClusterNode } from "@thresh/runtime/cluster-node";
import { StaticMembershipService } from "@thresh/runtime/static-membership";

const CLUSTER = "c1";
const serializer = new MessagePackSerializer();
const silo = (n: number) => new SiloAddress(`silo-${n}`, `uid-${n}`, `silo-${n}:11111`);
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

const exporter = new InMemoryMetricExporter(AggregationTemporality.CUMULATIVE);
const meterProvider = new MeterProvider({
  readers: [new PeriodicExportingMetricReader({ exporter, exportIntervalMillis: 100_000 })],
});
async function handlerFailures(): Promise<number> {
  await meterProvider.forceFlush();
  const metric = exporter
    .getMetrics()
    .slice(-1)
    .flatMap((rm) => rm.scopeMetrics)
    .flatMap((sm) => sm.metrics)
    .find((m) => m.descriptor.name === "thresh.messaging.handler.failed");
  return (metric?.dataPoints ?? []).reduce((sum, dp) => sum + Number(dp.value), 0);
}

class MembershipView implements MembershipService {
  constructor(
    private readonly shared: StaticMembershipService,
    private readonly local: SiloAddress,
  ) {}
  current() {
    return this.shared.current();
  }
  updates() {
    return this.shared.updates();
  }
  localSilo() {
    return this.local;
  }
}

describe("a system-message handler that throws", () => {
  const faults: unknown[] = [];
  const onFault = (e: unknown) => void faults.push(e);
  beforeAll(() => {
    metrics.setGlobalMeterProvider(meterProvider);
    process.on("unhandledRejection", onFault);
    process.on("uncaughtException", onFault);
  });
  afterAll(async () => {
    process.off("unhandledRejection", onFault);
    process.off("uncaughtException", onFault);
    await meterProvider.shutdown();
  });
  afterEach(() => {
    faults.length = 0;
  });

  it("is logged and counted, and the waiting peer gets an error response promptly", async () => {
    const network = new InProcessNetwork();
    const errors: string[] = [];
    const logger: Logger = {
      debug() {},
      info() {},
      warn() {},
      error: (message) => void errors.push(message),
    };
    const addresses = [silo(0), silo(9)];
    const membership = new StaticMembershipService(addresses[0]!, [addresses[0]!]);
    const node = new ClusterNode({
      local: addresses[0]!,
      clusterId: CLUSTER,
      membership: new MembershipView(membership, addresses[0]!),
      transport: new InProcessTransport(network, CLUSTER),
      activationOptions: { logger },
    });
    await node.start();

    // A hand-rolled peer: it listens for the node's reply and dials the node.
    const peerTransport = new InProcessTransport(network, CLUSTER);
    const replies: Message[] = [];
    await peerTransport.listen(addresses[1]!, (m) => void replies.push(m));
    const toNode = await peerTransport.connect(addresses[0]!, {
      protocolVersion: PROTOCOL_VERSION,
      siloAddress: addresses[1]!,
      clusterId: CLUSTER,
    });

    try {
      const before = await handlerFailures();
      const correlationId = nextCorrelationId();
      // "actcount" decodes its body unguarded; a body that is not MessagePack makes it throw.
      toNode.send({
        correlationId,
        direction: "request",
        system: "actcount",
        targetGrain: new GrainId("actcount", "x"),
        sendingSilo: addresses[1]!,
        interfaceId: 0,
        method: "",
        body: Uint8Array.of(0xc1, 0xff),
      });
      await sleep(50);

      const reply = replies.find((m) => m.correlationId === correlationId);
      expect(reply?.direction).toBe("response");
      expect(reply?.responseKind).toBe("error");
      expect(await handlerFailures()).toBe(before + 1);
      expect(errors.join("\n")).toMatch(/actcount/);
      expect(faults).toEqual([]);
      expect(serializer.deserialize<{ message: string }>(reply!.body).message).toBeTruthy();
    } finally {
      await node.stop();
    }
  });
});
