import { once } from "node:events";
import { afterAll, afterEach, beforeAll, describe, expect, it } from "vitest";
import { WebSocket, WebSocketServer } from "ws";
import { metrics } from "@opentelemetry/api";
import {
  AggregationTemporality,
  InMemoryMetricExporter,
  MeterProvider,
  PeriodicExportingMetricReader,
} from "@opentelemetry/sdk-metrics";
import { GrainId } from "@thresh/core/grain-id";
import type { Logger } from "@thresh/core/logger";
import { SiloAddress } from "@thresh/core/silo-address";
import { JsonSerializer } from "@thresh/messaging/json-serializer";
import { nextCorrelationId, type Message } from "@thresh/messaging/message";
import { MessagePackSerializer } from "@thresh/messaging/msgpack-serializer";
import {
  PROTOCOL_VERSION,
  type Connection,
  type ConnectionPreamble,
  type Listener,
} from "@thresh/messaging/transport";
import { WebSocketTransport } from "@thresh/messaging/web-socket-transport";

const CLUSTER = "c1";
const wire = new MessagePackSerializer();
const body = new JsonSerializer();
const loopback = (name: string) => new SiloAddress(name, `uid-${name}`, "127.0.0.1:0");
const preamble = (self: SiloAddress): ConnectionPreamble => ({
  protocolVersion: PROTOCOL_VERSION,
  siloAddress: self,
  clusterId: CLUSTER,
  epoch: 1,
});
const NOT_MESSAGEPACK = Uint8Array.of(0xc1, 0xff, 0x00);
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));
const oneWay = (n: number): Message => ({
  correlationId: nextCorrelationId(),
  direction: "oneWay",
  targetGrain: new GrainId("Doubler", "x"),
  interfaceId: 0,
  method: "double",
  body: body.serialize(n),
});
const portOf = (endpoint: string) => Number(endpoint.slice(endpoint.lastIndexOf(":") + 1));

const exporter = new InMemoryMetricExporter(AggregationTemporality.CUMULATIVE);
const meterProvider = new MeterProvider({
  readers: [new PeriodicExportingMetricReader({ exporter, exportIntervalMillis: 100_000 })],
});
async function decodeFailures(): Promise<number> {
  await meterProvider.forceFlush();
  // Cumulative: each flush appends a snapshot, and only the latest is current.
  const metric = exporter
    .getMetrics()
    .slice(-1)
    .flatMap((rm) => rm.scopeMetrics)
    .flatMap((sm) => sm.metrics)
    .find((m) => m.descriptor.name === "thresh.messaging.decode.failed");
  return (metric?.dataPoints ?? []).reduce((sum, dp) => sum + Number(dp.value), 0);
}

function recordingLogger(): Logger & { warnings: string[] } {
  const warnings: string[] = [];
  return {
    warnings,
    debug() {},
    info() {},
    warn: (message) => void warnings.push(message),
    error: (message) => void warnings.push(message),
  };
}

describe("WebSocketTransport: a frame that fails to decode", () => {
  const faults: string[] = [];
  const onUncaught = (e: unknown) => void faults.push(`uncaught ${String(e)}`);
  const onUnhandled = (e: unknown) => void faults.push(`unhandled ${String(e)}`);
  const listeners: Listener[] = [];

  beforeAll(() => {
    metrics.setGlobalMeterProvider(meterProvider);
    process.on("uncaughtException", onUncaught);
    process.on("unhandledRejection", onUnhandled);
  });
  afterAll(async () => {
    process.off("uncaughtException", onUncaught);
    process.off("unhandledRejection", onUnhandled);
    await meterProvider.shutdown();
  });
  afterEach(async () => {
    faults.length = 0;
    await Promise.all(listeners.splice(0).map((l) => l.close()));
  });

  async function rawPeer(port: number): Promise<WebSocket> {
    const sock = new WebSocket(`ws://127.0.0.1:${port}`);
    await once(sock, "open");
    sock.send(wire.serialize(preamble(new SiloAddress("raw", "u-raw", "127.0.0.1:1"))));
    await once(sock, "message"); // the preamble ack
    return sock;
  }

  it("closes that connection, counts and logs it, and leaves a healthy connection alone", async () => {
    const logger = recordingLogger();
    const transport = new WebSocketTransport(CLUSTER, { logger });
    const received: number[] = [];
    const listener = await transport.listen(loopback("S"), (m) => {
      received.push(body.deserialize<number>(m.body));
    });
    listeners.push(listener);
    const port = portOf(listener.address.endpoint);

    const healthy = await new WebSocketTransport(CLUSTER).connect(
      listener.address,
      preamble(loopback("H")),
    );
    const bad = await rawPeer(port);
    const badClosed = once(bad, "close");
    const before = await decodeFailures();

    bad.send(NOT_MESSAGEPACK);
    await badClosed;
    healthy.send(oneWay(21));
    await sleep(100);

    expect(received).toEqual([21]);
    expect(await decodeFailures()).toBe(before + 1);
    expect(logger.warnings.join("\n")).toMatch(/undecodable/);
    expect(faults).toEqual([]);
    await healthy.close();
  });

  it("closes a connection whose preamble does not decode, counting it", async () => {
    const logger = recordingLogger();
    const listener = await new WebSocketTransport(CLUSTER, { logger }).listen(
      loopback("S"),
      () => undefined,
    );
    listeners.push(listener);
    const sock = new WebSocket(`ws://127.0.0.1:${portOf(listener.address.endpoint)}`);
    await once(sock, "open");
    const closed = once(sock, "close");
    const before = await decodeFailures();

    sock.send(NOT_MESSAGEPACK);
    await closed;

    expect(await decodeFailures()).toBe(before + 1);
    expect(faults).toEqual([]);
  });

  it("closes a connection whose preamble decodes to null", async () => {
    const listener = await new WebSocketTransport(CLUSTER).listen(loopback("S"), () => undefined);
    listeners.push(listener);
    const sock = new WebSocket(`ws://127.0.0.1:${portOf(listener.address.endpoint)}`);
    await once(sock, "open");
    const closed = once(sock, "close");

    sock.send(wire.serialize(null));
    await closed;

    expect(faults).toEqual([]);
  });

  it("closes a dialled connection that receives an undecodable frame from its peer", async () => {
    const logger = recordingLogger();
    const server = new WebSocketServer({ host: "127.0.0.1", port: 0 });
    await once(server, "listening");
    server.on("connection", (socket) => {
      socket.once("message", () => {
        socket.send(Uint8Array.of(1)); // ack
        socket.send(NOT_MESSAGEPACK);
      });
    });
    const address = server.address();
    const port = typeof address === "object" && address !== null ? address.port : 0;
    let markLost: () => void = () => undefined;
    const lost = new Promise<void>((resolve) => {
      markLost = resolve;
    });
    const before = await decodeFailures();

    const connection: Connection = await new WebSocketTransport(CLUSTER, { logger }).connect(
      new SiloAddress("peer", "u-peer", `127.0.0.1:${port}`),
      preamble(loopback("C")),
      () => undefined,
    );
    connection.onClose?.(() => markLost());
    await lost;

    expect(await decodeFailures()).toBe(before + 1);
    expect(logger.warnings.join("\n")).toMatch(/undecodable/);
    expect(faults).toEqual([]);
    await new Promise<void>((resolve) => {
      for (const c of server.clients) c.terminate();
      server.close(() => resolve());
    });
  });
});
