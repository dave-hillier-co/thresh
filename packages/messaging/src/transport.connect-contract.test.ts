import { createServer, type Server } from "node:net";
import { once } from "node:events";
import { afterEach, describe, expect, it } from "vitest";
import { WebSocketServer } from "ws";
import { RejectionError } from "@thresh/core/errors";
import { SiloAddress } from "@thresh/core/silo-address";
import { FakeTimeProvider } from "@thresh/core/test-support/fake-time-provider";
import type { TimeProvider } from "@thresh/core/time-provider";
import { InProcessNetwork, InProcessTransport } from "@thresh/messaging/in-process-transport";
import {
  CONNECT_TIMEOUT_MS,
  PROTOCOL_VERSION,
  type ConnectFailure,
  type ConnectionPreamble,
  type Transport,
} from "@thresh/messaging/transport";
import { WebSocketTransport } from "@thresh/messaging/web-socket-transport";

const CLUSTER = "c1";
const SELF = new SiloAddress("silo-self", "u-self", "127.0.0.1:1");
const preamble: ConnectionPreamble = {
  protocolVersion: PROTOCOL_VERSION,
  siloAddress: SELF,
  clusterId: CLUSTER,
};

/** How each transport is made to present the three unreachable-peer situations. */
interface Harness {
  readonly transport: Transport;
  nothingListening(): Promise<SiloAddress>;
  closesDuringHandshake(): Promise<SiloAddress>;
  neverAnswers(): Promise<SiloAddress>;
  cleanup(): Promise<void>;
}

function inProcessHarness(time: TimeProvider): Harness {
  const network = new InProcessNetwork();
  const peer = (n: string) => new SiloAddress(n, `u-${n}`, `${n}:1`);
  return {
    transport: new InProcessTransport(network, CLUSTER, { timeProvider: time }),
    nothingListening: async () => peer("absent"),
    closesDuringHandshake: async () => {
      const address = peer("refuser");
      network.register({
        address,
        clusterId: CLUSTER,
        onMessage: () => undefined,
        onAccept: () => {
          throw new Error("listener closed during the handshake");
        },
      });
      return address;
    },
    neverAnswers: async () => {
      const address = peer("blackhole");
      network.blackhole(address);
      return address;
    },
    cleanup: async () => undefined,
  };
}

function webSocketHarness(time: TimeProvider): Harness {
  const servers: Array<{ close(): unknown }> = [];
  const sockets: Array<{ destroy(): unknown }> = [];
  const at = (port: number) => new SiloAddress("peer", "u-peer", `127.0.0.1:${port}`);
  const portOf = (s: Server | WebSocketServer): number => {
    const info = s.address();
    return typeof info === "object" && info !== null ? info.port : 0;
  };
  return {
    transport: new WebSocketTransport(CLUSTER, { timeProvider: time }),
    nothingListening: async () => {
      const probe = createServer().listen(0, "127.0.0.1");
      await once(probe, "listening");
      const port = portOf(probe);
      await new Promise<void>((resolve) => probe.close(() => resolve()));
      return at(port);
    },
    closesDuringHandshake: async () => {
      const server = new WebSocketServer({ host: "127.0.0.1", port: 0 });
      await once(server, "listening");
      server.on("connection", (socket) => socket.close(4001, "go away"));
      servers.push(server);
      return at(portOf(server));
    },
    neverAnswers: async () => {
      const server = createServer((socket) => sockets.push(socket)).listen(0, "127.0.0.1");
      await once(server, "listening");
      servers.push(server);
      return at(portOf(server));
    },
    cleanup: async () => {
      for (const s of sockets.splice(0)) s.destroy();
      for (const s of servers.splice(0)) s.close();
    },
  };
}

const harnesses: Array<[string, (time: TimeProvider) => Harness]> = [
  ["in-process transport", inProcessHarness],
  ["WebSocket transport", webSocketHarness],
];

describe.each(harnesses)("%s: connect to an unreachable peer", (_name, make) => {
  const time = new FakeTimeProvider();
  const harness = make(time);
  afterEach(() => harness.cleanup());

  async function rejection(promise: Promise<unknown>): Promise<RejectionError> {
    const err = await promise.then(
      () => undefined,
      (e: unknown) => e,
    );
    expect(err).toBeInstanceOf(RejectionError);
    return err as RejectionError;
  }

  it("rejects with a typed unknownTarget when nothing is listening", async () => {
    const to = await harness.nothingListening();
    const err = await rejection(harness.transport.connect(to, preamble));
    expect(err.kind).toBe("unknownTarget");
    expect((err as ConnectFailure).cause).toBe("refused");
  });

  it("rejects with the same typed rejection when the listener closes during the handshake", async () => {
    const to = await harness.closesDuringHandshake();
    const err = await rejection(harness.transport.connect(to, preamble));
    expect(err.kind).toBe("unknownTarget");
    expect((err as ConnectFailure).cause).toBe("handshake");
  });

  it("rejects with the same typed rejection when the dial outlasts the timeout", async () => {
    const to = await harness.neverAnswers();
    const dial = rejection(harness.transport.connect(to, preamble));
    time.advance(CONNECT_TIMEOUT_MS - 1);
    time.advance(1);
    const err = await dial;
    expect(err.kind).toBe("unknownTarget");
    expect(err.message).toMatch(/timed out/);
    expect((err as ConnectFailure).cause).toBe("timeout");
  });
});
