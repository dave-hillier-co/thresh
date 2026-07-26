import { once } from "node:events";
import { afterEach, describe, expect, it } from "vitest";
import { WebSocket } from "ws";
import { RejectionError } from "@thresh/core/errors";
import { GrainId } from "@thresh/core/grain-id";
import { SiloAddress } from "@thresh/core/silo-address";
import { CorrelationTable } from "@thresh/messaging/correlation-table";
import { JsonSerializer } from "@thresh/messaging/json-serializer";
import { MessagePackSerializer } from "@thresh/messaging/msgpack-serializer";
import { nextCorrelationId, responseTo, type Message } from "@thresh/messaging/message";
import type { ConnectionPreamble, Listener } from "@thresh/messaging/transport";
import { WebSocketTransport } from "@thresh/messaging/web-socket-transport";

const CLUSTER = "c1";
const body = new JsonSerializer();
const loopback = (name: string) => new SiloAddress(name, `uid-${name}`, "127.0.0.1:0");
const preamble = (self: SiloAddress, clusterId = CLUSTER): ConnectionPreamble => ({
  protocolVersion: 1,
  siloAddress: self,
  clusterId,
});
const request = (
  correlationId: bigint,
  direction: "request" | "oneWay",
  payload: number,
): Message => ({
  correlationId,
  direction,
  targetGrain: new GrainId("Doubler", "x"),
  interfaceId: 0,
  method: "double",
  body: body.serialize(payload),
});

const openListeners: Listener[] = [];
afterEach(async () => {
  await Promise.all(openListeners.splice(0).map((l) => l.close()));
});

describe("WebSocketTransport", () => {
  it("routes a request and its response across two silos over real sockets", async () => {
    const transportA = new WebSocketTransport(CLUSTER);
    const transportB = new WebSocketTransport(CLUSTER);
    const table = new CorrelationTable();

    const listenerA = await transportA.listen(loopback("A"), (msg) => {
      table.complete(msg);
    });
    const listenerB = await transportB.listen(loopback("B"), async (msg, from) => {
      const arg = body.deserialize<number>(msg.body);
      const conn = await transportB.connect(from, preamble(listenerB.address));
      conn.send(responseTo(msg, "success", body.serialize(arg * 2), listenerB.address));
    });
    openListeners.push(listenerA, listenerB);

    const conn = await transportA.connect(listenerB.address, preamble(listenerA.address));
    const corr = nextCorrelationId();
    const pending = table.register(corr, 2000);
    conn.send(request(corr, "request", 21));

    const response = await pending;
    expect(response.responseKind).toBe("success");
    expect(body.deserialize<number>(response.body)).toBe(42);
  });

  it("delivers a one-way message with no response", async () => {
    const transportA = new WebSocketTransport(CLUSTER);
    const transportB = new WebSocketTransport(CLUSTER);
    let received: number | undefined;

    const listenerB = await transportB.listen(loopback("B"), (msg) => {
      received = body.deserialize<number>(msg.body);
    });
    openListeners.push(listenerB);

    const conn = await transportA.connect(listenerB.address, preamble(loopback("A")));
    conn.send(request(nextCorrelationId(), "oneWay", 7));

    await expect.poll(() => received, { timeout: 2000 }).toBe(7);
  });

  it("rejects a connection from a different cluster", async () => {
    const transportA = new WebSocketTransport("other");
    const transportB = new WebSocketTransport(CLUSTER);
    const listenerB = await transportB.listen(loopback("B"), () => undefined);
    openListeners.push(listenerB);

    await expect(
      transportA.connect(listenerB.address, preamble(loopback("A"), "other")),
    ).rejects.toBeInstanceOf(RejectionError);
  });

  it("fires the onAccept hook with the preamble (including clientId) and the held connection can push a message the connecting peer receives", async () => {
    const transportB = new WebSocketTransport(CLUSTER);
    const clientAddress = loopback("client");
    const clientId = new GrainId("Client", "1");
    const msgpack = new MessagePackSerializer();
    let acceptedPreamble: ConnectionPreamble | undefined;
    let heldConnection: Parameters<
      NonNullable<Parameters<WebSocketTransport["listen"]>[2]>
    >[1] | undefined;

    const listenerB = await transportB.listen(
      loopback("B"),
      () => undefined,
      (preambleIn, connection) => {
        acceptedPreamble = preambleIn;
        heldConnection = connection;
      },
    );
    openListeners.push(listenerB);

    const [host, port] = listenerB.address.endpoint.split(":");
    const socket = new WebSocket(`ws://${host}:${port}`);
    socket.binaryType = "arraybuffer";
    await once(socket, "open");
    socket.send(
      msgpack.serialize({
        protocolVersion: 1,
        siloAddress: clientAddress,
        clusterId: CLUSTER,
        clientId,
      } satisfies ConnectionPreamble),
    );
    await once(socket, "message"); // ACK

    await expect.poll(() => heldConnection).toBeDefined();
    expect(acceptedPreamble?.siloAddress).toEqual(clientAddress);
    expect(acceptedPreamble?.clientId).toEqual(clientId);

    const pushed = new Promise<number>((resolve) => {
      socket.once("message", (data: ArrayBuffer) => {
        const msg = msgpack.deserialize<Message>(new Uint8Array(data));
        resolve(body.deserialize<number>(msg.body));
      });
    });
    heldConnection?.send(request(nextCorrelationId(), "oneWay", 13));

    await expect(pushed).resolves.toBe(13);
    socket.close();
  });
});
