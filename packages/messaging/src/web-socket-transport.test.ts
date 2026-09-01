import { afterEach, describe, expect, it } from "vitest";
import { RejectionError } from "@thresh/core/errors";
import { GrainId } from "@thresh/core/grain-id";
import { SiloAddress } from "@thresh/core/silo-address";
import { CorrelationTable } from "@thresh/messaging/correlation-table";
import { JsonSerializer } from "@thresh/messaging/json-serializer";
import { nextCorrelationId, responseTo, type Message } from "@thresh/messaging/message";
import type { Connection, ConnectionPreamble, Listener } from "@thresh/messaging/transport";
import { WebSocketTransport } from "@thresh/messaging/web-socket-transport";

const CLUSTER = "c1";
const body = new JsonSerializer();
const loopback = (name: string) => new SiloAddress(name, `uid-${name}`, "127.0.0.1:0");
const preamble = (self: SiloAddress, clusterId = CLUSTER): ConnectionPreamble => ({
  protocolVersion: 2,
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

  // A listener's onAccept connection now writes back down the ACCEPTED socket
  // rather than dialling the peer's advertised address — this is issue #65's
  // fix (a real successor to #55, which only made the reverse-dial version of
  // this work). The dialler advertises a deliberately UNROUTABLE address to
  // prove the push cannot possibly be arriving via a reverse dial.
  it("fires the onAccept hook with the preamble (including clientId) and the held connection answers down the socket the dialler dialled", async () => {
    const transportA = new WebSocketTransport(CLUSTER);
    const transportB = new WebSocketTransport(CLUSTER);
    const clientId = new GrainId("Client", "1");
    let acceptedPreamble: ConnectionPreamble | undefined;
    let heldConnection: Connection | undefined;
    let pushed: number | undefined;

    const listenerB = await transportB.listen(
      loopback("B"),
      () => undefined,
      (preambleIn, connection) => {
        acceptedPreamble = preambleIn;
        heldConnection = connection;
      },
    );
    openListeners.push(listenerB);

    // TEST-ONLY address (RFC 5737): nothing listens here, and nothing ever will.
    const unroutable = new SiloAddress("client", "uid-client", "203.0.113.1:9");
    const conn = await transportA.connect(
      listenerB.address,
      { ...preamble(unroutable), clientId },
      (msg) => {
        pushed = body.deserialize<number>(msg.body);
      },
    );

    await expect.poll(() => heldConnection).toBeDefined();
    expect(acceptedPreamble?.siloAddress).toEqual(unroutable);
    expect(acceptedPreamble?.clientId).toEqual(clientId);

    heldConnection?.send(request(nextCorrelationId(), "oneWay", 13));

    await expect.poll(() => pushed, { timeout: 2000 }).toBe(13);
    await conn.close();
  });

  it("completes a request/response round trip over the single dialled socket, with the dialler never listening", async () => {
    const transportA = new WebSocketTransport(CLUSTER);
    const transportB = new WebSocketTransport(CLUSTER);
    const table = new CorrelationTable();
    let heldConnection: Connection | undefined;

    const listenerB = await transportB.listen(
      loopback("B"),
      () => undefined,
      (_preambleIn, connection) => {
        heldConnection = connection;
      },
    );
    openListeners.push(listenerB);

    const unroutable = new SiloAddress("client", "uid-client", "203.0.113.1:9");
    await transportA.connect(listenerB.address, preamble(unroutable), (msg) => {
      table.complete(msg);
    });

    await expect.poll(() => heldConnection).toBeDefined();
    const corr = nextCorrelationId();
    const pending = table.register(corr, 2000);
    heldConnection?.send(
      responseTo(request(corr, "request", 0), "success", body.serialize(42), listenerB.address),
    );

    const response = await pending;
    expect(body.deserialize<number>(response.body)).toBe(42);
  });

  // The ack handler used to be `socket.once("message")`, which would consume (or race) a push
  // arriving in the same tick as the ack, not just the ack itself. `connect` now installs ONE
  // persistent handler before the preamble send: frame 1 resolves the ack, every later frame goes
  // to `onMessage`.
  it("does not lose a push sent in the same tick as the preamble ack", async () => {
    const transportA = new WebSocketTransport(CLUSTER);
    const transportB = new WebSocketTransport(CLUSTER);
    let pushed: number | undefined;

    const listenerB = await transportB.listen(
      loopback("B"),
      () => undefined,
      (_preambleIn, connection) => {
        // Push immediately on accept, in the same tick the ack goes out.
        connection.send(request(nextCorrelationId(), "oneWay", 77));
      },
    );
    openListeners.push(listenerB);

    await transportA.connect(listenerB.address, preamble(loopback("client")), (msg) => {
      pushed = body.deserialize<number>(msg.body);
    });

    await expect.poll(() => pushed, { timeout: 2000 }).toBe(77);
  });

  it("rejects a dialler whose preamble carries an old protocol version", async () => {
    const transportA = new WebSocketTransport(CLUSTER);
    const transportB = new WebSocketTransport(CLUSTER);
    const listenerB = await transportB.listen(loopback("B"), () => undefined);
    openListeners.push(listenerB);

    await expect(
      transportA.connect(listenerB.address, {
        ...preamble(loopback("A")),
        protocolVersion: 1,
      }),
    ).rejects.toBeInstanceOf(RejectionError);
  });
});
