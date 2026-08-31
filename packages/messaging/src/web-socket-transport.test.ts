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

  // The preamble half of this case is unchanged. Its push half used to assert
  // that the held connection wrote back down the ACCEPTED socket, proved with a
  // raw `ws` client that installed its own message handler. No Thresh node has
  // one — `connect()` returns a send-only `Connection` whose only inbound read
  // is the ack — so that mechanism could never deliver to a real peer, which is
  // issue #55: a gateway pushing to a client-hosted observer over real sockets
  // was silently dropped. The connection now reaches the peer's ADVERTISED
  // listener, as `InProcessTransport`'s reverse connection always did, so this
  // asserts arrival there instead of on the dialling socket.
  it("fires the onAccept hook with the preamble (including clientId) and the held connection reaches the peer's advertised listener", async () => {
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
    // The "client" listens on its own ephemeral port, exactly as `ClientNode`
    // does, and advertises the BOUND address in its preamble.
    const listenerA = await transportA.listen(loopback("client"), (msg) => {
      pushed = body.deserialize<number>(msg.body);
    });
    openListeners.push(listenerA, listenerB);

    const conn = await transportA.connect(listenerB.address, {
      ...preamble(listenerA.address),
      clientId,
    });

    await expect.poll(() => heldConnection).toBeDefined();
    expect(acceptedPreamble?.siloAddress).toEqual(listenerA.address);
    expect(acceptedPreamble?.clientId).toEqual(clientId);

    heldConnection?.send(request(nextCorrelationId(), "oneWay", 13));

    await expect.poll(() => pushed, { timeout: 2000 }).toBe(13);
    await conn.close();
  });

  // The reverse leg is dialled lazily on first send. Memoizing the dial PROMISE would make one
  // unlucky attempt permanent: the rejection stays cached, every later push awaits it, and the
  // peer is unreachable for the life of the accepted socket even once its listener is back.
  // The consumer symptom is silent -- `ClusterNode.deliverToProxy` registers a correlation and
  // fire-and-forgets, so the caller just blocks for the full call timeout with nothing logged.
  it("re-dials the reverse connection after a failed attempt instead of caching the failure", async () => {
    const transportA = new WebSocketTransport(CLUSTER);
    const transportB = new WebSocketTransport(CLUSTER);
    let heldConnection: Connection | undefined;
    let pushed: number | undefined;

    const listenerB = await transportB.listen(
      loopback("B"),
      () => undefined,
      (_preambleIn, connection) => {
        heldConnection = connection;
      },
    );
    openListeners.push(listenerB);

    // Bind once to claim a concrete port, then give it up: the address A advertises is real but
    // nothing is listening on it yet, so the gateway's first dial must fail.
    const probe = await transportA.listen(loopback("client"), () => undefined);
    const clientAddress = probe.address;
    await probe.close();

    const conn = await transportA.connect(listenerB.address, preamble(clientAddress));
    await expect.poll(() => heldConnection).toBeDefined();

    heldConnection?.send(request(nextCorrelationId(), "oneWay", 1)); // dropped: nothing is listening
    await expect.poll(() => heldConnection).toBeDefined();

    // The client comes up on the address it advertised, and a later push must reach it.
    const listenerA = await transportA.listen(clientAddress, (msg) => {
      pushed = body.deserialize<number>(msg.body);
    });
    openListeners.push(listenerA);

    await expect
      .poll(
        () => {
          heldConnection?.send(request(nextCorrelationId(), "oneWay", 21));
          return pushed;
        },
        { timeout: 5000 },
      )
      .toBe(21);
    await conn.close();
  });
});
