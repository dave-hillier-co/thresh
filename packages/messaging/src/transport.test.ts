import { describe, expect, it } from "vitest";
import { GrainCallTimeoutError, RejectionError } from "@thresh/core/errors";
import { GrainId } from "@thresh/core/grain-id";
import { SiloAddress } from "@thresh/core/silo-address";
import { CorrelationTable, type CorrelationTimer } from "@thresh/messaging/correlation-table";
import { InProcessNetwork, InProcessTransport } from "@thresh/messaging/in-process-transport";
import { nextCorrelationId, responseTo, type Message } from "@thresh/messaging/message";
import { JsonSerializer } from "@thresh/messaging/json-serializer";
import type { ConnectionPreamble } from "@thresh/messaging/transport";

const CLUSTER = "c1";
const A = new SiloAddress("silo-A", "ua", "a:1");
const B = new SiloAddress("silo-B", "ub", "b:1");
const ser = new JsonSerializer();
const preamble = (self: SiloAddress, clusterId = CLUSTER): ConnectionPreamble => ({
  protocolVersion: 2,
  siloAddress: self,
  clusterId,
});
const request = (
  correlationId: bigint,
  direction: "request" | "oneWay",
  body: Uint8Array,
): Message => ({
  correlationId,
  direction,
  targetGrain: new GrainId("Doubler", "x"),
  sendingSilo: A,
  interfaceId: 0,
  method: "double",
  body,
});

describe("in-process transport + correlation table", () => {
  it("routes a request to a peer and matches the response to its caller", async () => {
    const net = new InProcessNetwork();
    const transportA = new InProcessTransport(net, CLUSTER);
    const transportB = new InProcessTransport(net, CLUSTER);
    const table = new CorrelationTable();

    // B doubles the argument and replies.
    await transportB.listen(B, async (msg, from) => {
      const arg = ser.deserialize<number>(msg.body);
      const conn = await transportB.connect(from, preamble(B));
      conn.send(responseTo(msg, "success", ser.serialize(arg * 2), B));
    });
    // A completes pending calls from inbound responses.
    await transportA.listen(A, (msg) => {
      table.complete(msg);
    });

    const connAB = await transportA.connect(B, preamble(A));
    const corr = nextCorrelationId();
    const pending = table.register(corr);
    connAB.send(request(corr, "request", ser.serialize(21)));

    const response = await pending;
    expect(response.responseKind).toBe("success");
    expect(ser.deserialize<number>(response.body)).toBe(42);
  });

  it("delivers a one-way message without a response", async () => {
    const net = new InProcessNetwork();
    const transportA = new InProcessTransport(net, CLUSTER);
    const transportB = new InProcessTransport(net, CLUSTER);
    let received: number | undefined;

    await transportB.listen(B, (msg) => {
      received = ser.deserialize<number>(msg.body);
    });
    const conn = await transportA.connect(B, preamble(A));
    conn.send(request(nextCorrelationId(), "oneWay", ser.serialize(7)));

    await new Promise((r) => setTimeout(r, 0));
    expect(received).toBe(7);
  });

  it("rejects a connection from a different cluster", async () => {
    const net = new InProcessNetwork();
    const transportA = new InProcessTransport(net, "other");
    const transportB = new InProcessTransport(net, CLUSTER);
    await transportB.listen(B, () => undefined);

    await expect(transportA.connect(B, preamble(A, "other"))).rejects.toBeInstanceOf(
      RejectionError,
    );
  });

  it("rejects when there is no listener at the target", async () => {
    const net = new InProcessNetwork();
    const transportA = new InProcessTransport(net, CLUSTER);
    await expect(transportA.connect(B, preamble(A))).rejects.toBeInstanceOf(RejectionError);
  });

  it("rejects a dialler whose preamble carries an old protocol version", async () => {
    const net = new InProcessNetwork();
    const transportA = new InProcessTransport(net, CLUSTER);
    const transportB = new InProcessTransport(net, CLUSTER);
    await transportB.listen(B, () => undefined);

    await expect(
      transportA.connect(B, { ...preamble(A), protocolVersion: 1 }),
    ).rejects.toBeInstanceOf(RejectionError);
  });

  it("times out a call whose response never arrives", async () => {
    let fire: (() => void) | undefined;
    const fakeTimer: CorrelationTimer = {
      set: (cb) => {
        fire = cb;
        return 1;
      },
      clear: () => undefined,
    };
    const table = new CorrelationTable(fakeTimer);
    const pending = table.register(nextCorrelationId(), 1000);
    fire?.();
    await expect(pending).rejects.toBeInstanceOf(GrainCallTimeoutError);
  });

  it("ignores a response with an unknown correlation id", () => {
    const table = new CorrelationTable();
    expect(table.complete(request(9999n, "request", ser.serialize(1)))).toBe(false);
  });
});

describe("in-process transport onAccept hook", () => {
  it("does not error on connect when the listener registers no onAccept", async () => {
    const net = new InProcessNetwork();
    const transportA = new InProcessTransport(net, CLUSTER);
    const transportB = new InProcessTransport(net, CLUSTER);
    await transportB.listen(B, () => undefined);

    await expect(transportA.connect(B, preamble(A))).resolves.toBeDefined();
  });

  it("fires onAccept with the preamble (including clientId) and the held connection reaches the dialler's onMessage hook, never a listener", async () => {
    const net = new InProcessNetwork();
    const gatewayTransport = new InProcessTransport(net, CLUSTER);
    const clientTransport = new InProcessTransport(net, CLUSTER);
    const client = new SiloAddress("client", "u-client", "client:1");
    const clientId = new GrainId("Client", "1");

    let receivedPreamble: ConnectionPreamble | undefined;
    let heldConnection: Awaited<ReturnType<InProcessTransport["connect"]>> | undefined;
    let clientReceived: number | undefined;

    await gatewayTransport.listen(
      B,
      () => undefined,
      (preambleIn, connection) => {
        receivedPreamble = preambleIn;
        heldConnection = connection;
      },
    );

    await clientTransport.connect(B, { ...preamble(client), clientId }, (msg) => {
      clientReceived = ser.deserialize<number>(msg.body);
    });

    expect(receivedPreamble?.clientId).toEqual(clientId);
    expect(heldConnection).toBeDefined();
    expect(net.lookup(client)).toBeUndefined(); // the dialler never listens

    heldConnection?.send(request(nextCorrelationId(), "oneWay", ser.serialize(99)));
    await new Promise((r) => setTimeout(r, 0));
    expect(clientReceived).toBe(99);
  });
});

describe("in-process transport duplex connect", () => {
  it("delivers a push down the dialled connection to the dialler's onMessage hook, with the dialler never registered on the network", async () => {
    const net = new InProcessNetwork();
    const gatewayTransport = new InProcessTransport(net, CLUSTER);
    const clientTransport = new InProcessTransport(net, CLUSTER);
    const client = new SiloAddress("client", "u-client", "client:never-registered");
    let heldConnection: Awaited<ReturnType<InProcessTransport["connect"]>> | undefined;
    let pushed: number | undefined;

    await gatewayTransport.listen(
      B,
      () => undefined,
      (_preambleIn, connection) => {
        heldConnection = connection;
      },
    );

    await clientTransport.connect(B, preamble(client), (msg) => {
      pushed = ser.deserialize<number>(msg.body);
    });

    expect(heldConnection).toBeDefined();
    expect(net.lookup(client)).toBeUndefined(); // the dialler never listens

    heldConnection?.send(request(nextCorrelationId(), "oneWay", ser.serialize(55)));
    await new Promise((r) => setTimeout(r, 0));
    expect(pushed).toBe(55);
  });

  it("completes a request/response round trip entirely over the dialled connection, without either side calling listen", async () => {
    const net = new InProcessNetwork();
    const gatewayTransport = new InProcessTransport(net, CLUSTER);
    const clientTransport = new InProcessTransport(net, CLUSTER);
    const client = new SiloAddress("client", "u-client", "client:round-trip");
    const table = new CorrelationTable();
    let heldConnection: Awaited<ReturnType<InProcessTransport["connect"]>> | undefined;

    await gatewayTransport.listen(
      B,
      () => undefined,
      (_preambleIn, connection) => {
        heldConnection = connection;
      },
    );

    await clientTransport.connect(B, preamble(client), (msg) => {
      table.complete(msg);
    });

    const corr = nextCorrelationId();
    const pending = table.register(corr, 2000);
    heldConnection?.send(
      responseTo(request(corr, "request", ser.serialize(0)), "success", ser.serialize(42), B),
    );

    const response = await pending;
    expect(ser.deserialize<number>(response.body)).toBe(42);
  });

  it("throws a RejectionError(unknownTarget) from the accepted connection's send when the dialler passed no onMessage", async () => {
    const net = new InProcessNetwork();
    const gatewayTransport = new InProcessTransport(net, CLUSTER);
    const clientTransport = new InProcessTransport(net, CLUSTER);
    const client = new SiloAddress("client", "u-client", "client:no-hook");
    let heldConnection: Awaited<ReturnType<InProcessTransport["connect"]>> | undefined;

    await gatewayTransport.listen(
      B,
      () => undefined,
      (_preambleIn, connection) => {
        heldConnection = connection;
      },
    );

    await clientTransport.connect(B, preamble(client)); // no onMessage passed

    expect(() =>
      heldConnection?.send(request(nextCorrelationId(), "oneWay", ser.serialize(1))),
    ).toThrow(RejectionError);
  });
});
