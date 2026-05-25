import { describe, expect, it } from "vitest";
import { GrainCallTimeoutError, RejectionError } from "@tsva/core/errors";
import { GrainId } from "@tsva/core/grain-id";
import { SiloAddress } from "@tsva/core/silo-address";
import { CorrelationTable, type CorrelationTimer } from "@tsva/messaging/correlation-table";
import { InProcessNetwork, InProcessTransport } from "@tsva/messaging/in-process-transport";
import { nextCorrelationId, responseTo, type Message } from "@tsva/messaging/message";
import { JsonSerializer } from "@tsva/messaging/json-serializer";
import type { ConnectionPreamble } from "@tsva/messaging/transport";

const CLUSTER = "c1";
const A = new SiloAddress("silo-A", "ua", "a:1");
const B = new SiloAddress("silo-B", "ub", "b:1");
const ser = new JsonSerializer();
const preamble = (self: SiloAddress, clusterId = CLUSTER): ConnectionPreamble => ({
  protocolVersion: 1,
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
