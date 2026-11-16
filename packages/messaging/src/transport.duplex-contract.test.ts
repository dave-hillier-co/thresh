import { afterEach, describe, expect, it } from "vitest";
import { GrainId } from "@thresh/core/grain-id";
import { SiloAddress } from "@thresh/core/silo-address";
import {
  FaultInjectingTransport,
  NetworkFaults,
} from "@thresh/messaging/fault-injecting-transport";
import { InProcessNetwork, InProcessTransport } from "@thresh/messaging/in-process-transport";
import { JsonSerializer } from "@thresh/messaging/json-serializer";
import { responseTo, type Message } from "@thresh/messaging/message";
import {
  PROTOCOL_VERSION,
  type Connection,
  type ConnectionPreamble,
  type Listener,
  type Transport,
} from "@thresh/messaging/transport";
import { WebSocketTransport } from "@thresh/messaging/web-socket-transport";

/**
 * The duplex contract every transport keeps: a listener learns which connection each message
 * arrived on, an answer sent down that connection reaches the dialler (which never listens), and
 * a connection closed at either end is reported to the other and refuses further sends.
 */
const CLUSTER = "c1";
const ser = new JsonSerializer();
const preamble = (self: SiloAddress): ConnectionPreamble => ({
  protocolVersion: PROTOCOL_VERSION,
  siloAddress: self,
  clusterId: CLUSTER,
  epoch: 7,
});
const request = (from: SiloAddress, payload: number): Message => ({
  correlationId: 1n,
  direction: "request",
  targetGrain: new GrainId("Doubler", "x"),
  sendingSilo: from,
  interfaceId: 0,
  method: "double",
  body: ser.serialize(payload),
});

interface Harness {
  /** A transport for the listening side and one for the dialling side. */
  make(): { listener: Transport; dialler: Transport };
  /** The address to listen on; the listener reports the one it bound. */
  listenAt(name: string): SiloAddress;
}

const inProcess: Harness = (() => {
  const network = new InProcessNetwork();
  return {
    make: () => ({
      listener: new InProcessTransport(network, CLUSTER),
      dialler: new InProcessTransport(network, CLUSTER),
    }),
    listenAt: (name) => new SiloAddress(name, `u-${name}`, `${name}:1`),
  };
})();

const webSocket: Harness = {
  make: () => ({
    listener: new WebSocketTransport(CLUSTER),
    dialler: new WebSocketTransport(CLUSTER),
  }),
  listenAt: (name) => new SiloAddress(name, `u-${name}`, "127.0.0.1:0"),
};

const faultInjecting: Harness = (() => {
  const network = new InProcessNetwork();
  const faults = new NetworkFaults();
  return {
    make: () => ({
      listener: new FaultInjectingTransport(new InProcessTransport(network, CLUSTER), faults),
      dialler: new FaultInjectingTransport(new InProcessTransport(network, CLUSTER), faults),
    }),
    listenAt: (name) => new SiloAddress(name, `u-${name}`, `${name}:1`),
  };
})();

const harnesses: Array<[string, Harness]> = [
  ["in-process transport", inProcess],
  ["WebSocket transport", webSocket],
  ["fault-injecting transport", faultInjecting],
];

const eventually = async (check: () => boolean): Promise<void> => {
  for (let i = 0; i < 200 && !check(); i++) await new Promise((r) => setTimeout(r, 5));
  expect(check()).toBe(true);
};

describe.each(harnesses)("%s: duplex connections", (_name, harness) => {
  const listeners: Listener[] = [];
  afterEach(async () => {
    await Promise.all(listeners.splice(0).map((l) => l.close()));
  });

  let names = 0;
  const caller = () => new SiloAddress(`caller-${++names}`, "u-caller", `caller:${names}`);

  it("answers a request down the connection it arrived on, to a dialler that never listens", async () => {
    const { listener, dialler } = harness.make();
    const accepted: Connection[] = [];
    const bound = await listener.listen(
      harness.listenAt("doubler"),
      (message, _from, via) => {
        via?.send(
          responseTo(message, "success", ser.serialize(ser.deserialize<number>(message.body) * 2)),
        );
      },
      (_preamble, connection) => accepted.push(connection),
    );
    listeners.push(bound);
    const answers: number[] = [];
    const self = caller();
    const conn = await dialler.connect(bound.address, preamble(self), (message) => {
      answers.push(ser.deserialize<number>(message.body));
    });

    conn.send(request(self, 21));

    await eventually(() => answers.length === 1);
    expect(answers).toEqual([42]);
  });

  it("hands the listener's handler the same connection its accept hook received", async () => {
    const { listener, dialler } = harness.make();
    const accepted: Connection[] = [];
    const arrivedOn: Array<Connection | undefined> = [];
    const bound = await listener.listen(
      harness.listenAt("identity"),
      (_message, _from, via) => {
        arrivedOn.push(via);
      },
      (_preamble, connection) => accepted.push(connection),
    );
    listeners.push(bound);
    const self = caller();
    const conn = await dialler.connect(bound.address, preamble(self), () => undefined);

    conn.send(request(self, 1));

    await eventually(() => arrivedOn.length === 1);
    expect(accepted).toHaveLength(1);
    expect(arrivedOn[0]).toBe(accepted[0]);
  });

  it("reports a close at the accepting end to the dialler, and refuses a send on it", async () => {
    const { listener, dialler } = harness.make();
    const accepted: Connection[] = [];
    const bound = await listener.listen(
      harness.listenAt("closer"),
      () => undefined,
      (_preamble, connection) => accepted.push(connection),
    );
    listeners.push(bound);
    const self = caller();
    const conn = await dialler.connect(bound.address, preamble(self), () => undefined);
    let lost = false;
    conn.onClose?.(() => {
      lost = true;
    });
    await eventually(() => accepted.length === 1);

    await accepted[0]!.close();

    await eventually(() => lost);
    expect(() => conn.send(request(self, 1))).toThrow();
    expect(() => accepted[0]!.send(request(self, 1))).toThrow();
  });
});
