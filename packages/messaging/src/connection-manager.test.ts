import { describe, expect, it } from "vitest";
import { SiloAddress } from "@thresh/core/silo-address";
import { GrainId } from "@thresh/core/grain-id";
import { ConnectionManager } from "@thresh/messaging/connection-manager";
import { InProcessNetwork, InProcessTransport } from "@thresh/messaging/in-process-transport";
import { PROCESS_EPOCH } from "@thresh/messaging/message";
import type { Connection, Listener, MessageHandler, Transport } from "@thresh/messaging/transport";

const self = new SiloAddress("self", "u0", "self:1");
const peer = (n: number) => new SiloAddress(`peer-${n}`, `u${n}`, `peer-${n}:1`);

class CountingTransport implements Transport {
  connects = 0;
  closes = 0;
  failNext = false;
  private readonly closeCallbacks: Array<(err?: unknown) => void> = [];

  async listen(_address: SiloAddress, _onMessage: MessageHandler): Promise<Listener> {
    throw new Error("not used");
  }

  async connect(): Promise<Connection> {
    this.connects++;
    if (this.failNext) {
      this.failNext = false;
      throw new Error("connect failed");
    }
    return {
      send: () => undefined,
      close: async () => void this.closes++,
      onClose: (callback) => this.closeCallbacks.push(callback),
    };
  }

  /** Simulates the transport losing the most-recently-dialled connection on its own. */
  dropUnderneath(err?: unknown): void {
    for (const callback of this.closeCallbacks.splice(0)) callback(err);
  }
}

describe("ConnectionManager", () => {
  it("opens one connection per peer and reuses it", async () => {
    const transport = new CountingTransport();
    const cm = new ConnectionManager(transport, self, "c1");
    const a = await cm.get(peer(1));
    const b = await cm.get(peer(1));
    expect(a).toBe(b);
    expect(transport.connects).toBe(1);
  });

  it("opens separate connections for different peers", async () => {
    const transport = new CountingTransport();
    const cm = new ConnectionManager(transport, self, "c1");
    await cm.get(peer(1));
    await cm.get(peer(2));
    expect(transport.connects).toBe(2);
  });

  it("reconnects after a connection is dropped", async () => {
    const transport = new CountingTransport();
    const cm = new ConnectionManager(transport, self, "c1");
    await cm.get(peer(1));
    await cm.drop(peer(1));
    expect(transport.closes).toBe(1);
    await cm.get(peer(1));
    expect(transport.connects).toBe(2);
  });

  it("does not cache a failed connect", async () => {
    const transport = new CountingTransport();
    transport.failNext = true;
    const cm = new ConnectionManager(transport, self, "c1");
    await expect(cm.get(peer(1))).rejects.toThrow("connect failed");
    // Next attempt retries rather than returning the cached rejection.
    await expect(cm.get(peer(1))).resolves.toBeDefined();
    expect(transport.connects).toBe(2);
  });

  it("closes every connection on closeAll", async () => {
    const transport = new CountingTransport();
    const cm = new ConnectionManager(transport, self, "c1");
    await cm.get(peer(1));
    await cm.get(peer(2));
    await cm.closeAll();
    expect(transport.closes).toBe(2);
  });

  it("re-dials the next time the peer is asked for after the transport drops the connection on its own", async () => {
    const transport = new CountingTransport();
    const cm = new ConnectionManager(transport, self, "c1");
    await cm.get(peer(1));
    expect(transport.connects).toBe(1);

    transport.dropUnderneath(new Error("ECONNRESET"));

    await cm.get(peer(1));
    expect(transport.connects).toBe(2);
  });

  it("notifies onConnectionLost with the peer when the transport drops the connection on its own", async () => {
    const transport = new CountingTransport();
    const lost: SiloAddress[] = [];
    const cm = new ConnectionManager(transport, self, "c1", undefined, undefined, (peerAddress) =>
      lost.push(peerAddress),
    );
    await cm.get(peer(1));

    transport.dropUnderneath(new Error("ECONNRESET"));

    expect(lost).toEqual([peer(1)]);
  });

  it("names the lost connection itself, so only the calls sent on it are failed", async () => {
    const transport = new CountingTransport();
    const lost: unknown[] = [];
    const cm = new ConnectionManager(
      transport,
      self,
      "c1",
      undefined,
      undefined,
      (_peer, _err, connection) => lost.push(connection),
    );
    const connection = await cm.get(peer(1));

    transport.dropUnderneath(new Error("ECONNRESET"));

    expect(lost).toEqual([connection]);
  });

  it("hands its message hook the connection each message arrived on, the one get() returned", async () => {
    const network = new InProcessNetwork();
    const arrivedOn: unknown[] = [];
    let toDialler: Connection | undefined;
    await new InProcessTransport(network, "c1").listen(
      peer(1),
      () => undefined,
      (_preamble, accepted) => {
        toDialler = accepted;
      },
    );
    const cm = new ConnectionManager(
      new InProcessTransport(network, "c1"),
      self,
      "c1",
      undefined,
      (_message, _from, connection) => void arrivedOn.push(connection),
    );
    const connection = await cm.get(peer(1));

    toDialler!.send({
      correlationId: 1n,
      direction: "oneWay",
      targetGrain: new GrainId("Probe", "x"),
      interfaceId: 0,
      method: "push",
      body: new Uint8Array(),
    });
    await new Promise((resolve) => setTimeout(resolve, 0));

    expect(arrivedOn).toEqual([connection]);
    expect(arrivedOn[0]).toBe(connection);
  });

  it("dials with this process's epoch in the preamble", async () => {
    const network = new InProcessNetwork();
    const epochs: number[] = [];
    await new InProcessTransport(network, "c1").listen(
      peer(1),
      () => undefined,
      (preamble) => void epochs.push(preamble.epoch),
    );
    const cm = new ConnectionManager(new InProcessTransport(network, "c1"), self, "c1");
    await cm.get(peer(1));
    expect(epochs).toEqual([PROCESS_EPOCH]);
  });
});
