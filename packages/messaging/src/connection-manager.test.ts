import { describe, expect, it } from "vitest";
import { SiloAddress } from "@thresh/core/silo-address";
import { ConnectionManager } from "@thresh/messaging/connection-manager";
import type { Connection, Listener, MessageHandler, Transport } from "@thresh/messaging/transport";

const self = new SiloAddress("self", "u0", "self:1");
const peer = (n: number) => new SiloAddress(`peer-${n}`, `u${n}`, `peer-${n}:1`);

class CountingTransport implements Transport {
  connects = 0;
  closes = 0;
  failNext = false;

  async listen(_address: SiloAddress, _onMessage: MessageHandler): Promise<Listener> {
    throw new Error("not used");
  }

  async connect(): Promise<Connection> {
    this.connects++;
    if (this.failNext) {
      this.failNext = false;
      throw new Error("connect failed");
    }
    return { send: () => undefined, close: async () => void this.closes++ };
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
});
