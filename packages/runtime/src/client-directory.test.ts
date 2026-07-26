import { describe, expect, it } from "vitest";
import { clientId } from "@thresh/core/client-grain-id";
import { SiloAddress } from "@thresh/core/silo-address";
import { ClientDirectory } from "@thresh/runtime/client-directory";

const silo = (n: number) => new SiloAddress(`silo-${n}`, `uid-${n}`, `silo-${n}:1111`);

describe("ClientDirectory", () => {
  it("returns undefined for a client with no registered gateway", () => {
    const dir = new ClientDirectory();
    expect(dir.lookup(clientId("c1"))).toBeUndefined();
  });

  it("looks up the gateway a client registered through", () => {
    const dir = new ClientDirectory();
    const client = clientId("c1");
    dir.register(client, silo(0));
    expect(dir.lookup(client)).toEqual(silo(0));
  });

  it("tracks multiple gateways for the same client and returns a deterministic first entry", () => {
    const dir = new ClientDirectory();
    const client = clientId("c1");
    dir.register(client, silo(0));
    dir.register(client, silo(1));
    expect(dir.lookup(client)).toEqual(silo(0)); // first by insertion order
  });

  it("prefers the given silo when it is among the client's gateways", () => {
    const dir = new ClientDirectory();
    const client = clientId("c1");
    dir.register(client, silo(0));
    dir.register(client, silo(1));
    expect(dir.lookup(client, silo(1))).toEqual(silo(1));
  });

  it("falls back to a deterministic entry when the preferred silo isn't registered", () => {
    const dir = new ClientDirectory();
    const client = clientId("c1");
    dir.register(client, silo(0));
    expect(dir.lookup(client, silo(9))).toEqual(silo(0));
  });

  it("unregisters a single gateway for a client, leaving others intact", () => {
    const dir = new ClientDirectory();
    const client = clientId("c1");
    dir.register(client, silo(0));
    dir.register(client, silo(1));
    dir.unregister(client, silo(0));
    expect(dir.lookup(client)).toEqual(silo(1));
  });

  it("drops the client entirely once its last gateway is unregistered", () => {
    const dir = new ClientDirectory();
    const client = clientId("c1");
    dir.register(client, silo(0));
    dir.unregister(client, silo(0));
    expect(dir.lookup(client)).toBeUndefined();
  });

  it("unregistering a gateway that was never registered is a no-op", () => {
    const dir = new ClientDirectory();
    const client = clientId("c1");
    dir.register(client, silo(0));
    dir.unregister(client, silo(9));
    expect(dir.lookup(client)).toEqual(silo(0));
  });

  it("unregisterSilo drops that gateway from every client's entry", () => {
    const dir = new ClientDirectory();
    const c1 = clientId("c1");
    const c2 = clientId("c2");
    dir.register(c1, silo(0));
    dir.register(c1, silo(1));
    dir.register(c2, silo(0));
    dir.unregisterSilo(silo(0));
    expect(dir.lookup(c1)).toEqual(silo(1));
    expect(dir.lookup(c2)).toBeUndefined();
  });

  it("distinguishes clients by id, not sharing entries across different clients", () => {
    const dir = new ClientDirectory();
    const c1 = clientId("c1");
    const c2 = clientId("c2");
    dir.register(c1, silo(0));
    expect(dir.lookup(c2)).toBeUndefined();
    expect(dir.lookup(c1)).toEqual(silo(0));
  });
});
