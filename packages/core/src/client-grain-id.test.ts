import { describe, expect, it } from "vitest";
import {
  CLIENT_GRAIN_TYPE,
  clientId,
  clientIdOf,
  createClientId,
  createObserverId,
  isClient,
  isClientGrainType,
  isObserverGrainId,
  observerId,
  scopeOf,
} from "@tsva/core/client-grain-id";
import { GrainId } from "@tsva/core/grain-id";

describe("isClientGrainType", () => {
  it("is true for the reserved client grain type", () => {
    expect(isClientGrainType(CLIENT_GRAIN_TYPE)).toBe(true);
  });

  it("is false for an ordinary grain type", () => {
    expect(isClientGrainType("Counter")).toBe(false);
  });
});

describe("createClientId / clientId", () => {
  it("produces a client-typed grain id", () => {
    const client = createClientId();
    expect(isClient(client)).toBe(true);
    expect(client.type).toBe(CLIENT_GRAIN_TYPE);
    expect(typeof client.key).toBe("string");
  });

  it("is not an observer id", () => {
    expect(isObserverGrainId(createClientId())).toBe(false);
  });

  it("generates distinct keys across calls", () => {
    const a = createClientId();
    const b = createClientId();
    expect(a.equals(b)).toBe(false);
  });

  it("clientId is deterministic for a given key", () => {
    const a = clientId("abc-123");
    const b = clientId("abc-123");
    expect(a.equals(b)).toBe(true);
    expect(a.toString()).toBe(`${CLIENT_GRAIN_TYPE}/abc-123`);
  });

  it("is false for a normal grain id", () => {
    expect(isClient(new GrainId("Counter", "x"))).toBe(false);
  });
});

describe("createObserverId / observerId", () => {
  it("produces an observer id scoped to the client", () => {
    const client = createClientId();
    const observer = createObserverId(client);
    expect(isClient(observer)).toBe(true);
    expect(isObserverGrainId(observer)).toBe(true);
  });

  it("recovers the original client id via clientIdOf", () => {
    const client = createClientId();
    const observer = createObserverId(client);
    expect(clientIdOf(observer).equals(client)).toBe(true);
  });

  it("two observers off one client differ but share the same clientIdOf", () => {
    const client = createClientId();
    const obs1 = createObserverId(client);
    const obs2 = createObserverId(client);
    expect(obs1.equals(obs2)).toBe(false);
    expect(clientIdOf(obs1).equals(client)).toBe(true);
    expect(clientIdOf(obs2).equals(client)).toBe(true);
  });

  it("observerId is deterministic for a given scope", () => {
    const client = clientId("client-a");
    const a = observerId(client, "scope-1");
    const b = observerId(client, "scope-1");
    expect(a.equals(b)).toBe(true);
    expect(a.toString()).toBe(`${CLIENT_GRAIN_TYPE}/client-a+scope-1`);
  });

  it("scopeOf returns the segment after the separator", () => {
    const client = clientId("client-a");
    const observer = observerId(client, "scope-1");
    expect(scopeOf(observer)).toBe("scope-1");
  });

  it("throws when creating an observer for a non-client grain id", () => {
    expect(() => createObserverId(new GrainId("Counter", "x"))).toThrow();
    expect(() => observerId(new GrainId("Counter", "x"), "scope")).toThrow();
  });
});

describe("clientIdOf", () => {
  it("is the identity for a plain client id (no observer suffix)", () => {
    const client = createClientId();
    expect(clientIdOf(client).equals(client)).toBe(true);
  });

  it("throws for a non-client grain id", () => {
    expect(() => clientIdOf(new GrainId("Counter", "x"))).toThrow();
  });
});

describe("hashing", () => {
  it("distinct observers hash distinctly", () => {
    const client = createClientId();
    const obs1 = createObserverId(client);
    const obs2 = createObserverId(client);
    expect(obs1.getUniformHashCode()).not.toBe(obs2.getUniformHashCode());
  });
});
