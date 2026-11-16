import { describe, expect, it } from "vitest";
import { GrainId } from "@thresh/core/grain-id";
import type { InvocationRequest } from "@thresh/core/request";
import { SiloAddress } from "@thresh/core/silo-address";
import { requestEnvelope } from "@thresh/messaging/request-envelope";

const sender = new SiloAddress("silo-a", "uid-a", "silo-a:1");
const body = Uint8Array.of(1, 2, 3);

function fullRequest(overrides: Partial<InvocationRequest> = {}): InvocationRequest {
  return {
    target: new GrainId("counter", "k"),
    interfaceId: 42,
    interfaceVersion: 3,
    method: "increment",
    args: [1],
    options: {},
    reentrancyId: "chain-1",
    sender: new GrainId("caller", "c"),
    callingGrain: new GrainId("caller", "c"),
    transaction: {
      id: "tx-1",
      timeStamp: 7,
      readOnly: false,
      participants: new Map(),
      pendingCalls: 0,
    },
    headers: { traceparent: "00-abc-def-01" },
    deadline: 1_500,
    forwardCount: 1,
    ...overrides,
  };
}

describe("requestEnvelope", () => {
  it("carries every field a request needs across a hop, whoever sends it", () => {
    const message = requestEnvelope(fullRequest(), {
      correlationId: 9n,
      sendingSilo: sender,
      body,
      now: 1_000,
      timeToLiveMs: 250,
    });

    expect(message).toEqual({
      correlationId: 9n,
      direction: "request",
      targetGrain: new GrainId("counter", "k"),
      sendingSilo: sender,
      sendingGrain: new GrainId("caller", "c"),
      callingGrain: new GrainId("caller", "c"),
      interfaceId: 42,
      interfaceVersion: 3,
      method: "increment",
      deadlineInMs: 500,
      timeToLiveMs: 250,
      forwardCount: 1,
      requestContext: {
        reentrancyId: "chain-1",
        transaction: { id: "tx-1", timeStamp: 7, readOnly: false },
        headers: { traceparent: "00-abc-def-01" },
      },
      body,
    });
  });

  it("gives a one-way request no time-to-live, since nobody waits for its reply", () => {
    const message = requestEnvelope(fullRequest({ options: { oneWay: true } }), {
      correlationId: 1n,
      sendingSilo: sender,
      body,
      now: 1_000,
      timeToLiveMs: 250,
    });
    expect(message.direction).toBe("oneWay");
    expect(message).not.toHaveProperty("timeToLiveMs");
  });

  it("omits the optional fields a request does not carry", () => {
    const message = requestEnvelope(
      { target: new GrainId("counter", "k"), interfaceId: 1, method: "m", args: [], options: {} },
      { correlationId: 1n, sendingSilo: sender, body, now: 0, timeToLiveMs: 10 },
    );
    expect(Object.keys(message).sort()).toEqual(
      [
        "body",
        "correlationId",
        "direction",
        "interfaceId",
        "method",
        "requestContext",
        "sendingSilo",
        "targetGrain",
        "timeToLiveMs",
      ].sort(),
    );
    expect(message.requestContext).toEqual({});
  });
});
