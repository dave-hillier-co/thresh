import { describe, expect, it, vi } from "vitest";
import { clientIdOf, isObserverGrainId } from "@tsva/core/client-grain-id";
import { GrainTaskCanceledError } from "@tsva/core/errors";
import { GrainCancellationToken } from "@tsva/core/grain-cancellation-token";
import { defineGrainInterface } from "@tsva/core/grain-interface";
import { GrainId } from "@tsva/core/grain-id";
import { grainReferenceIdentity } from "@tsva/core/grain-reference";
import type { GrainWithStringKey } from "@tsva/core/key-kinds";
import { SiloAddress } from "@tsva/core/silo-address";
import { InProcessNetwork, InProcessTransport } from "@tsva/messaging/in-process-transport";
import { nextCorrelationId, type Message } from "@tsva/messaging/message";
import { MessagePackSerializer } from "@tsva/messaging/msgpack-serializer";
import { ICancellationSourcesExtension } from "@tsva/runtime/cancellation-extension";
import { waitFor } from "@tsva/testing/wait";
import { createClient } from "@tsva/client/client-node";

interface IObserver extends GrainWithStringKey {
  onEvent(payload: string): Promise<string>;
}
const IObserver = defineGrainInterface<IObserver>("IObserver.client");

const CLUSTER = "c1";
const clientAddr = new SiloAddress("client", "uid-c", "client:33333");
const siloAddr = new SiloAddress("silo", "uid-s", "silo:33334");

/**
 * A minimal stand-in for the calling silo: it can send a request/oneWay
 * message directly to the client and observe whatever reply (if any) comes
 * back, without spinning up a full `ClusterNode`.
 */
async function startFakeSilo(network: InProcessNetwork) {
  const serializer = new MessagePackSerializer();
  let resolveReply: ((message: Message) => void) | undefined;
  const replies: Message[] = [];
  const transport = new InProcessTransport(network, CLUSTER);
  const listener = await transport.listen(siloAddr, (message) => {
    replies.push(message);
    resolveReply?.(message);
  });
  return {
    serializer,
    listener,
    send(message: Message): void {
      network.deliver(clientAddr, message, siloAddr);
    },
    async waitForReply(): Promise<Message> {
      const existing = replies.find((m) => m.direction === "response");
      if (existing !== undefined) return existing;
      return new Promise((resolve) => {
        resolveReply = (message) => {
          if (message.direction === "response") resolve(message);
        };
      });
    },
    replies,
  };
}

function baseMessage(overrides: Partial<Message>): Message {
  return {
    correlationId: nextCorrelationId(),
    direction: "request",
    targetGrain: new GrainId("$client", "unset"),
    sendingSilo: siloAddr,
    interfaceId: IObserver.id,
    method: "onEvent",
    body: new Uint8Array(),
    ...overrides,
  };
}

describe("client object hosting (createObjectReference)", () => {
  it("mints an observer reference owned by the client", async () => {
    const network = new InProcessNetwork();
    const clientId = new GrainId("$client", "c-1");
    const client = createClient({
      clusterId: CLUSTER,
      local: clientAddr,
      transport: new InProcessTransport(network, CLUSTER),
      gateway: siloAddr,
      clientId,
    });
    await client.connect();
    try {
      const ref = client.createObjectReference(IObserver, { onEvent: async (p: string) => p });
      const identity = grainReferenceIdentity(ref);
      expect(identity).toBeDefined();
      expect(isObserverGrainId(identity!.grainId)).toBe(true);
      expect(clientIdOf(identity!.grainId).equals(clientId)).toBe(true);
    } finally {
      await client.close();
    }
  });

  it("invokes the hosted object and replies with a success response", async () => {
    const network = new InProcessNetwork();
    const clientId = new GrainId("$client", "c-2");
    const client = createClient({
      clusterId: CLUSTER,
      local: clientAddr,
      transport: new InProcessTransport(network, CLUSTER),
      gateway: siloAddr,
      clientId,
    });
    await client.connect();
    const silo = await startFakeSilo(network);
    try {
      const handler = { onEvent: vi.fn(async (payload: string) => `got:${payload}`) };
      const ref = client.createObjectReference(IObserver, handler);
      const identity = grainReferenceIdentity(ref)!;

      silo.send(
        baseMessage({
          targetGrain: identity.grainId,
          body: silo.serializer.serialize(["hello"]),
        }),
      );

      const reply = await silo.waitForReply();
      expect(reply.responseKind).toBe("success");
      expect(silo.serializer.deserialize(reply.body)).toBe("got:hello");
      expect(handler.onEvent).toHaveBeenCalledWith("hello");
    } finally {
      await client.close();
      await silo.listener.close();
    }
  });

  it("replies with an error response when the hosted method throws", async () => {
    const network = new InProcessNetwork();
    const clientId = new GrainId("$client", "c-3");
    const client = createClient({
      clusterId: CLUSTER,
      local: clientAddr,
      transport: new InProcessTransport(network, CLUSTER),
      gateway: siloAddr,
      clientId,
    });
    await client.connect();
    const silo = await startFakeSilo(network);
    try {
      const handler = {
        onEvent: async () => {
          throw new Error("boom");
        },
      };
      const ref = client.createObjectReference(IObserver, handler);
      const identity = grainReferenceIdentity(ref)!;

      silo.send(
        baseMessage({
          targetGrain: identity.grainId,
          body: silo.serializer.serialize(["x"]),
        }),
      );

      const reply = await silo.waitForReply();
      expect(reply.responseKind).toBe("error");
      const payload = silo.serializer.deserialize<{ message: string }>(reply.body);
      expect(payload.message).toBe("boom");
    } finally {
      await client.close();
      await silo.listener.close();
    }
  });

  it("invokes the hosted object for a oneWay message and sends no reply", async () => {
    const network = new InProcessNetwork();
    const clientId = new GrainId("$client", "c-4");
    const client = createClient({
      clusterId: CLUSTER,
      local: clientAddr,
      transport: new InProcessTransport(network, CLUSTER),
      gateway: siloAddr,
      clientId,
    });
    await client.connect();
    const silo = await startFakeSilo(network);
    try {
      // Resolve exactly when the hosted method runs, so the assertion waits on
      // a real signal rather than a guessed number of microtask ticks.
      let invoked!: () => void;
      const invokedOnce = new Promise<void>((resolve) => {
        invoked = resolve;
      });
      const handler = {
        onEvent: vi.fn(async (payload: string) => {
          invoked();
          return payload;
        }),
      };
      const ref = client.createObjectReference(IObserver, handler);
      const identity = grainReferenceIdentity(ref)!;

      silo.send(
        baseMessage({
          direction: "oneWay",
          targetGrain: identity.grainId,
          body: silo.serializer.serialize(["fire-and-forget"]),
        }),
      );

      await invokedOnce;
      // A reply (if any were wrongly sent) would arrive at the silo over one
      // more microtask hop; flush once so its absence is a real assertion.
      await Promise.resolve();

      expect(handler.onEvent).toHaveBeenCalledWith("fire-and-forget");
      expect(silo.replies).toHaveLength(0);
    } finally {
      await client.close();
      await silo.listener.close();
    }
  });

  it("stops dispatching to a reference after deleteObjectReference", async () => {
    const network = new InProcessNetwork();
    const clientId = new GrainId("$client", "c-5");
    const client = createClient({
      clusterId: CLUSTER,
      local: clientAddr,
      transport: new InProcessTransport(network, CLUSTER),
      gateway: siloAddr,
      clientId,
    });
    await client.connect();
    const silo = await startFakeSilo(network);
    try {
      const handler = { onEvent: vi.fn(async (payload: string) => payload) };
      const ref = client.createObjectReference(IObserver, handler);
      const identity = grainReferenceIdentity(ref)!;

      client.deleteObjectReference(ref);

      silo.send(
        baseMessage({
          targetGrain: identity.grainId,
          body: silo.serializer.serialize(["late"]),
        }),
      );

      const reply = await silo.waitForReply();
      expect(reply.responseKind).toBe("rejection");
      expect(handler.onEvent).not.toHaveBeenCalled();
    } finally {
      await client.close();
      await silo.listener.close();
    }
  });
});

describe("client-side cancellation binding for hosted objects", () => {
  /** Awaits `delayMs`, honouring `token` — same shape as the runtime-side helper. */
  function cancellableDelay(token: GrainCancellationToken, delayMs: number): Promise<void> {
    return new Promise<void>((resolve, reject) => {
      if (token.isCancellationRequested) {
        reject(new GrainTaskCanceledError());
        return;
      }
      const timer = setTimeout(resolve, delayMs);
      token.signal.addEventListener(
        "abort",
        () => {
          clearTimeout(timer);
          reject(new GrainTaskCanceledError());
        },
        { once: true },
      );
    });
  }

  it("aborts a hosted object's in-flight call when a matching cancelRemoteToken request arrives", async () => {
    const network = new InProcessNetwork();
    const clientId = new GrainId("$client", "c-cancel");
    const client = createClient({
      clusterId: CLUSTER,
      local: clientAddr,
      transport: new InProcessTransport(network, CLUSTER),
      gateway: siloAddr,
      clientId,
    });
    await client.connect();
    const silo = await startFakeSilo(network);
    try {
      const started: string[] = [];
      const cancelled: string[] = [];
      const handler = {
        longWait: async (delayMs: number, callId: string, token: GrainCancellationToken) => {
          started.push(callId);
          try {
            await cancellableDelay(token, delayMs);
          } catch (err) {
            cancelled.push(callId);
            throw err;
          }
        },
      };
      const ref = client.createObjectReference(IObserver, handler as never);
      const identity = grainReferenceIdentity(ref)!;

      const tokenId = "token-1";
      const callId = "call-1";
      // The wire-arrived token, exactly as a caller's `GrainCancellationToken`
      // would encode: never fired yet.
      const wireToken = new GrainCancellationToken({
        tokenId,
        signal: new AbortController().signal,
      });

      const longWaitCorrelation = nextCorrelationId();
      silo.send(
        baseMessage({
          correlationId: longWaitCorrelation,
          targetGrain: identity.grainId,
          method: "longWait",
          body: silo.serializer.serialize([10_000, callId, wireToken]),
        }),
      );

      // Let the hosted call actually start before cancelling.
      await waitFor(() => started.includes(callId));

      // A `cancelRemoteToken(tokenId)` request targeting the SAME observer id
      // (mirroring how the silo's dispatcher addresses the extension call).
      const cancelCorrelation = nextCorrelationId();
      silo.send(
        baseMessage({
          correlationId: cancelCorrelation,
          targetGrain: identity.grainId,
          interfaceId: ICancellationSourcesExtension.id,
          method: "cancelRemoteToken",
          body: silo.serializer.serialize([tokenId]),
        }),
      );

      await waitFor(() => cancelled.includes(callId));

      const responses = new Map(
        silo.replies
          .filter((m): m is Message & { direction: "response" } => m.direction === "response")
          .map((m) => [m.correlationId, m]),
      );
      const longWaitReply = responses.get(longWaitCorrelation);
      const cancelReply = responses.get(cancelCorrelation);
      expect(cancelReply?.responseKind).toBe("success");
      expect(longWaitReply?.responseKind).toBe("error");
    } finally {
      await client.close();
      await silo.listener.close();
    }
  });
});
