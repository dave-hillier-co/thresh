import { describe, expect, it } from "vitest";
import { grain } from "@thresh/core/decorators";
import { GrainCallTimeoutError } from "@thresh/core/errors";
import { Grain } from "@thresh/core/grain";
import { defineGrainInterface } from "@thresh/core/grain-interface";
import type { GrainKey } from "@thresh/core/key-kinds";
import { SiloAddress } from "@thresh/core/silo-address";
import { InProcessNetwork, InProcessTransport } from "@thresh/messaging/in-process-transport";
import { responseTo, type Message } from "@thresh/messaging/message";
import { MessagePackSerializer } from "@thresh/messaging/msgpack-serializer";
import type { Connection } from "@thresh/messaging/transport";
import { createClient } from "@thresh/client/client-node";
import { staticGatewayProvider } from "@thresh/client/gateway-provider";

/**
 * W6: the client resends a call to another gateway only when the call did not run — the gateway
 * refused it before execution, or it never left. A reply that timed out may have run.
 */
interface IPing extends GrainKey<string> {
  ping(): Promise<string>;
}
const IPing = defineGrainInterface<IPing>("IPing.client-resend");

@grain()
class PingGrain extends Grain implements IPing {
  async ping(): Promise<string> {
    return "pong";
  }
}

const CLUSTER = "c1";
const serializer = new MessagePackSerializer();
const gateways = [new SiloAddress("gw-1", "u1", "gw-1:1"), new SiloAddress("gw-2", "u2", "gw-2:1")];

type Answer = (request: Message, connection: Connection) => void;

/** Two gateways at the network boundary; each request any of them receives is passed to `answer`. */
async function fakeGateways(network: InProcessNetwork, answer: Answer): Promise<Message[]> {
  const received: Message[] = [];
  for (const address of gateways) {
    await new InProcessTransport(network, CLUSTER).listen(address, (message, _from, via) => {
      if (message.direction !== "request") return;
      received.push(message);
      answer(message, via!);
    });
  }
  return received;
}

describe("client resend", () => {
  it("does not resend a call whose reply timed out to another gateway: it may have run", async () => {
    const network = new InProcessNetwork();
    const received = await fakeGateways(network, () => undefined); // never answers
    const client = createClient({
      clusterId: CLUSTER,
      transport: new InProcessTransport(network, CLUSTER),
      gateways: staticGatewayProvider(gateways),
      callTimeoutMs: 50,
      // The client's own budget never runs out, so only the reply timeout ends the attempt.
      now: () => 0,
      delay: async () => undefined,
    }).registerGrain(PingGrain, { interfaces: [IPing] });
    await client.connect();
    try {
      const outcome = await Promise.race([
        client
          .getGrain(IPing, "k")
          .ping()
          .catch((e: unknown) => e),
        new Promise((resolve) => setTimeout(() => resolve("still resending"), 500)),
      ]);
      expect(outcome).toBeInstanceOf(GrainCallTimeoutError);
      expect(received).toHaveLength(1);
    } finally {
      await client.close();
    }
  });

  it("resends a call a gateway refused before execution, and it runs once", async () => {
    const network = new InProcessNetwork();
    let answered = 0;
    const received = await fakeGateways(network, (request, via) => {
      answered += 1;
      if (answered === 1) {
        via.send(
          responseTo(
            request,
            "rejection",
            serializer.serialize({ message: "activation moved", kind: "noActivation" }),
          ),
        );
        return;
      }
      via.send(responseTo(request, "success", serializer.serialize("pong")));
    });
    const client = createClient({
      clusterId: CLUSTER,
      transport: new InProcessTransport(network, CLUSTER),
      gateways: staticGatewayProvider(gateways),
      callTimeoutMs: 2_000,
      delay: async () => undefined,
    }).registerGrain(PingGrain, { interfaces: [IPing] });
    await client.connect();
    try {
      await expect(client.getGrain(IPing, "k").ping()).resolves.toBe("pong");
      expect(received).toHaveLength(2);
    } finally {
      await client.close();
    }
  });
});
