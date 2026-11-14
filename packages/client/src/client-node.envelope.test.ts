import { describe, expect, it } from "vitest";
import { grain } from "@thresh/core/decorators";
import { Grain } from "@thresh/core/grain";
import { defineGrainInterface } from "@thresh/core/grain-interface";
import type { GrainKey } from "@thresh/core/key-kinds";
import { SiloAddress } from "@thresh/core/silo-address";
import { InProcessNetwork, InProcessTransport } from "@thresh/messaging/in-process-transport";
import { responseTo, type Message } from "@thresh/messaging/message";
import { MessagePackSerializer } from "@thresh/messaging/msgpack-serializer";
import type { Connection } from "@thresh/messaging/transport";
import { createClient } from "@thresh/client/client-node";

interface IVersioned extends GrainKey<string> {
  ping(): Promise<string>;
}
const IVersioned = defineGrainInterface<IVersioned>("IVersioned.client-envelope", { version: 4 });

@grain()
class VersionedGrain extends Grain implements IVersioned {
  async ping(): Promise<string> {
    return "pong";
  }
}

const CLUSTER = "c1";
const gatewayAddr = new SiloAddress("gateway", "uid-g", "gateway:1");

describe("client request envelope", () => {
  it("carries the caller's interface version, as every other sender's envelope does", async () => {
    const network = new InProcessNetwork();
    const serializer = new MessagePackSerializer();
    const received: Message[] = [];
    // A gateway at the network boundary: it records what arrives and answers it down the
    // connection the client dialled.
    let toClient: Connection | undefined;
    await new InProcessTransport(network, CLUSTER).listen(
      gatewayAddr,
      (message) => {
        received.push(message);
        toClient?.send(responseTo(message, "success", serializer.serialize("pong"), gatewayAddr));
      },
      (_preamble, connection) => {
        toClient = connection;
      },
    );
    const client = createClient({
      clusterId: CLUSTER,
      transport: new InProcessTransport(network, CLUSTER),
      gateway: gatewayAddr,
    }).registerGrain(VersionedGrain, { interfaces: [IVersioned] });
    await client.connect();
    try {
      await client.getGrain(IVersioned, "k").ping();
    } finally {
      await client.close();
    }
    expect(received).toHaveLength(1);
    expect(received[0]).toMatchObject({
      direction: "request",
      interfaceId: IVersioned.id,
      interfaceVersion: 4,
      method: "ping",
    });
    expect(received[0]!.timeToLiveMs).toBeGreaterThan(0);
  });
});
