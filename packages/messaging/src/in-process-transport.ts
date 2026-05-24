import { RejectionError } from "@tsva/core/errors";
import type { SiloAddress } from "@tsva/core/silo-address";
import type { Message } from "@tsva/messaging/message";
import type {
  Connection,
  ConnectionPreamble,
  Listener,
  MessageHandler,
  Transport,
} from "@tsva/messaging/transport";

interface Endpoint {
  onMessage: MessageHandler;
  clusterId: string;
  address: SiloAddress;
}

/**
 * Shared fabric that in-process transports route through, standing in for the
 * real network in fast multi-silo tests. Delivery is asynchronous to mimic it.
 */
export class InProcessNetwork {
  private readonly endpoints = new Map<string, Endpoint>();

  register(endpoint: Endpoint): void {
    this.endpoints.set(endpoint.address.endpoint, endpoint);
  }

  unregister(address: SiloAddress): void {
    this.endpoints.delete(address.endpoint);
  }

  lookup(address: SiloAddress): Endpoint | undefined {
    return this.endpoints.get(address.endpoint);
  }

  deliver(to: SiloAddress, message: Message, from: SiloAddress): void {
    const endpoint = this.endpoints.get(to.endpoint);
    if (endpoint === undefined) {
      throw new RejectionError(`no listener at ${to.toString()}`, "unknownTarget");
    }
    queueMicrotask(() => void endpoint.onMessage(message, from));
  }
}

export class InProcessTransport implements Transport {
  constructor(
    private readonly network: InProcessNetwork,
    private readonly clusterId: string,
  ) {}

  async listen(address: SiloAddress, onMessage: MessageHandler): Promise<Listener> {
    this.network.register({ onMessage, clusterId: this.clusterId, address });
    return {
      address,
      close: async () => this.network.unregister(address),
    };
  }

  async connect(to: SiloAddress, preamble: ConnectionPreamble): Promise<Connection> {
    const endpoint = this.network.lookup(to);
    if (endpoint === undefined) {
      throw new RejectionError(`no listener at ${to.toString()}`, "unknownTarget");
    }
    if (endpoint.clusterId !== preamble.clusterId) {
      throw new RejectionError(
        `cluster mismatch: ${preamble.clusterId} != ${endpoint.clusterId}`,
        "unknownTarget",
      );
    }
    const network = this.network;
    const from = preamble.siloAddress;
    return {
      send: (message) => network.deliver(to, message, from),
      close: async () => undefined,
    };
  }
}
