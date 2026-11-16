import { RejectionError } from "@thresh/core/errors";
import type { SiloAddress } from "@thresh/core/silo-address";
import { systemTimeProvider, type TimeProvider } from "@thresh/core/time-provider";
import { recordMessageReceived } from "@thresh/observability/messaging-metrics";
import type { Message } from "@thresh/messaging/message";
import {
  CONNECT_TIMEOUT_MS,
  PROTOCOL_VERSION,
  boundDial,
  connectFailure,
  type Connection,
  type ConnectionAcceptHandler,
  type ConnectionPreamble,
  type Listener,
  type MessageHandler,
  type Transport,
} from "@thresh/messaging/transport";

interface Endpoint {
  onMessage: MessageHandler;
  clusterId: string;
  address: SiloAddress;
  onAccept?: ConnectionAcceptHandler;
  /** Fired once when the endpoint is unregistered, however that happens. */
  onUnregistered?: () => void;
}

/**
 * Shared fabric that in-process transports route through, standing in for the
 * real network in fast multi-silo tests. Delivery is asynchronous to mimic it.
 */
export class InProcessNetwork {
  private readonly endpoints = new Map<string, Endpoint>();
  private readonly blackholes = new Set<string>();

  /**
   * Fault injection: dials to `address` are accepted but the handshake never
   * completes, as with a host that drops packets. The dial only ends when the
   * transport's connect timeout fires.
   */
  blackhole(address: SiloAddress): void {
    this.blackholes.add(address.endpoint);
  }

  isBlackholed(address: SiloAddress): boolean {
    return this.blackholes.has(address.endpoint);
  }

  register(endpoint: Endpoint): void {
    this.endpoints.set(endpoint.address.endpoint, endpoint);
  }

  /** Remove the endpoint at `address`: its listener is gone, whether it closed or the fabric dropped it. */
  unregister(address: SiloAddress): void {
    const endpoint = this.endpoints.get(address.endpoint);
    this.endpoints.delete(address.endpoint);
    endpoint?.onUnregistered?.();
  }

  lookup(address: SiloAddress): Endpoint | undefined {
    return this.endpoints.get(address.endpoint);
  }

  /** Deliver `message` to the listener at `to`, as having arrived on the accepted `connection`. */
  deliver(to: SiloAddress, message: Message, from: SiloAddress, connection?: Connection): void {
    const endpoint = this.endpoints.get(to.endpoint);
    if (endpoint === undefined) {
      throw new RejectionError(`no listener at ${to.toString()}`, "unknownTarget");
    }
    queueMicrotask(() => void endpoint.onMessage(message, from, connection));
  }
}

export class InProcessTransport implements Transport {
  constructor(
    private readonly network: InProcessNetwork,
    private readonly clusterId: string,
    private readonly options: {
      readonly timeProvider?: TimeProvider;
      readonly connectTimeoutMs?: number;
    } = {},
  ) {}

  async listen(
    address: SiloAddress,
    onMessage: MessageHandler,
    onAccept?: ConnectionAcceptHandler,
  ): Promise<Listener> {
    const closed: Array<() => void> = [];
    this.network.register({
      onUnregistered: () => {
        for (const callback of closed.splice(0)) callback();
      },
      onMessage: (message, from, connection) => {
        recordMessageReceived({
          "thresh.peer": from.endpoint,
          "thresh.message.direction": message.direction,
        });
        return onMessage(message, from, connection);
      },
      clusterId: this.clusterId,
      address,
      ...(onAccept && { onAccept }),
    });
    return {
      address,
      onClose: (callback) => {
        closed.push(callback);
      },
      close: async () => this.network.unregister(address),
    };
  }

  /**
   * Dial `to`. The two ends of the connection are a pair: the dialled end delivers to the
   * listener's handler as having arrived on the accepted end, and the accepted end (handed to
   * the listener's `onAccept` hook and to its handler with every message) delivers straight to
   * THIS dialler's own `onMessage` hook (via `queueMicrotask`, preserving the async-delivery
   * fiction the rest of the network uses) — never to a network address, since a dialler need
   * not (and, for a client, does not) register one. A dialler that passed no `onMessage` cannot
   * receive anything back, so the accepted end's `send` throws rather than silently dropping.
   * Closing either end reports the loss to the other end's `onClose`, and both ends refuse
   * every send from then on, as a closed socket does.
   */
  connect(
    to: SiloAddress,
    preamble: ConnectionPreamble,
    onMessage?: MessageHandler,
  ): Promise<Connection> {
    const dial = this.network.isBlackholed(to)
      ? new Promise<Connection>(() => undefined)
      : this.dial(to, preamble, onMessage);
    return boundDial(
      this.options.timeProvider ?? systemTimeProvider,
      this.options.connectTimeoutMs ?? CONNECT_TIMEOUT_MS,
      to,
      dial,
    );
  }

  private async dial(
    to: SiloAddress,
    preamble: ConnectionPreamble,
    onMessage?: MessageHandler,
  ): Promise<Connection> {
    const endpoint = this.network.lookup(to);
    if (endpoint === undefined) throw connectFailure(to, "no listener", "refused");
    if (endpoint.clusterId !== preamble.clusterId) {
      throw connectFailure(to, `cluster mismatch: ${preamble.clusterId} != ${endpoint.clusterId}`);
    }
    if (preamble.protocolVersion !== PROTOCOL_VERSION) {
      throw connectFailure(
        to,
        `protocol version mismatch: ${preamble.protocolVersion} != ${PROTOCOL_VERSION}`,
      );
    }
    const network = this.network;
    const from = preamble.siloAddress;
    let closed = false;
    let notifyDiallerClosed: ((err?: unknown) => void) | undefined;
    let notifyAcceptedClosed: ((err?: unknown) => void) | undefined;
    const refuseIfClosed = (): void => {
      if (closed) {
        throw new RejectionError(`connection to ${to.toString()} is closed`, "unknownTarget");
      }
    };
    const dialled: Connection = {
      send: (message) => {
        refuseIfClosed();
        network.deliver(to, message, from, accepted);
      },
      close: async () => {
        if (closed) return;
        closed = true;
        notifyAcceptedClosed?.();
      },
      onClose: (callback) => {
        notifyDiallerClosed = callback;
      },
    };
    const accepted: Connection = {
      send: (message) => {
        refuseIfClosed();
        if (onMessage === undefined) {
          throw new RejectionError("peer connection is not duplex", "unknownTarget");
        }
        recordMessageReceived({
          "thresh.peer": to.endpoint,
          "thresh.message.direction": message.direction,
        });
        queueMicrotask(() => void onMessage(message, to, dialled));
      },
      close: async () => {
        if (closed) return;
        closed = true;
        notifyDiallerClosed?.();
      },
      onClose: (callback) => {
        notifyAcceptedClosed = callback;
      },
    };
    if (endpoint.onAccept !== undefined) {
      try {
        endpoint.onAccept(preamble, accepted);
      } catch (err) {
        throw connectFailure(to, `listener refused the connection (${String(err)})`);
      }
    }
    return dialled;
  }
}
