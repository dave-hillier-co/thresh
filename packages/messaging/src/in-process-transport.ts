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
      onMessage: (message, from) => {
        recordMessageReceived({
          "thresh.peer": from.endpoint,
          "thresh.message.direction": message.direction,
        });
        return onMessage(message, from);
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
   * Dial `to`. When the target registered an `onAccept` hook, it gets back a
   * duplex `Connection` whose `send` delivers straight to THIS dialler's own
   * `onMessage` hook (via `queueMicrotask`, preserving the async-delivery
   * fiction the rest of the network uses) — never to a network address, since
   * a dialler need not (and, for a client, does not) register one. A dialler
   * that passed no `onMessage` cannot receive anything back, so `send` throws
   * rather than silently dropping.
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
    // Fired if the dialler's `close()` runs, so the accepted side (the
    // listener's `onAccept` hook) learns the socket dropped — mirrors the real
    // transports, where closing either end of a TCP/WS socket fires the
    // other's `onClose`.
    let notifyAcceptedSideClosed: ((err?: unknown) => void) | undefined;
    if (endpoint.onAccept !== undefined) {
      const duplexConnection: Connection = {
        send: (message) => {
          if (onMessage === undefined) {
            throw new RejectionError("peer connection is not duplex", "unknownTarget");
          }
          recordMessageReceived({
            "thresh.peer": to.endpoint,
            "thresh.message.direction": message.direction,
          });
          queueMicrotask(() => void onMessage(message, to));
        },
        close: async () => undefined,
        onClose: (callback) => {
          notifyAcceptedSideClosed = callback;
        },
      };
      try {
        endpoint.onAccept(preamble, duplexConnection);
      } catch (err) {
        throw connectFailure(to, `listener refused the connection (${String(err)})`);
      }
    }
    return {
      send: (message) => network.deliver(to, message, from),
      close: async () => notifyAcceptedSideClosed?.(),
    };
  }
}
