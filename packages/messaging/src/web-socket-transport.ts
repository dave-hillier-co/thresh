import { once } from "node:events";
import { WebSocket, WebSocketServer } from "ws";
import { defaultLogger, type Logger } from "@thresh/core/logger";
import { SiloAddress } from "@thresh/core/silo-address";
import { systemTimeProvider, type TimeProvider } from "@thresh/core/time-provider";
import {
  recordDecodeFailure,
  recordHandlerFailure,
  recordMessageReceived,
} from "@thresh/observability/messaging-metrics";
import type { Message } from "@thresh/messaging/message";
import { MessagePackSerializer } from "@thresh/messaging/msgpack-serializer";
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

const ACK = Uint8Array.of(1);

export interface WebSocketTransportOptions {
  /** Receives decode and handler failures. Defaults to discarding them. */
  readonly logger?: Logger;
  /** Drives the dial timeout. Defaults to the system clock. */
  readonly timeProvider?: TimeProvider;
  /** Overrides the dial timeout (default `CONNECT_TIMEOUT_MS`, Orleans' OpenConnectionTimeout). */
  readonly connectTimeoutMs?: number;
}

function splitHostPort(endpoint: string): { host: string; port: number } {
  const i = endpoint.lastIndexOf(":");
  return { host: endpoint.slice(0, i), port: Number(endpoint.slice(i + 1)) };
}

function toBytes(data: ArrayBuffer): Uint8Array {
  return new Uint8Array(data);
}

/**
 * WebSocket-backed transport. A socket is duplex from the moment it is
 * accepted: the `Connection` a listener's `onAccept` hook receives writes
 * back down that SAME accepted socket, and a dialler that supplies an
 * `onMessage` hook to `connect` receives every frame the peer sends back down
 * the socket it dialled after the preamble ack. There is no reverse dial —
 * the connection the two ends already share IS the route back. On connect,
 * the peer's identity, cluster id and protocol version are exchanged in a
 * preamble; a mismatched cluster or an old protocol version closes the
 * socket so `connect` rejects.
 */
export class WebSocketTransport implements Transport {
  private readonly serializer = new MessagePackSerializer();

  constructor(
    private readonly clusterId: string,
    private readonly options: WebSocketTransportOptions = {},
  ) {}

  private get logger(): Logger {
    return this.options.logger ?? defaultLogger;
  }

  /**
   * A frame that cannot be decoded is a fault of that one connection, never of
   * the process: count it, log it and close the socket, leaving every other
   * connection untouched.
   */
  private rejectFrame(socket: WebSocket, peer: string, what: string, err: unknown): void {
    recordDecodeFailure({ "thresh.peer": peer, "thresh.frame": what });
    this.logger.warn(`closing connection to ${peer}: undecodable ${what} frame`, {
      peer,
      error: String(err),
    });
    socket.terminate();
  }

  /** Runs a message handler so that a throw or a rejection can never escape as a process fault. */
  private dispatch(onMessage: MessageHandler, message: Message, from: SiloAddress): void {
    const failed = (err: unknown) => {
      recordHandlerFailure({ "thresh.peer": from.endpoint });
      this.logger.error(`message handler failed for a frame from ${from.endpoint}`, {
        error: String(err),
      });
    };
    try {
      void Promise.resolve(onMessage(message, from)).catch(failed);
    } catch (err) {
      failed(err);
    }
  }

  private decodeMessage(bytes: Uint8Array): Message {
    const message = this.serializer.deserialize<Message | null>(bytes);
    if (typeof message !== "object" || message === null) throw new Error("frame is not a message");
    return message;
  }

  async listen(
    address: SiloAddress,
    onMessage: MessageHandler,
    onAccept?: ConnectionAcceptHandler,
  ): Promise<Listener> {
    const { host, port } = splitHostPort(address.endpoint);
    const server = new WebSocketServer({ host, port });
    server.on("error", () => {
      // ws requires an 'error' listener on the server itself or an
      // unusual server-level fault (e.g. the underlying http server) would
      // throw and crash the whole silo.
    });
    await once(server, "listening");
    const info = server.address();
    const boundPort = typeof info === "object" && info !== null ? info.port : port;
    const boundAddress = new SiloAddress(address.podName, address.podUid, `${host}:${boundPort}`);

    server.on("connection", (socket) => {
      socket.binaryType = "arraybuffer";
      let from: SiloAddress | undefined;
      let notifyClosed: ((err?: unknown) => void) | undefined;
      socket.on("error", () => {
        // Swallow: ws follows an established connection's 'error' (ECONNRESET,
        // a malformed frame, ...) with a 'close', where teardown happens below.
        // Without this listener Node's EventEmitter throws and crashes the silo.
      });
      socket.once("close", () => notifyClosed?.());
      socket.on("message", (data: ArrayBuffer) => {
        const bytes = toBytes(data);
        if (from === undefined) {
          const preamble = this.tryPreamble(bytes);
          if (preamble === undefined) {
            this.rejectFrame(socket, "unidentified peer", "preamble", "not a valid preamble");
            return;
          }
          if (
            preamble.clusterId !== this.clusterId ||
            preamble.protocolVersion !== PROTOCOL_VERSION
          ) {
            socket.close(4001, "cluster mismatch");
            return;
          }
          from = preamble.siloAddress;
          socket.send(ACK);
          if (onAccept !== undefined) {
            // Writes back down THIS accepted socket — the connection the peer
            // dialled is its own reachability, so there is nothing to dial.
            const connection: Connection = {
              send: (message) => socket.send(this.serializer.serialize(message)),
              close: () =>
                new Promise<void>((resolve) => {
                  if (socket.readyState === WebSocket.CLOSED) {
                    resolve();
                    return;
                  }
                  socket.once("close", () => resolve());
                  socket.close();
                }),
              onClose: (callback) => {
                notifyClosed = callback;
              },
            };
            onAccept(preamble, connection);
          }
          return;
        }
        let message: Message;
        try {
          message = this.decodeMessage(bytes);
        } catch (err) {
          this.rejectFrame(socket, from.endpoint, "message", err);
          return;
        }
        recordMessageReceived({
          "thresh.peer": from.endpoint,
          "thresh.message.direction": message.direction,
        });
        this.dispatch(onMessage, message, from);
      });
    });

    return {
      address: boundAddress,
      onClose: (callback) => {
        server.once("close", callback);
      },
      close: async () => {
        await new Promise<void>((resolve) => {
          for (const client of server.clients) client.terminate(); // else close() waits forever
          server.close(() => resolve());
        });
      },
    };
  }

  /**
   * Dial `to`. A single persistent `message` handler is installed BEFORE the
   * preamble is sent, so no frame can arrive with nothing listening for it:
   * the first frame received is always the preamble ack (resolving `connect`'s
   * promise), and every frame after that is a push from the peer, delivered to
   * `onMessage` when the caller supplied one. Using `socket.once("message")`
   * for just the ack — the previous shape — would race a push the peer sends
   * in the same tick as the ack, consuming or losing it.
   */
  connect(
    to: SiloAddress,
    preamble: ConnectionPreamble,
    onMessage?: MessageHandler,
  ): Promise<Connection> {
    const { host, port } = splitHostPort(to.endpoint);
    const socket = new WebSocket(`ws://${host}:${port}`);
    return boundDial(
      this.options.timeProvider ?? systemTimeProvider,
      this.options.connectTimeoutMs ?? CONNECT_TIMEOUT_MS,
      to,
      this.dial(socket, to, preamble, onMessage),
      () => socket.terminate(),
    );
  }

  /**
   * The handshake on an already-created socket. Every way it can fail to
   * reach an acknowledged connection (a refused or reset TCP connect, the
   * listener closing the socket, a rejected preamble) rejects with the one
   * typed `connectFailure`; a raw socket error never escapes to the caller.
   */
  private async dial(
    socket: WebSocket,
    to: SiloAddress,
    preamble: ConnectionPreamble,
    onMessage?: MessageHandler,
  ): Promise<Connection> {
    socket.binaryType = "arraybuffer";
    let ackReceived = false;
    let failHandshake: ((err: unknown) => void) | undefined;
    const handshakeFailed = new Promise<never>((_, reject) => {
      failHandshake = reject;
    });
    handshakeFailed.catch(() => undefined); // observed through the races below
    socket.on("error", (err: Error) => {
      // An 'error' is always followed by 'close', where teardown happens. Before
      // the ack it is also the reason the dial failed (ECONNREFUSED, ...).
      if (!ackReceived)
        failHandshake?.(
          connectFailure(
            to,
            err.message,
            (err as NodeJS.ErrnoException).code === "ECONNREFUSED" ? "refused" : "handshake",
          ),
        );
    });
    let resolveAck: (() => void) | undefined;
    const ack = new Promise<void>((resolve) => {
      resolveAck = resolve;
    });

    // A single persistent `message` handler is installed BEFORE the preamble is
    // sent, so no frame can arrive with nothing listening for it.
    socket.on("message", (data: ArrayBuffer) => {
      if (!ackReceived) {
        ackReceived = true;
        resolveAck?.();
        return;
      }
      if (onMessage === undefined) return; // no hook installed; post-ack frames are dropped
      let message: Message;
      try {
        message = this.decodeMessage(toBytes(data));
      } catch (err) {
        this.rejectFrame(socket, to.endpoint, "message", err);
        return;
      }
      recordMessageReceived({
        "thresh.peer": to.endpoint,
        "thresh.message.direction": message.direction,
      });
      this.dispatch(onMessage, message, to);
    });
    let notifyClosed: ((err?: unknown) => void) | undefined;
    socket.once("close", (code: number) => {
      if (!ackReceived) failHandshake?.(connectFailure(to, `connection rejected (code ${code})`));
      notifyClosed?.();
    });

    const opened = new Promise<void>((resolve) => socket.once("open", () => resolve()));
    try {
      await Promise.race([opened, handshakeFailed]);
      socket.send(this.serializer.serialize(preamble));
      await Promise.race([ack, handshakeFailed]);
    } catch (err) {
      socket.terminate();
      throw err;
    }

    return {
      send: (message) => socket.send(this.serializer.serialize(message)),
      close: () =>
        new Promise<void>((resolve) => {
          // The peer may already have closed this socket (e.g. it shut down
          // first). Once `close` has fired it won't fire again, so waiting on it
          // would hang — resolve immediately when already closed.
          if (socket.readyState === WebSocket.CLOSED) {
            resolve();
            return;
          }
          socket.once("close", () => resolve());
          socket.close();
        }),
      onClose: (callback) => {
        notifyClosed = callback;
      },
    };
  }

  private tryPreamble(bytes: Uint8Array): ConnectionPreamble | undefined {
    try {
      const preamble = this.serializer.deserialize<ConnectionPreamble | null>(bytes);
      return typeof preamble === "object" && preamble !== null ? preamble : undefined;
    } catch {
      return undefined;
    }
  }
}
