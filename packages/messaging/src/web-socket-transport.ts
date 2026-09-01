import { once } from "node:events";
import { WebSocket, WebSocketServer } from "ws";
import { RejectionError } from "@thresh/core/errors";
import { SiloAddress } from "@thresh/core/silo-address";
import { recordMessageReceived } from "@thresh/observability/messaging-metrics";
import type { Message } from "@thresh/messaging/message";
import { MessagePackSerializer } from "@thresh/messaging/msgpack-serializer";
import {
  PROTOCOL_VERSION,
  type Connection,
  type ConnectionAcceptHandler,
  type ConnectionPreamble,
  type Listener,
  type MessageHandler,
  type Transport,
} from "@thresh/messaging/transport";

const ACK = Uint8Array.of(1);

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

  constructor(private readonly clusterId: string) {}

  async listen(
    address: SiloAddress,
    onMessage: MessageHandler,
    onAccept?: ConnectionAcceptHandler,
  ): Promise<Listener> {
    const { host, port } = splitHostPort(address.endpoint);
    const server = new WebSocketServer({ host, port });
    await once(server, "listening");
    const info = server.address();
    const boundPort = typeof info === "object" && info !== null ? info.port : port;
    const boundAddress = new SiloAddress(address.podName, address.podUid, `${host}:${boundPort}`);

    server.on("connection", (socket) => {
      socket.binaryType = "arraybuffer";
      let from: SiloAddress | undefined;
      socket.on("message", (data: ArrayBuffer) => {
        const bytes = toBytes(data);
        if (from === undefined) {
          const preamble = this.tryPreamble(bytes);
          if (
            preamble === undefined ||
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
            };
            onAccept(preamble, connection);
          }
          return;
        }
        const message = this.serializer.deserialize<Message>(bytes);
        recordMessageReceived({
          "thresh.peer": from.endpoint,
          "thresh.message.direction": message.direction,
        });
        void onMessage(message, from);
      });
    });

    return {
      address: boundAddress,
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
  async connect(
    to: SiloAddress,
    preamble: ConnectionPreamble,
    onMessage?: MessageHandler,
  ): Promise<Connection> {
    const { host, port } = splitHostPort(to.endpoint);
    const socket = new WebSocket(`ws://${host}:${port}`);
    socket.binaryType = "arraybuffer";
    await once(socket, "open");

    let ackReceived = false;
    let resolveAck: (() => void) | undefined;
    let rejectAck: ((err: unknown) => void) | undefined;
    const ack = new Promise<void>((resolve, reject) => {
      resolveAck = resolve;
      rejectAck = reject;
    });

    socket.on("message", (data: ArrayBuffer) => {
      if (!ackReceived) {
        ackReceived = true;
        resolveAck?.();
        return;
      }
      if (onMessage === undefined) return; // no hook installed; post-ack frames are dropped
      const message = this.serializer.deserialize<Message>(toBytes(data));
      recordMessageReceived({
        "thresh.peer": to.endpoint,
        "thresh.message.direction": message.direction,
      });
      void onMessage(message, to);
    });
    socket.once("close", (code: number) => {
      if (!ackReceived)
        rejectAck?.(new RejectionError(`connection rejected (code ${code})`, "unknownTarget"));
    });

    socket.send(this.serializer.serialize(preamble));
    await ack;

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
    };
  }

  private tryPreamble(bytes: Uint8Array): ConnectionPreamble | undefined {
    try {
      return this.serializer.deserialize<ConnectionPreamble>(bytes);
    } catch {
      return undefined;
    }
  }
}
