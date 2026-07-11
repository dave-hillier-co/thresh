import { once } from "node:events";
import { WebSocket, WebSocketServer } from "ws";
import { RejectionError } from "@tsva/core/errors";
import { SiloAddress } from "@tsva/core/silo-address";
import type { Message } from "@tsva/messaging/message";
import { MessagePackSerializer } from "@tsva/messaging/msgpack-serializer";
import type {
  Connection,
  ConnectionAcceptHandler,
  ConnectionPreamble,
  Listener,
  MessageHandler,
  Transport,
} from "@tsva/messaging/transport";

const ACK = Uint8Array.of(1);

function splitHostPort(endpoint: string): { host: string; port: number } {
  const i = endpoint.lastIndexOf(":");
  return { host: endpoint.slice(0, i), port: Number(endpoint.slice(i + 1)) };
}

function toBytes(data: ArrayBuffer): Uint8Array {
  return new Uint8Array(data);
}

/**
 * WebSocket-backed transport. One client->server socket carries traffic in each
 * direction (responses flow back over the reverse connection, as in-process).
 * On connect, the peer's identity and cluster id are exchanged in a preamble; a
 * mismatched cluster closes the socket so `connect` rejects.
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
          if (preamble === undefined || preamble.clusterId !== this.clusterId) {
            socket.close(4001, "cluster mismatch");
            return;
          }
          from = preamble.siloAddress;
          socket.send(ACK);
          if (onAccept !== undefined) {
            const serializer = this.serializer;
            onAccept(preamble, {
              send: (m) => socket.send(serializer.serialize(m)),
              close: () =>
                new Promise<void>((resolve) => {
                  if (socket.readyState === WebSocket.CLOSED) {
                    resolve();
                    return;
                  }
                  socket.once("close", () => resolve());
                  socket.close();
                }),
            });
          }
          return;
        }
        void onMessage(this.serializer.deserialize<Message>(bytes), from);
      });
    });

    return {
      address: boundAddress,
      close: () =>
        new Promise<void>((resolve) => {
          for (const client of server.clients) client.terminate(); // else close() waits forever
          server.close(() => resolve());
        }),
    };
  }

  async connect(to: SiloAddress, preamble: ConnectionPreamble): Promise<Connection> {
    const { host, port } = splitHostPort(to.endpoint);
    const socket = new WebSocket(`ws://${host}:${port}`);
    socket.binaryType = "arraybuffer";
    await once(socket, "open");
    socket.send(this.serializer.serialize(preamble));
    await this.awaitAck(socket);
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

  private awaitAck(socket: WebSocket): Promise<void> {
    return new Promise<void>((resolve, reject) => {
      socket.once("message", () => resolve());
      socket.once("close", (code: number) =>
        reject(new RejectionError(`connection rejected (code ${code})`, "unknownTarget")),
      );
    });
  }
}
