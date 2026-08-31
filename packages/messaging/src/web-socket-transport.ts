import { once } from "node:events";
import { WebSocket, WebSocketServer } from "ws";
import { RejectionError } from "@thresh/core/errors";
import { SiloAddress } from "@thresh/core/silo-address";
import { recordMessageReceived } from "@thresh/observability/messaging-metrics";
import type { Message } from "@thresh/messaging/message";
import { MessagePackSerializer } from "@thresh/messaging/msgpack-serializer";
import type {
  Connection,
  ConnectionAcceptHandler,
  ConnectionPreamble,
  Listener,
  MessageHandler,
  Transport,
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
 * WebSocket-backed transport. Each socket carries traffic ONE way, from dialler
 * to listener; the reply flows over a reverse connection the peer opens to the
 * dialler's advertised endpoint, as in-process. On connect, the peer's identity
 * and cluster id are exchanged in a preamble; a mismatched cluster closes the
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
    // Reverse legs opened for accepted peers, so `close()` takes them down too:
    // they are OUTBOUND sockets from this process, which `server.close()` cannot
    // see and which would otherwise keep the event loop alive.
    const reverse = new Set<Connection>();

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
            const connection = this.reverseConnection(preamble.siloAddress, boundAddress);
            reverse.add(connection);
            socket.once("close", () => {
              reverse.delete(connection);
              void connection.close();
            });
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
        const legs = [...reverse];
        reverse.clear();
        await Promise.all(legs.map((c) => c.close().catch(() => undefined)));
        await new Promise<void>((resolve) => {
          for (const client of server.clients) client.terminate(); // else close() waits forever
          server.close(() => resolve());
        });
      },
    };
  }

  /**
   * The `Connection` a listener hands to `onAccept` for an accepted peer.
   *
   * It does NOT write back down the accepted socket. A socket here carries
   * traffic ONE way — `connect()` returns a send-only `Connection` whose only
   * inbound read is the preamble ack — so a frame written back down it lands on
   * the dialler with nothing listening and is dropped. The reverse leg is
   * therefore addressed to the peer's ADVERTISED endpoint, the one it announced
   * in its preamble, and arrives on that peer's own listener: exactly what
   * `InProcessTransport` does, where the reverse connection is
   * `network.deliver(from, ...)`. That is what lets a gateway push a call to a
   * client-hosted observer (`ClusterNode.deliverToProxy`) over real sockets.
   *
   * Dialled lazily on the first send and reused after; sends are chained so
   * they keep their order across the dial, and a peer that has gone away drops
   * the message rather than throwing into the caller's turn — `send` is
   * fire-and-forget, and the waiting correlation times out.
   *
   * A FAILED leg is forgotten rather than memoized. Caching the promise itself
   * would make one unlucky dial permanent: the rejected promise stays in
   * `dialled`, every later send awaits the same rejection, and the peer is
   * unreachable for the life of the accepted socket even after it recovers.
   * The same applies once a dialled socket dies — the peer restarted its
   * listener while this accepted socket stayed open — so a throwing `send`
   * clears the slot too and the next one re-dials. The message that hit the
   * dead socket is still lost; this only stops the loss from being permanent.
   */
  private reverseConnection(peer: SiloAddress, self: SiloAddress): Connection {
    let dialled: Promise<Connection> | undefined;
    const open = (): Promise<Connection> => {
      if (dialled === undefined) {
        const attempt = this.connect(peer, {
          protocolVersion: 1,
          siloAddress: self,
          clusterId: this.clusterId,
        });
        // Forget a dial that never completed, so the next send retries it. The slot holds the
        // GUARDED promise, so that is what the guard must compare against — comparing the raw
        // `attempt` never matches and the reset silently never fires.
        const guarded: Promise<Connection> = attempt.catch((err: unknown) => {
          if (dialled === guarded) dialled = undefined;
          throw err;
        });
        dialled = guarded;
      }
      return dialled;
    };
    let queue: Promise<void> = Promise.resolve();
    return {
      send: (message) => {
        queue = queue
          .then(async () => {
            const connection = await open();
            try {
              connection.send(message);
            } catch (err) {
              // The socket died under us; drop this message but re-dial next time.
              if (dialled !== undefined) {
                const stale = dialled;
                dialled = undefined;
                void stale.then((c) => c.close()).catch(() => undefined);
              }
              throw err;
            }
          })
          .catch(() => undefined);
      },
      close: async () => {
        const pending = dialled;
        dialled = undefined;
        if (pending === undefined) return;
        await pending.then((c) => c.close()).catch(() => undefined);
      },
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
