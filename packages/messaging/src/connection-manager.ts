import { RejectionError } from "@thresh/core/errors";
import type { GrainId } from "@thresh/core/grain-id";
import type { SiloAddress } from "@thresh/core/silo-address";
import { recordMessageSent, recordQueueLatency } from "@thresh/observability/messaging-metrics";
import { PROCESS_EPOCH, type Message } from "@thresh/messaging/message";
import {
  PROTOCOL_VERSION,
  type Connection,
  type MessageHandler,
  type Transport,
} from "@thresh/messaging/transport";

/**
 * Pools one duplex connection per peer silo, opened lazily on first use and
 * reused for all traffic to that peer. A failed connect is not cached, so the
 * next call retries; `drop` discards a connection (e.g. after a peer leaves) so
 * a later call reconnects.
 */
export class ConnectionManager {
  private readonly connections = new Map<string, Promise<Connection>>();

  constructor(
    private readonly transport: Transport,
    private readonly self: SiloAddress,
    private readonly clusterId: string,
    /** When set, advertised in the preamble so the accepting silo learns this is a client connection. */
    private readonly clientId?: GrainId,
    /**
     * Receives every frame the peer sends back down a dialled socket after the
     * preamble ack — a reply, or a push — the duplex half of the connection
     * this manager pools. Its `connection` argument is the very object `get`
     * returned for that dial, so a caller can tell which connection a reply
     * came in on and answer a push down the same one.
     */
    private readonly onMessage?: MessageHandler,
    /**
     * Fired when the transport reports a pooled connection was lost on its
     * own (peer closed it, or a socket error) rather than via `drop`/
     * `closeAll`. The cache entry is already invalidated by the time this
     * fires. `connection` is the object `get` returned for it, so a caller can
     * fail exactly the calls sent on it (a correlation table's `rejectFor`)
     * instead of waiting on a timeout.
     */
    private readonly onConnectionLost?: (
      to: SiloAddress,
      err: unknown,
      connection: Connection,
    ) => void,
  ) {}

  get(to: SiloAddress): Promise<Connection> {
    const key = to.endpoint;
    let conn = this.connections.get(key);
    if (conn === undefined) {
      const dialStart = Date.now();
      // The one object this dial is known by: returned from `get`, handed to
      // `onMessage` with every frame the peer sends back, and named to
      // `onConnectionLost`. It exists before the dial completes so a frame
      // racing the preamble ack is attributed to it too.
      let opened: Connection | undefined;
      const pooled: Connection = {
        send: (message: Message) => {
          if (opened === undefined) {
            throw new RejectionError(`connection to ${key} is not open yet`, "unknownTarget");
          }
          recordMessageSent({ "thresh.peer": key, "thresh.message.direction": message.direction });
          opened.send(message);
        },
        close: async (reason?: string) => {
          await opened?.close(reason);
        },
      };
      const onMessage = this.onMessage;
      conn = this.transport
        .connect(
          to,
          {
            protocolVersion: PROTOCOL_VERSION,
            siloAddress: this.self,
            clusterId: this.clusterId,
            epoch: PROCESS_EPOCH,
            ...(this.clientId ? { clientId: this.clientId } : {}),
          },
          onMessage === undefined ? undefined : (message, from) => onMessage(message, from, pooled),
        )
        .then((connection) => {
          recordQueueLatency(Date.now() - dialStart, { "thresh.peer": key });
          opened = connection;
          // The transport may drop this connection on its own (the peer closed
          // it, or the underlying socket errored). Only invalidate the cache
          // when this is still the pooled connection for `key` — a stale
          // callback from a connection already superseded by `drop`+re-dial
          // must not evict the fresh one.
          connection.onClose?.((err) => {
            if (this.connections.get(key) === conn) this.connections.delete(key);
            this.onConnectionLost?.(to, err, pooled);
          });
          return pooled;
        })
        .catch((err: unknown) => {
          this.connections.delete(key); // don't cache a failed connect
          throw err;
        });
      this.connections.set(key, conn);
    }
    return conn;
  }

  async drop(to: SiloAddress): Promise<void> {
    const key = to.endpoint;
    const conn = this.connections.get(key);
    this.connections.delete(key);
    if (conn !== undefined) await closeQuietly(conn);
  }

  async closeAll(): Promise<void> {
    const all = [...this.connections.values()];
    this.connections.clear();
    await Promise.all(all.map(closeQuietly));
  }
}

async function closeQuietly(conn: Promise<Connection>): Promise<void> {
  try {
    await (await conn).close();
  } catch {
    // a connection that never opened or already closed is fine to ignore
  }
}
