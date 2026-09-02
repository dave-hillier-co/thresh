import type { GrainId } from "@thresh/core/grain-id";
import type { SiloAddress } from "@thresh/core/silo-address";
import { recordMessageSent, recordQueueLatency } from "@thresh/observability/messaging-metrics";
import type { Message } from "@thresh/messaging/message";
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
     * Forwarded to every dial as `connect`'s third argument, so a push the
     * peer sends back down the dialled socket after the preamble ack reaches
     * this node — the duplex half of the connection this manager pools.
     */
    private readonly onMessage?: MessageHandler,
    /**
     * Fired when the transport reports a pooled connection was lost on its
     * own (peer closed it, or a socket error) rather than via `drop`/
     * `closeAll`. The cache entry is already invalidated by the time this
     * fires; callers use it to fail outstanding calls to that peer fast
     * (e.g. a correlation table's `rejectAll`) instead of waiting on a timeout.
     */
    private readonly onConnectionLost?: (to: SiloAddress, err?: unknown) => void,
  ) {}

  get(to: SiloAddress): Promise<Connection> {
    const key = to.endpoint;
    let conn = this.connections.get(key);
    if (conn === undefined) {
      const dialStart = Date.now();
      conn = this.transport
        .connect(
          to,
          {
            protocolVersion: PROTOCOL_VERSION,
            siloAddress: this.self,
            clusterId: this.clusterId,
            ...(this.clientId ? { clientId: this.clientId } : {}),
          },
          this.onMessage,
        )
        .then((connection) => {
          recordQueueLatency(Date.now() - dialStart, { "thresh.peer": key });
          // The transport may drop this connection on its own (the peer closed
          // it, or the underlying socket errored). Only invalidate the cache
          // when this is still the pooled connection for `key` — a stale
          // callback from a connection already superseded by `drop`+re-dial
          // must not evict the fresh one.
          connection.onClose?.((err) => {
            if (this.connections.get(key) === conn) this.connections.delete(key);
            this.onConnectionLost?.(to, err);
          });
          return instrumented(connection, key);
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

/** Wraps a freshly dialed connection's `send` to record the `thresh.messaging.sent` counter. */
function instrumented(connection: Connection, peer: string): Connection {
  return {
    send: (message: Message) => {
      recordMessageSent({ "thresh.peer": peer, "thresh.message.direction": message.direction });
      connection.send(message);
    },
    close: (reason?: string) => connection.close(reason),
  };
}

async function closeQuietly(conn: Promise<Connection>): Promise<void> {
  try {
    await (await conn).close();
  } catch {
    // a connection that never opened or already closed is fine to ignore
  }
}
