import type { GrainId } from "@thresh/core/grain-id";
import type { SiloAddress } from "@thresh/core/silo-address";
import { recordMessageSent, recordQueueLatency } from "@thresh/observability/messaging-metrics";
import type { Message } from "@thresh/messaging/message";
import type { Connection, Transport } from "@thresh/messaging/transport";

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
    private self: SiloAddress,
    private readonly clusterId: string,
    /** When set, advertised in the preamble so the accepting silo learns this is a client connection. */
    private readonly clientId?: GrainId,
  ) {}

  /**
   * Adopt the address this node actually listens on, once its listener has
   * bound. A node that asks for an EPHEMERAL port (endpoint `host:0` — an
   * embedded client leg on a silo, which has no port of its own to advertise)
   * only learns its real address from `Listener.address`, and the peer it dials
   * has to be told that one: the preamble's `siloAddress` is what the peer
   * replies to. Call before the first `get`, so no pooled connection has
   * already announced the placeholder.
   */
  setSelf(address: SiloAddress): void {
    this.self = address;
  }

  get(to: SiloAddress): Promise<Connection> {
    const key = to.endpoint;
    let conn = this.connections.get(key);
    if (conn === undefined) {
      const dialStart = Date.now();
      conn = this.transport
        .connect(to, {
          protocolVersion: 1,
          siloAddress: this.self,
          clusterId: this.clusterId,
          ...(this.clientId ? { clientId: this.clientId } : {}),
        })
        .then((connection) => {
          recordQueueLatency(Date.now() - dialStart, { "thresh.peer": key });
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
