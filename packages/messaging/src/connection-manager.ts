import type { SiloAddress } from "@tsva/core/silo-address";
import type { Connection, Transport } from "@tsva/messaging/transport";

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
  ) {}

  get(to: SiloAddress): Promise<Connection> {
    const key = to.endpoint;
    let conn = this.connections.get(key);
    if (conn === undefined) {
      conn = this.transport
        .connect(to, { protocolVersion: 1, siloAddress: this.self, clusterId: this.clusterId })
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
