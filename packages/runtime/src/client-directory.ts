import type { GrainId } from "@thresh/core/grain-id";
import type { SiloAddress } from "@thresh/core/silo-address";

/**
 * A per-silo view of which gateway(s) each connected client is reachable
 * through, kept current by gossip from every silo a client connects to
 * (mirrors Orleans' gateway-to-client directory). A client may be connected
 * through several gateways at once; `lookup` picks one, preferring the local
 * silo when it is among them so a silo that already holds the connection
 * routes through itself rather than hopping to a peer.
 */
export class ClientDirectory {
  private readonly byClient = new Map<string, Map<string, SiloAddress>>();

  /** Record that `clientId` is reachable through `gateway`. */
  register(clientId: GrainId, gateway: SiloAddress): void {
    const key = clientId.toString();
    let gateways = this.byClient.get(key);
    if (gateways === undefined) {
      gateways = new Map();
      this.byClient.set(key, gateways);
    }
    gateways.set(gateway.ringKey, gateway);
  }

  /** Drop `gateway` from `clientId`'s entry (e.g. the client disconnected from it). */
  unregister(clientId: GrainId, gateway: SiloAddress): void {
    const key = clientId.toString();
    const gateways = this.byClient.get(key);
    if (gateways === undefined) return;
    gateways.delete(gateway.ringKey);
    if (gateways.size === 0) this.byClient.delete(key);
  }

  /** Drop `gateway` from every client's entry (the silo left the cluster). */
  unregisterSilo(gateway: SiloAddress): void {
    for (const [key, gateways] of this.byClient) {
      gateways.delete(gateway.ringKey);
      if (gateways.size === 0) this.byClient.delete(key);
    }
  }

  /**
   * A gateway `clientId` is reachable through, or `undefined` if none is
   * known. Prefers `preferred` (typically the local silo) when it is among
   * the client's gateways; otherwise returns a deterministic first entry
   * (insertion order).
   */
  lookup(clientId: GrainId, preferred?: SiloAddress): SiloAddress | undefined {
    const gateways = this.byClient.get(clientId.toString());
    if (gateways === undefined) return undefined;
    if (preferred !== undefined) {
      const match = gateways.get(preferred.ringKey);
      if (match !== undefined) return match;
    }
    return gateways.values().next().value;
  }
}
