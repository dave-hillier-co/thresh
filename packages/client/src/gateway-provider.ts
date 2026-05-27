import { activeSilos, type MembershipService } from "@tsva/core/membership";
import { SiloAddress } from "@tsva/core/silo-address";

/**
 * Supplies the set of gateway silos a client may connect through (Orleans'
 * `IGatewayListProvider`). The client's `GatewayManager` polls this to learn the
 * live gateways and to recover ones it had marked dead; an implementation may
 * return a fixed list or a list that tracks cluster membership.
 */
export interface GatewayListProvider {
  /** The gateway silos currently advertised for clients to connect through. */
  getGateways(): SiloAddress[] | Promise<SiloAddress[]>;
}

/** A fixed set of gateways — the list never changes (Orleans' `StaticGatewayListProvider`). */
export function staticGatewayProvider(gateways: readonly SiloAddress[]): GatewayListProvider {
  const snapshot = [...gateways];
  return { getGateways: () => snapshot };
}

/**
 * Gateways tracked from cluster membership: every active silo is a candidate
 * gateway. As silos join and leave, a `refresh` picks up the change — the client
 * equivalent of Orleans' membership-table gateway list provider.
 */
export function membershipGatewayProvider(membership: MembershipService): GatewayListProvider {
  return { getGateways: () => activeSilos(membership.current()) };
}

/**
 * Gateways named by URL (`gateway: { url }` style) — for a client that only knows
 * an address, not a `SiloAddress`. Each URL's `host:port` becomes the gateway's
 * endpoint (the scheme, if any, is ignored); a stable synthetic identity is
 * derived from it, since the client addresses a gateway only by endpoint and
 * routes replies to its own `local`.
 */
export function urlGatewayProvider(urls: readonly string[]): GatewayListProvider {
  const gateways = urls.map((url) => {
    const endpoint = url.replace(/^[a-z]+:\/\//i, "").replace(/\/.*$/, "");
    return new SiloAddress(`gateway@${endpoint}`, endpoint, endpoint);
  });
  return staticGatewayProvider(gateways);
}
