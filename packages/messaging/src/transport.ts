import type { GrainId } from "@thresh/core/grain-id";
import type { SiloAddress } from "@thresh/core/silo-address";
import type { Message } from "@thresh/messaging/message";

/** Identifies a peer when a connection is established, mirroring Orleans' ConnectionPreamble. */
export interface ConnectionPreamble {
  protocolVersion: number;
  siloAddress: SiloAddress;
  /** Rejects cross-cluster connections. */
  clusterId: string;
  /** Identity of a connecting client, when the peer is a client rather than a silo. */
  clientId?: GrainId;
}

export type MessageHandler = (message: Message, from: SiloAddress) => void | Promise<void>;

export interface Listener {
  readonly address: SiloAddress;
  close(): Promise<void>;
}

export interface Connection {
  /** Fire-and-forget; a response to a dialler arrives on its `onMessage` hook, not here. */
  send(message: Message): void;
  close(reason?: string): Promise<void>;
}

/**
 * Fires once per accepted inbound connection, giving the listener the peer's
 * preamble and a duplex `Connection` it can hold to reach that peer later
 * (mirroring how Orleans' gateway learns a client connection on accept). The
 * connection writes back down the ACCEPTED socket — this is the peer's own
 * reachability, not a separate dial to an address it advertised.
 */
export type ConnectionAcceptHandler = (
  preamble: ConnectionPreamble,
  connection: Connection,
) => void;

/** Current preamble wire version. Bump (and validate on accept) whenever the duplex contract changes. */
export const PROTOCOL_VERSION = 2;

/** Abstracts silo-to-silo and client-to-silo message transport. */
export interface Transport {
  listen(
    address: SiloAddress,
    onMessage: MessageHandler,
    onAccept?: ConnectionAcceptHandler,
  ): Promise<Listener>;
  /**
   * Dial `to`. When `onMessage` is supplied, every frame the peer sends back
   * down THIS socket after the preamble ack is delivered to it — this is what
   * makes the dialled connection duplex: a silo answering a client, or
   * pushing to a client-hosted observer, does so over the socket the client
   * dialled, never by dialling the client back.
   */
  connect(
    to: SiloAddress,
    preamble: ConnectionPreamble,
    onMessage?: MessageHandler,
  ): Promise<Connection>;
}
