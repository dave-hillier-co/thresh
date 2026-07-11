import type { GrainId } from "@tsva/core/grain-id";
import type { SiloAddress } from "@tsva/core/silo-address";
import type { Message } from "@tsva/messaging/message";

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
  /** Fire-and-forget; responses arrive as inbound messages on the listener. */
  send(message: Message): void;
  close(reason?: string): Promise<void>;
}

/**
 * Fires once per accepted inbound connection, giving the listener the peer's
 * preamble and a duplex `Connection` it can hold to reach that peer later
 * (mirroring how Orleans' gateway learns a client connection on accept).
 */
export type ConnectionAcceptHandler = (preamble: ConnectionPreamble, connection: Connection) => void;

/** Abstracts silo-to-silo and client-to-silo message transport. */
export interface Transport {
  listen(
    address: SiloAddress,
    onMessage: MessageHandler,
    onAccept?: ConnectionAcceptHandler,
  ): Promise<Listener>;
  connect(to: SiloAddress, preamble: ConnectionPreamble): Promise<Connection>;
}
