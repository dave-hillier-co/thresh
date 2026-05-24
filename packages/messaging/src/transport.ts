import type { SiloAddress } from "@tsva/core/silo-address";
import type { Message } from "@tsva/messaging/message";

/** Identifies a peer when a connection is established, mirroring Orleans' ConnectionPreamble. */
export interface ConnectionPreamble {
  protocolVersion: number;
  siloAddress: SiloAddress;
  /** Rejects cross-cluster connections. */
  clusterId: string;
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

/** Abstracts silo-to-silo and client-to-silo message transport. */
export interface Transport {
  listen(address: SiloAddress, onMessage: MessageHandler): Promise<Listener>;
  connect(to: SiloAddress, preamble: ConnectionPreamble): Promise<Connection>;
}
