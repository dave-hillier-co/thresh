import { RejectionError } from "@thresh/core/errors";
import type { GrainId } from "@thresh/core/grain-id";
import type { SiloAddress } from "@thresh/core/silo-address";
import type { TimeProvider } from "@thresh/core/time-provider";
import type { Message } from "@thresh/messaging/message";

/** Identifies a peer when a connection is established, mirroring Orleans' ConnectionPreamble. */
export interface ConnectionPreamble {
  protocolVersion: number;
  siloAddress: SiloAddress;
  /** Rejects cross-cluster connections. */
  clusterId: string;
  /** Identity of a connecting client, when the peer is a client rather than a silo. */
  clientId?: GrainId;
  /**
   * The dialling process's epoch (`PROCESS_EPOCH`), fixed for the life of that process. Two
   * incarnations of a container share a `SiloAddress` (it has no generation here, unlike
   * Orleans'); the epoch is what tells them apart, and it is also stamped on every request the
   * process sends (`Message.correlationEpoch`) so a late reply meant for a predecessor can
   * complete nothing in its successor.
   */
  epoch: number;
}

/**
 * Receives one inbound message. `connection` is the connection it arrived on, writing back to
 * its sender: the answer to a request goes down it, never down a second connection dialled back
 * to `from` (Orleans answers on the connection the request came in on). A transport passes it
 * for every message; the parameter is optional only so a handler may ignore it.
 */
export type MessageHandler = (
  message: Message,
  from: SiloAddress,
  connection?: Connection,
) => void | Promise<void>;

export interface Listener {
  readonly address: SiloAddress;
  close(): Promise<void>;
  /**
   * Registers a callback fired once when the listener stops accepting connections, whether
   * through `close()` or on its own (the server went away). Optional: a transport that cannot
   * observe this need not implement it, and its listener is then assumed to stay up.
   */
  onClose?(callback: () => void): void;
}

export interface Connection {
  /**
   * Fire-and-forget; a response to a dialler arrives on its `onMessage` hook, not here. Throws
   * when the connection is already closed (by either end): a message that cannot leave must
   * not look as though it did.
   */
  send(message: Message): void;
  close(reason?: string): Promise<void>;
  /**
   * Registers a callback fired once if the transport drops this connection on
   * its own — the peer closing the socket, or a socket error (ECONNRESET, a
   * malformed frame) — as opposed to a close this side initiated by calling
   * `close()`. Optional: a transport (or test double) that never loses a
   * connection out from under its caller need not implement it.
   */
  onClose?(callback: (err?: unknown) => void): void;
}

/**
 * Fires once per accepted inbound connection, giving the listener the peer's
 * preamble and a duplex `Connection` it can hold to reach that peer later
 * (mirroring how Orleans learns a client or silo connection on accept). The
 * connection writes back down the ACCEPTED socket — this is the peer's own
 * reachability, not a separate dial to an address it advertised. It is the
 * same object the listener's `MessageHandler` receives for every message that
 * arrives on that socket.
 */
export type ConnectionAcceptHandler = (
  preamble: ConnectionPreamble,
  connection: Connection,
) => void;

/** Current preamble wire version. Bump (and validate on accept) whenever the duplex contract changes. */
export const PROTOCOL_VERSION = 3;

/** Orleans' `ConnectionOptions.OpenConnectionTimeout` default: how long a dial may take. */
export const CONNECT_TIMEOUT_MS = 5_000;

/**
 * The one failure `Transport.connect` reports for a peer it could not reach:
 * nothing listening, a listener that closed or refused during the handshake, or
 * a dial that outlasted `CONNECT_TIMEOUT_MS`. Orleans' `ConnectionManager`
 * wraps all of these in a `ConnectionFailedException`, and `MessageCenter`
 * turns it into a `Transient` rejection of the message. The kind is
 * `unknownTarget`, this runtime's reroutable refusal: a failed dial means the
 * request was never written to any socket, so the callee cannot have started
 * the turn and a resend is safe. That is the opposite of `siloUnavailable`,
 * which means an ESTABLISHED connection died mid-call and is deliberately not
 * resendable (issue #88).
 */
export function connectFailure(
  to: SiloAddress,
  reason: string,
  cause: ConnectFailureCause = "handshake",
): ConnectFailure {
  return new ConnectFailure(`unable to connect to ${to.toString()}: ${reason}`, cause);
}

/**
 * Why a dial failed. `refused` means no process is serving at the address (no
 * endpoint registered, ECONNREFUSED): the only case where the peer can be
 * presumed to host nothing. `handshake` (the listener closed or rejected the
 * connection, a cluster or protocol mismatch) and `timeout` say nothing about
 * whether a live silo is behind the address, so callers that reason about what
 * the peer holds must not treat them as `refused`.
 */
export type ConnectFailureCause = "refused" | "handshake" | "timeout";

/** A `RejectionError` of kind `unknownTarget` carrying the `ConnectFailureCause`. */
export class ConnectFailure extends RejectionError {
  constructor(
    message: string,
    override readonly cause: ConnectFailureCause,
  ) {
    super(message, "unknownTarget");
  }
}

/**
 * Bounds a dial: rejects with `connectFailure` once `timeoutMs` elapse on
 * `time`, calling `onTimeout` so the transport can abandon the half-open dial.
 * The timer is armed synchronously, so a fake clock can advance it at once.
 */
export function boundDial<T>(
  time: TimeProvider,
  timeoutMs: number,
  to: SiloAddress,
  dial: Promise<T>,
  onTimeout?: () => void,
): Promise<T> {
  return new Promise<T>((resolve, reject) => {
    const timer = time.setTimer(() => {
      onTimeout?.();
      reject(connectFailure(to, `timed out after ${timeoutMs}ms`, "timeout"));
    }, timeoutMs);
    dial.then(
      (value) => {
        time.clearTimer(timer);
        resolve(value);
      },
      (err: unknown) => {
        time.clearTimer(timer);
        reject(err);
      },
    );
  });
}

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
   *
   * Rejects with `connectFailure` (a `RejectionError` of kind `unknownTarget`)
   * for ANY failure to reach the peer, on every transport: never a raw socket
   * error such as ECONNREFUSED.
   */
  connect(
    to: SiloAddress,
    preamble: ConnectionPreamble,
    onMessage?: MessageHandler,
  ): Promise<Connection>;
}
