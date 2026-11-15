import { RejectionError } from "@thresh/core/errors";
import type { MembershipService, MembershipSnapshot } from "@thresh/core/membership";
import type { SiloAddress } from "@thresh/core/silo-address";
import { nowNanosOf, type TimeProvider, type TimerHandle } from "@thresh/core/time-provider";
import type {
  Connection,
  ConnectionAcceptHandler,
  ConnectionPreamble,
  Listener,
  MessageHandler,
  Transport,
} from "@thresh/messaging/transport";

/**
 * The pieces of an in-process silo that a crash has to cut off, each owned by the harness so it
 * can stop them without the silo's cooperation: a crashed process runs no shutdown code.
 */

/**
 * One silo's clock: the cluster's clock, plus the record of every timer this silo armed, so a
 * crash cancels them all and no new one ever fires. Grain timers, reminder ticks and refreshes,
 * durable-job polls, idle collection, load publishing and the rebalancer all arm their timers
 * here.
 */
export class SiloClock implements TimeProvider {
  private readonly pending = new Map<object, TimerHandle>();
  private stopped = false;

  constructor(private readonly base: TimeProvider) {}

  now(): number {
    return this.base.now();
  }

  nowNanos(): bigint {
    return nowNanosOf(this.base);
  }

  setTimer(handler: () => void, delayMs: number): TimerHandle {
    const token = {};
    if (this.stopped) return token;
    const inner = this.base.setTimer(() => {
      this.pending.delete(token);
      if (!this.stopped) handler();
    }, delayMs);
    this.pending.set(token, inner);
    return token;
  }

  clearTimer(handle: TimerHandle): void {
    if (typeof handle !== "object" || handle === null) return;
    const inner = this.pending.get(handle);
    if (inner === undefined) return;
    this.pending.delete(handle);
    this.base.clearTimer(inner);
  }

  /** Cancel every timer and refuse new ones. */
  stop(): void {
    this.stopped = true;
    for (const inner of this.pending.values()) this.base.clearTimer(inner);
    this.pending.clear();
  }
}

/**
 * One silo's transport, which a crash takes away: its listeners close (peers dialling it are
 * refused, as by a host that is gone), and nothing it sends or is sent arrives any more.
 */
export class CrashableTransport implements Transport {
  private readonly listeners: Listener[] = [];
  private dead = false;

  constructor(private readonly inner: Transport) {}

  async listen(
    address: SiloAddress,
    onMessage: MessageHandler,
    onAccept?: ConnectionAcceptHandler,
  ): Promise<Listener> {
    if (this.dead) throw this.gone(address);
    // One wrapper per accepted connection, so the accept hook and the handler see the same object.
    const wrappers = new WeakMap<Connection, Connection>();
    const wrapperOf = (connection: Connection): Connection => {
      let wrapped = wrappers.get(connection);
      if (wrapped === undefined) {
        wrapped = this.severable(connection);
        wrappers.set(connection, wrapped);
      }
      return wrapped;
    };
    const listener = await this.inner.listen(
      address,
      (message, from, connection) =>
        this.dead
          ? undefined
          : onMessage(message, from, connection === undefined ? undefined : wrapperOf(connection)),
      onAccept === undefined
        ? undefined
        : (preamble, connection) => {
            if (!this.dead) onAccept(preamble, wrapperOf(connection));
          },
    );
    this.listeners.push(listener);
    return listener;
  }

  async connect(
    to: SiloAddress,
    preamble: ConnectionPreamble,
    onMessage?: MessageHandler,
  ): Promise<Connection> {
    if (this.dead) throw this.gone(to);
    const wrapper: { connection?: Connection } = {};
    const connection = await this.inner.connect(
      to,
      preamble,
      onMessage === undefined
        ? undefined
        : (message, from, via) =>
            this.dead ? undefined : onMessage(message, from, wrapper.connection ?? via),
    );
    wrapper.connection = this.severable(connection);
    return wrapper.connection;
  }

  /** Close every listener and cut every connection, for good. */
  async crash(): Promise<void> {
    this.dead = true;
    await Promise.all(this.listeners.splice(0).map((l) => l.close().catch(() => undefined)));
  }

  private severable(connection: Connection): Connection {
    const severable: Connection = {
      send: (message) => {
        if (!this.dead) connection.send(message);
      },
      close: (reason) => connection.close(reason),
    };
    if (connection.onClose !== undefined) {
      severable.onClose = (callback) => connection.onClose!(callback);
    }
    return severable;
  }

  private gone(peer: SiloAddress): RejectionError {
    return new RejectionError(
      `this silo has crashed (contacting ${peer.toString()})`,
      "unknownTarget",
    );
  }
}

/**
 * One silo's window onto the shared static membership: it reports the silo's own `localSilo()`,
 * and a crash freezes it, so the dead silo sees no later view and runs no ownership hook (no
 * reminder range read, job shard claim or queue takeover) after it died.
 */
export class SharedMembershipView implements MembershipService {
  private frozenAt: MembershipSnapshot | undefined;
  private readonly frozenListeners = new Set<() => void>();

  constructor(
    private readonly shared: MembershipService,
    private readonly local: SiloAddress,
  ) {}

  current(): MembershipSnapshot {
    return this.frozenAt ?? this.shared.current();
  }

  updates(): AsyncIterableIterator<MembershipSnapshot> {
    const inner = this.shared.updates()[Symbol.asyncIterator]();
    let ended = this.frozenAt !== undefined;
    let wake: (() => void) | undefined;
    const frozen = new Promise<IteratorResult<MembershipSnapshot>>((resolve) => {
      wake = () => resolve({ done: true, value: undefined });
    });
    if (wake !== undefined) this.frozenListeners.add(wake);
    const iterator: AsyncIterableIterator<MembershipSnapshot> = {
      next: async () => {
        if (ended || this.frozenAt !== undefined) return { done: true, value: undefined };
        const result = await Promise.race([inner.next(), frozen]);
        if (result.done === true || this.frozenAt !== undefined) {
          ended = true;
          return { done: true, value: undefined };
        }
        return result;
      },
      return: async () => {
        ended = true;
        if (wake !== undefined) this.frozenListeners.delete(wake);
        await inner.return?.();
        return { done: true, value: undefined };
      },
      [Symbol.asyncIterator]: () => iterator,
    };
    return iterator;
  }

  localSilo(): SiloAddress {
    return this.local;
  }

  refresh(version: number): Promise<void> {
    return this.frozenAt !== undefined ? Promise.resolve() : this.shared.refresh(version);
  }

  /** Stop following the shared view: hold the current snapshot and end every update stream. */
  freeze(): void {
    if (this.frozenAt !== undefined) return;
    this.frozenAt = this.shared.current();
    for (const wake of this.frozenListeners) wake();
    this.frozenListeners.clear();
  }
}
