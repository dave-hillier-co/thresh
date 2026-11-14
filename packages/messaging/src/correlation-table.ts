import { GrainCallTimeoutError } from "@thresh/core/errors";
import { recordMisroutedResponse } from "@thresh/observability/messaging-metrics";
import { PROCESS_EPOCH, type Message } from "@thresh/messaging/message";

/** Injectable timer so call-timeout behaviour is deterministic in tests. */
export interface CorrelationTimer {
  set(callback: () => void, ms: number): unknown;
  clear(handle: unknown): void;
}

const realTimer: CorrelationTimer = {
  set: (cb, ms) => setTimeout(cb, ms),
  clear: (h) => clearTimeout(h as ReturnType<typeof setTimeout>),
};

interface Pending {
  resolve: (message: Message) => void;
  reject: (err: unknown) => void;
  timer: unknown;
  /** The connection the request went out on; only a response arriving on it completes the call. */
  connection?: object;
}

/**
 * Matches a response to the promise of its awaiting request. A call's identity
 * is its correlation id, this process's epoch (stamped on the request and
 * copied onto its response) and the connection the request went out on: a
 * response completes a call only when all three agree, so a late reply meant
 * for this container's previous incarnation, or one arriving on some other
 * connection, completes nothing — it is ignored and counted (`misrouted`).
 * Response interpretation (success vs error vs rejection) is the dispatcher's
 * job; this only resolves on arrival, rejects on timeout, and fails exactly
 * the calls pending on a connection when that connection drops.
 *
 * Every promise this hands out carries a catch-all of its own (see `register`),
 * because a caller is free to stop awaiting one: a call abandoned during a
 * shutdown, or one whose send threw before the `await` was reached, leaves an
 * armed entry behind, and the deadline firing on it must not become a
 * process-level unhandled rejection — under Node's default
 * `--unhandled-rejections=throw` that terminates the process. Abandonment is
 * the caller's business; ending the process is not.
 */
export class CorrelationTable {
  private readonly pending = new Map<string, Pending>();
  private misroutedCount = 0;

  constructor(
    private readonly timer: CorrelationTimer = realTimer,
    /** Stamped on every request this table awaits; a response must carry it back. */
    readonly epoch: number = PROCESS_EPOCH,
  ) {}

  /** Responses that named an outstanding call but arrived on another connection or epoch. */
  get misrouted(): number {
    return this.misroutedCount;
  }

  /**
   * Arm a call. `connection` binds it to the connection its request goes out on: only a
   * response arriving there completes it, and `rejectFor(connection)` fails it.
   */
  register(correlationId: bigint, timeoutMs?: number, connection?: object): Promise<Message> {
    const key = correlationId.toString();
    const promise = new Promise<Message>((resolve, reject) => {
      let handle: unknown;
      if (timeoutMs !== undefined) {
        handle = this.timer.set(() => {
          this.pending.delete(key);
          reject(new GrainCallTimeoutError(`grain call ${key} timed out after ${timeoutMs}ms`));
        }, timeoutMs);
      }
      this.pending.set(key, {
        resolve,
        reject,
        timer: handle,
        ...(connection !== undefined ? { connection } : {}),
      });
    });
    // Marks this promise as handled so its rejection can never surface unobserved. A caller that
    // awaits it still sees the error exactly as before — the extra handler observes nothing and
    // swallows only what nobody else was ever going to look at.
    void promise.catch(() => undefined);
    return promise;
  }

  /**
   * Resolve the pending call this response answers, when it arrived on `connection` — the one
   * the call went out on — and carries this table's epoch. Returns false, completing nothing,
   * otherwise: for an id with nothing outstanding (a reply after its call timed out), and for a
   * known id on the wrong connection or epoch, which is also counted.
   */
  complete(message: Message, connection?: object): boolean {
    const key = message.correlationId.toString();
    const entry = this.pending.get(key);
    if (entry === undefined) return false;
    const wrongConnection = entry.connection !== undefined && entry.connection !== connection;
    if (wrongConnection || message.correlationEpoch !== this.epoch) {
      this.misroutedCount += 1;
      recordMisroutedResponse({
        "thresh.misrouted.reason": wrongConnection ? "connection" : "epoch",
      });
      return false;
    }
    this.pending.delete(key);
    this.clearTimer(entry);
    entry.resolve(message);
    return true;
  }

  /**
   * Fail one outstanding call whose request never left this process — the send
   * threw after `register` armed the entry, so no reply can ever complete it.
   * Releases the entry rather than leaving it to fire its deadline on a call
   * nobody is waiting for. Returns false if the call is not outstanding.
   */
  fail(correlationId: bigint, err: unknown): boolean {
    const key = correlationId.toString();
    const entry = this.pending.get(key);
    if (entry === undefined) return false;
    this.pending.delete(key);
    this.clearTimer(entry);
    entry.reject(err);
    return true;
  }

  /** Fail all outstanding calls, e.g. on shutdown. */
  rejectAll(err: unknown): void {
    for (const entry of this.pending.values()) {
      this.clearTimer(entry);
      entry.reject(err);
    }
    this.pending.clear();
  }

  /** Fail exactly the outstanding calls bound to `connection`, e.g. when it is lost. */
  rejectFor(connection: object, err: unknown): void {
    for (const [key, entry] of this.pending) {
      if (entry.connection !== connection) continue;
      this.pending.delete(key);
      this.clearTimer(entry);
      entry.reject(err);
    }
  }

  private clearTimer(entry: Pending): void {
    if (entry.timer !== undefined) this.timer.clear(entry.timer);
  }
}
