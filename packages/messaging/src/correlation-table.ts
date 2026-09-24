import { GrainCallTimeoutError } from "@thresh/core/errors";
import type { Message } from "@thresh/messaging/message";

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
  peer?: string;
}

/**
 * Matches a response to the promise of its awaiting request, keyed by
 * correlation id. Response interpretation (success vs error vs rejection) is
 * the dispatcher's job; this only resolves on arrival, rejects on timeout, and
 * fails everything outstanding when a connection drops.
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

  constructor(private readonly timer: CorrelationTimer = realTimer) {}

  /** `peer` tags the call with the connection it went out on, for `rejectFor`. */
  register(correlationId: bigint, timeoutMs?: number, peer?: string): Promise<Message> {
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
        ...(peer !== undefined ? { peer } : {}),
      });
    });
    // Marks this promise as handled so its rejection can never surface unobserved. A caller that
    // awaits it still sees the error exactly as before — the extra handler observes nothing and
    // swallows only what nobody else was ever going to look at.
    void promise.catch(() => undefined);
    return promise;
  }

  /** Resolve the pending promise for this response. Returns false if unknown. */
  complete(message: Message): boolean {
    const key = message.correlationId.toString();
    const entry = this.pending.get(key);
    if (entry === undefined) return false;
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

  /** Fail the outstanding calls tagged with this peer, e.g. when its connection is lost. */
  rejectFor(peer: string, err: unknown): void {
    for (const [key, entry] of this.pending) {
      if (entry.peer !== peer) continue;
      this.pending.delete(key);
      this.clearTimer(entry);
      entry.reject(err);
    }
  }

  private clearTimer(entry: Pending): void {
    if (entry.timer !== undefined) this.timer.clear(entry.timer);
  }
}
