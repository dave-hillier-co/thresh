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
 */
export class CorrelationTable {
  private readonly pending = new Map<string, Pending>();

  constructor(private readonly timer: CorrelationTimer = realTimer) {}

  /** `peer` tags the call with the connection it went out on, for `rejectFor`. */
  register(correlationId: bigint, timeoutMs?: number, peer?: string): Promise<Message> {
    const key = correlationId.toString();
    return new Promise<Message>((resolve, reject) => {
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
