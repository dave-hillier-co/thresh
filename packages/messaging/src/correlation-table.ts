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

  register(correlationId: bigint, timeoutMs?: number): Promise<Message> {
    const key = correlationId.toString();
    return new Promise<Message>((resolve, reject) => {
      let handle: unknown;
      if (timeoutMs !== undefined) {
        handle = this.timer.set(() => {
          this.pending.delete(key);
          reject(new GrainCallTimeoutError(`grain call ${key} timed out after ${timeoutMs}ms`));
        }, timeoutMs);
      }
      this.pending.set(key, { resolve, reject, timer: handle });
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

  /** Fail all outstanding calls, e.g. when a connection is lost. */
  rejectAll(err: unknown): void {
    for (const entry of this.pending.values()) {
      this.clearTimer(entry);
      entry.reject(err);
    }
    this.pending.clear();
  }

  private clearTimer(entry: Pending): void {
    if (entry.timer !== undefined) this.timer.clear(entry.timer);
  }
}
