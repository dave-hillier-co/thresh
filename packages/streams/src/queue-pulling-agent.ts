import type { GrainType } from "@tsva/core/grain-type";
import {
  recordAgentPoll,
  recordStreamDelivered,
  recordStreamFailed,
} from "@tsva/observability/stream-metrics";
import type { QueueEntry } from "@tsva/streams/redis-stream-queue";
import type { HashRange } from "@tsva/streams/queue-ownership";
import type { StreamDeliver } from "@tsva/streams/stream-deliver";
import { RecoverableStreamDeliveryError } from "@tsva/streams/stream-recovery";

/** Delivers one pulled event to the stream's subscribers; the agent supplies it. */
export type DeliverEvent = (streamKey: string, event: unknown, token: number) => Promise<void>;

/**
 * The minimal shape a physical queue must offer a pulling agent: a durably
 * committed cursor, entries strictly after it, and a way to advance the
 * cursor. `RedisStreamQueue` is the durable implementation; a generator queue
 * (`GeneratorStreamQueue`) synthesizes entries in-memory instead of reading a
 * real backing store, but plugs into the same agent unchanged.
 */
export interface PullableQueue {
  getCursor(): Promise<number>;
  readAfter(cursor: number, count?: number): Promise<QueueEntry[]>;
  commit(cursor: number): Promise<void>;
}

/**
 * What a host needs to drive a pulling-agent-backed stream provider through a
 * silo's lifecycle: wire delivery/implicit subscribers once, then hand it the
 * hash ranges it owns on every membership change (`RedisPullingStreamProvider`
 * and `GeneratorPullingStreamProvider` both implement this).
 */
export interface PullingStreamProviderHost {
  setDeliver(deliver: StreamDeliver): void;
  setImplicitSubscribers(typesFor: (namespace: string) => Iterable<GrainType>): void;
  refreshOwnership(ranges: readonly HashRange[]): void;
}

/**
 * Reports a permanently-failed delivery — Orleans' `IStreamFailureHandler.OnDeliveryFailure`.
 * Called once the agent has exhausted the retry budget and is about to advance
 * the queue cursor past the bad event so it stops blocking other subscribers
 * multiplexed on the same physical queue.
 */
export interface StreamFailureHandler {
  onDeliveryFailure(
    streamKey: string,
    event: unknown,
    token: number,
    error: unknown,
    attempts: number,
  ): Promise<void> | void;
}

export interface QueuePullingAgentOptions {
  /** How often to poll the queue when idle (defaults to 50ms). */
  pollIntervalMs?: number;
  /** Maximum entries read per poll (defaults to 128). */
  batchSize?: number;
  /** Max delivery attempts per event before skipping (defaults to 3). */
  maxAttempts?: number;
  /** Backoff between retries within a single pump (defaults to 2^attempt * 50ms, capped at 5s). */
  retryBackoffMs?: (attempt: number) => number;
  /** Notified when an event is skipped after exhausting the retry budget. */
  failureHandler?: StreamFailureHandler;
  /** Sleep injection — the clock is a true boundary, fake it in tests. */
  sleep?: (ms: number) => Promise<void>;
}

const defaultBackoff = (attempt: number): number => Math.min(50 * 2 ** attempt, 5000);
const defaultSleep = (ms: number): Promise<void> => new Promise((r) => setTimeout(r, ms));

/**
 * Pulls one physical queue and delivers each entry to its subscribers, then
 * commits the queue cursor — so delivery is at-least-once and a successor agent
 * (after a membership change) resumes from the committed position with no gaps.
 * Mirrors an Orleans persistent-stream pulling agent. One agent runs per queue a
 * silo owns; `PullingAgentManager` (a later slice) starts/stops them as ring
 * ownership changes.
 *
 * On delivery failure the agent retries up to `maxAttempts` with exponential
 * backoff. If still failing it skips the event (commits the cursor past it) and
 * notifies the `failureHandler` — Orleans' `IStreamFailureHandler` — so a
 * single poison event cannot stall every other subscriber sharing the queue.
 */
export class QueuePullingAgent {
  private readonly pollIntervalMs: number;
  private readonly batchSize: number;
  private readonly maxAttempts: number;
  private readonly retryBackoffMs: (attempt: number) => number;
  private readonly failureHandler: StreamFailureHandler | undefined;
  private readonly sleep: (ms: number) => Promise<void>;
  private timer: ReturnType<typeof setTimeout> | undefined;
  private cursor: number | undefined;
  private pumping = false;
  private running = false;

  constructor(
    private readonly queue: PullableQueue,
    private readonly deliver: DeliverEvent,
    options: QueuePullingAgentOptions = {},
  ) {
    this.pollIntervalMs = options.pollIntervalMs ?? 50;
    this.batchSize = options.batchSize ?? 128;
    this.maxAttempts = options.maxAttempts ?? 3;
    this.retryBackoffMs = options.retryBackoffMs ?? defaultBackoff;
    this.failureHandler = options.failureHandler;
    this.sleep = options.sleep ?? defaultSleep;
  }

  start(): void {
    if (this.running) return;
    this.running = true;
    this.schedule(0);
  }

  stop(): void {
    this.running = false;
    if (this.timer !== undefined) clearTimeout(this.timer);
    this.timer = undefined;
  }

  /**
   * Forget the cached cursor so the next pump re-reads it from the queue
   * (`queue.getCursor()`) instead of continuing from wherever this agent last
   * left off. Needed when the underlying queue is reconfigured out from under
   * a running agent (Orleans' generator `IControllable.ExecuteCommand(Configure)`
   * mid-run) — without this the agent keeps polling past the queue's
   * newly-reset committed position and never observes the fresh stream.
   */
  resetCursor(): void {
    this.cursor = undefined;
  }

  private schedule(delayMs: number): void {
    if (!this.running) return;
    if (this.timer !== undefined) clearTimeout(this.timer);
    this.timer = setTimeout(() => {
      this.timer = undefined;
      void this.pump();
    }, delayMs);
  }

  private async pump(): Promise<void> {
    if (this.pumping || !this.running) return;
    this.pumping = true;
    recordAgentPoll();
    try {
      // Resume from the durably committed cursor on first pump (covers a
      // successor that took the queue over from another silo).
      if (this.cursor === undefined) this.cursor = await this.queue.getCursor();
      const entries: QueueEntry[] = await this.queue.readAfter(this.cursor, this.batchSize);
      for (const { token, streamKey, event } of entries) {
        if (!this.running) break;
        const delivered = await this.tryDeliver(streamKey, event, token);
        if (!delivered) break; // transient failure: keep the cursor, redeliver next poll
        this.cursor = token;
        await this.queue.commit(token); // commit only after delivery (at-least-once)
      }
    } finally {
      this.pumping = false;
      if (this.running) this.schedule(this.pollIntervalMs);
    }
  }

  /**
   * Attempts delivery up to `maxAttempts` times with exponential backoff.
   * Returns true if the entry is durably handled (delivered, or skipped after
   * exhausting the retry budget) so the caller advances the cursor; returns
   * false to leave the cursor so the entry is redelivered on the next poll —
   * used when the agent is stopping mid-retry.
   */
  private async tryDeliver(streamKey: string, event: unknown, token: number): Promise<boolean> {
    let lastError: unknown;
    for (let attempt = 1; attempt <= this.maxAttempts; attempt++) {
      if (!this.running) return false;
      try {
        await this.deliver(streamKey, event, token);
        recordStreamDelivered({ "tsva.stream.key": streamKey });
        return true;
      } catch (err) {
        if (err instanceof RecoverableStreamDeliveryError) {
          // The consumer is deactivating to resume from its own persisted
          // checkpoint (see the error's doc) rather than have this event
          // retried in place. Rewind the queue's committed cursor to that
          // checkpoint and forget the cached cursor, so the next poll
          // re-reads from there and redelivers everything the reactivated
          // consumer needs — not just this one event.
          await this.queue.commit(err.resumeToken);
          this.resetCursor();
          return false; // leave the cursor; caller stops this batch, no advance
        }
        lastError = err;
        if (attempt < this.maxAttempts) await this.sleep(this.retryBackoffMs(attempt));
      }
    }
    // Retry budget exhausted: notify the failure handler, then advance past the
    // event so a single poison entry does not stall the rest of the queue.
    recordStreamFailed({ "tsva.stream.key": streamKey });
    try {
      await this.failureHandler?.onDeliveryFailure(
        streamKey,
        event,
        token,
        lastError,
        this.maxAttempts,
      );
    } catch {
      // Swallow handler errors — observability must not itself block delivery.
    }
    return true;
  }
}
