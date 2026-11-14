import type { GrainType } from "@thresh/core/grain-type";
import type { Logger } from "@thresh/core/logger";
import {
  systemTimeProvider,
  type TimeProvider,
  type TimerHandle,
} from "@thresh/core/time-provider";
import { recordAgentPoll } from "@thresh/observability/stream-metrics";
import type { QueueEntry } from "@thresh/streams/redis-stream-queue";
import type { HashRange } from "@thresh/streams/queue-ownership";
import type { StreamDeliver } from "@thresh/streams/stream-deliver";
import { RecoverableStreamDeliveryError } from "@thresh/streams/stream-recovery";

/**
 * Delivers one pulled event to the stream's subscribers; the agent supplies it.
 * `signal` aborts when the agent stops, so a retrying or hung delivery is
 * abandoned instead of holding `stop()` (and silo shutdown) hostage.
 */
export type DeliverEvent = (
  streamKey: string,
  event: unknown,
  token: number,
  signal: AbortSignal,
) => Promise<void>;

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
  /**
   * Advance the committed cursor after at-least-once delivery. Backings make
   * this monotonic (only ever advances) so a stale commit racing in from a
   * de-owned pulling agent during ownership handoff cannot rewind a newer
   * commit already made by the new owner and cause a whole batch to be
   * redelivered.
   */
  commit(cursor: number): Promise<void>;
  /**
   * Unconditionally set the committed cursor, bypassing `commit`'s monotonic
   * guard. Used only for an intentional rewind to an earlier checkpoint
   * (`RecoverableStreamDeliveryError`) — never for ordinary at-least-once
   * advancement, which must go through `commit`.
   */
  seek(cursor: number): Promise<void>;
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
 * Called once one subscriber's own retry budget is exhausted (`FanOutDelivery`,
 * `maxEventDeliveryTimeMs`) and that subscriber alone is about to be skipped;
 * every other subscriber of the same event keeps its own retry/delivery
 * unaffected.
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
  /**
   * Notified when a pump fails outside delivery (cursor read, queue read,
   * commit), in addition to the logger. The agent keeps polling — the next poll
   * retries from the committed cursor, after a backoff.
   */
  onPumpError?: (error: unknown) => void;
  /**
   * Where a poll failure is reported, once per failure. Defaults to a logger
   * that writes errors to the console, so a provider that is not handed the
   * silo's logger still surfaces a store outage.
   */
  logger?: Logger;
  /** The clock the poll wait is taken from (defaults to the system clock). */
  time?: TimeProvider;
  /**
   * The longest wait between polls while the store keeps failing (defaults to
   * 30s). Consecutive failures double the wait from `pollIntervalMs` up to this;
   * the first success returns it to `pollIntervalMs`.
   */
  maxBackoffMs?: number;
  /** A source of [0, 1) for the backoff jitter (defaults to `Math.random`). */
  random?: () => number;
}

const consoleLogger: Logger = {
  debug() {},
  info() {},
  warn() {},
  error: (message, fields) => console.error(message, fields),
};

/**
 * Pulls one physical queue and delivers each entry to its subscribers, then
 * commits the queue cursor — so delivery is at-least-once and a successor agent
 * (after a membership change) resumes from the committed position with no gaps.
 * Mirrors an Orleans persistent-stream pulling agent. One agent runs per queue a
 * silo owns; `PullingAgentManager` (a later slice) starts/stops them as ring
 * ownership changes.
 *
 * Retry-then-skip on a delivery failure is the `deliver` callback's own job
 * (`PullingStreamProviderCore.fanOut` delegates it to `FanOutDelivery`, which
 * retries and skips each subscriber independently — issues #97/#98/#111):
 * this agent's own retry is unbounded and driven entirely by its poll loop
 * (`this.pollIntervalMs`), matching how a queue-read or commit failure is
 * already handled below — the whole entry (e.g. a `registry.subscribers()`
 * read failing) is simply retried on the next poll, with the cursor left
 * uncommitted, until `deliver` stops throwing.
 */
export class QueuePullingAgent {
  private readonly pollIntervalMs: number;
  private readonly batchSize: number;
  private readonly onPumpError: ((error: unknown) => void) | undefined;
  private readonly logger: Logger;
  private readonly time: TimeProvider;
  private readonly maxBackoffMs: number;
  private readonly random: () => number;
  private consecutiveFailures = 0;
  private timer: TimerHandle | undefined;
  private cursor: number | undefined;
  private pumping = false;
  private running = false;
  private inflight: Promise<void> | undefined;
  private shutdown = new AbortController();

  constructor(
    private readonly queue: PullableQueue,
    private readonly deliver: DeliverEvent,
    options: QueuePullingAgentOptions = {},
  ) {
    this.pollIntervalMs = options.pollIntervalMs ?? 50;
    this.batchSize = options.batchSize ?? 128;
    this.onPumpError = options.onPumpError;
    this.logger = options.logger ?? consoleLogger;
    this.time = options.time ?? systemTimeProvider;
    this.maxBackoffMs = Math.max(this.pollIntervalMs, options.maxBackoffMs ?? 30_000);
    this.random = options.random ?? Math.random;
  }

  start(): void {
    if (this.running) return;
    this.running = true;
    if (this.shutdown.signal.aborted) this.shutdown = new AbortController();
    this.schedule(0);
  }

  /**
   * Resolves once any in-flight pump has settled, so the caller can safely
   * close the queue's backing client. Aborts the delivery signal first, so a
   * subscriber still being retried, or a hung `onNext`, is abandoned (cursor
   * left uncommitted) rather than waited out — Orleans' `IsShutdown` retry filter.
   */
  async stop(): Promise<void> {
    this.running = false;
    this.shutdown.abort();
    if (this.timer !== undefined) this.time.clearTimer(this.timer);
    this.timer = undefined;
    await this.inflight;
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
    if (this.timer !== undefined) this.time.clearTimer(this.timer);
    this.timer = this.time.setTimer(() => {
      this.timer = undefined;
      this.inflight = this.pump();
    }, delayMs);
  }

  private async pump(): Promise<void> {
    if (this.pumping || !this.running) return;
    this.pumping = true;
    recordAgentPoll();
    let failed = false;
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
    } catch (err) {
      // A failed read/commit must not kill the poll loop (or escape as an
      // unhandled rejection); the next poll retries from the committed cursor.
      failed = true;
      this.consecutiveFailures += 1;
      this.logger.error("stream pump failed", {
        error: err,
        consecutiveFailures: this.consecutiveFailures,
      });
      this.onPumpError?.(err);
    } finally {
      this.pumping = false;
      if (!failed) this.consecutiveFailures = 0;
      if (this.running) this.schedule(failed ? this.backoffDelayMs() : this.pollIntervalMs);
    }
  }

  /**
   * The wait after the latest of N consecutive failures: `pollIntervalMs`
   * doubled per failure up to `maxBackoffMs`, with the upper half of each step
   * jittered so agents that failed together do not retry together.
   */
  private backoffDelayMs(): number {
    const exponent = Math.min(this.consecutiveFailures - 1, 30);
    const ceiling = Math.min(this.maxBackoffMs, this.pollIntervalMs * 2 ** exponent);
    return Math.round(ceiling / 2 + (ceiling / 2) * this.random());
  }

  /**
   * Attempts delivery once. `deliver` (`PullingStreamProviderCore.fanOut`)
   * never rejects for an ordinary subscriber failure — it retries and skips
   * each subscriber on its own (`FanOutDelivery`) — so a rejection here means
   * either a queue-wide failure (e.g. the subscriber registry read itself
   * failed) or a `RecoverableStreamDeliveryError`. Returns true once the
   * entry is durably handled so the caller advances the cursor; returns
   * false to leave the cursor so the entry is redelivered on the next poll.
   */
  private async tryDeliver(streamKey: string, event: unknown, token: number): Promise<boolean> {
    try {
      await this.deliver(streamKey, event, token, this.shutdown.signal);
      return true;
    } catch (err) {
      // Stopped mid-delivery: leave the cursor for the next owner.
      if (!this.running) return false;
      if (err instanceof RecoverableStreamDeliveryError) {
        // The consumer is deactivating to resume from its own persisted
        // checkpoint (see the error's doc) rather than have this event
        // retried in place. Rewind the queue's committed cursor to that
        // checkpoint and forget the cached cursor, so the next poll
        // re-reads from there and redelivers everything the reactivated
        // consumer needs — not just this one event.
        await this.queue.seek(err.resumeToken);
        this.resetCursor();
        return false; // leave the cursor; caller stops this batch, no advance
      }
      // A queue-wide failure (not a subscriber's): leave the cursor and
      // propagate to `pump()`'s catch, whose next scheduled poll retries this
      // same entry — unbounded, the same way a queue read/commit failure is
      // already handled, since it isn't one poison subscriber to skip.
      throw err;
    }
  }
}
