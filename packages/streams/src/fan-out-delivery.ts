import { GrainCallAbortedError, GrainCallTimeoutError } from "@thresh/core/errors";
import type { GrainId } from "@thresh/core/grain-id";
import { systemTimeProvider, type TimeProvider } from "@thresh/core/time-provider";
import { executeWithRetries, INFINITE_RETRIES } from "@thresh/core/async-executor-with-retries";
import { recordStreamDelivered, recordStreamFailed } from "@thresh/observability/stream-metrics";
import type { StreamFailureHandler } from "@thresh/streams/queue-pulling-agent";
import type { StreamDeliver } from "@thresh/streams/stream-deliver";
import { RecoverableStreamDeliveryError } from "@thresh/streams/stream-recovery";

/** Orleans `StreamPullingAgentOptions.DEFAULT_MAX_EVENT_DELIVERY_TIME` (1 minute). */
const DEFAULT_MAX_EVENT_DELIVERY_TIME_MS = 60_000;
/** Orleans `MessagingOptions.ResponseTimeout` (30 seconds). */
const DEFAULT_DELIVERY_RESPONSE_TIMEOUT_MS = 30_000;
const defaultBackoff = (attempt: number): number => Math.min(50 * 2 ** attempt, 5000);

export interface FanOutDeliveryOptions {
  /**
   * Total time (ms) to keep retrying delivery to ONE subscriber before giving
   * up on it and moving on (Orleans `MaxEventDeliveryTime`; default 1
   * minute). Every other subscriber of the same event is retried and timed
   * out independently — see {@link FanOutDelivery.deliverToAll}.
   */
  maxEventDeliveryTimeMs?: number;
  /**
   * Bounds a single delivery attempt to one subscriber (Orleans
   * `ResponseTimeout`; default 30s). A subscriber whose `onNext` hangs past
   * this is treated as a failed attempt and retried (or skipped once
   * `maxEventDeliveryTimeMs` elapses) instead of blocking the caller — a
   * queue-pulling agent's pump, and therefore its `stop()` — forever.
   */
  deliveryResponseTimeoutMs?: number;
  /** Backoff between retries to the same subscriber; defaults to 2^attempt * 50ms, capped at 5s. */
  retryBackoffMs?: (attempt: number) => number;
  /** Notified once a subscriber's retry budget is exhausted (Orleans `IStreamFailureHandler`). */
  failureHandler?: StreamFailureHandler;
  /** Clock driving delivery deadlines and backoff sleeps — a true boundary, fake it in tests. */
  time?: TimeProvider;
}

/**
 * Fans one pulled event out to every subscriber concurrently and retries each
 * one independently — the mechanism `QueuePullingAgent`'s pump delegates to
 * so a poison or slow subscriber only ever costs that one subscriber, never
 * the others sharing the same physical queue (issue #97; Orleans keeps a
 * cursor per consumer, `PersistentStreamPullingAgent.RunConsumerCursor`,
 * `ErrorProtocol` faulting/skipping only the failing one).
 *
 * `deliverToAll` never rejects for an ordinary delivery failure — a
 * subscriber that keeps failing past `maxEventDeliveryTimeMs` (Orleans
 * `ExecuteWithRetries` bounded by `MaxEventDeliveryTime`, default 1 minute,
 * issue #98) is reported to `failureHandler` and simply skipped, leaving
 * every other subscriber's own delivery/retry untouched. Each attempt is
 * itself bounded by `deliveryResponseTimeoutMs` (Orleans `ResponseTimeout`,
 * default 30s, issue #111) so a hung `onNext` cannot stall this subscriber's
 * retry loop, and therefore the caller awaiting `deliverToAll`, forever.
 *
 * The one exception is `RecoverableStreamDeliveryError`: it is never retried
 * (the consumer is deactivating to resume from its own checkpoint instead —
 * see the error's doc) and is rethrown once every subscriber has settled, so
 * the caller (the queue-owning agent) can rewind its cursor rather than
 * commit past this entry. When several subscribers ask to rewind, the
 * earliest `resumeToken` wins so no subscriber's checkpoint is skipped.
 *
 * `signal` is the owning agent's shutdown (Orleans `IsShutdown` in
 * `RunConsumerCursor`'s retry filter): once it aborts, no further attempt is
 * made, an in-flight attempt or backoff is abandoned without waiting for it,
 * and `deliverToAll` rejects with `GrainCallAbortedError` — never reporting a
 * skip — so the agent leaves the cursor for the next owner instead of
 * committing past an event that was not actually delivered.
 */
export class FanOutDelivery {
  private readonly maxEventDeliveryTimeMs: number;
  private readonly deliveryResponseTimeoutMs: number;
  private readonly retryBackoffMs: (attempt: number) => number;
  private readonly failureHandler: StreamFailureHandler | undefined;
  private readonly time: TimeProvider;

  constructor(
    private readonly deliver: StreamDeliver,
    options: FanOutDeliveryOptions = {},
  ) {
    this.maxEventDeliveryTimeMs =
      options.maxEventDeliveryTimeMs ?? DEFAULT_MAX_EVENT_DELIVERY_TIME_MS;
    this.deliveryResponseTimeoutMs =
      options.deliveryResponseTimeoutMs ?? DEFAULT_DELIVERY_RESPONSE_TIMEOUT_MS;
    this.retryBackoffMs = options.retryBackoffMs ?? defaultBackoff;
    this.failureHandler = options.failureHandler;
    this.time = options.time ?? systemTimeProvider;
  }

  /**
   * Deliver `event`/`token` to every subscriber, `Promise.allSettled` so one
   * subscriber's rejection or retry loop never delays or blocks delivery to
   * the rest (mirrors `MemoryStreamProvider.fanOut`'s `ResumeAfterSlowSubscriber`
   * reasoning).
   */
  async deliverToAll(
    subscribers: readonly GrainId[],
    streamKey: string,
    event: unknown,
    token: number,
    signal?: AbortSignal,
  ): Promise<void> {
    const results = await Promise.allSettled(
      subscribers.map((subscriber) =>
        this.deliverToOne(subscriber, streamKey, event, token, signal),
      ),
    );
    if (signal?.aborted === true) throw new GrainCallAbortedError();
    let earliest: RecoverableStreamDeliveryError | undefined;
    for (const r of results) {
      if (r.status !== "rejected" || !(r.reason instanceof RecoverableStreamDeliveryError))
        continue;
      if (earliest === undefined || r.reason.resumeToken < earliest.resumeToken)
        earliest = r.reason;
    }
    if (earliest !== undefined) throw earliest;
  }

  /** Retries one subscriber's delivery until it succeeds or its own deadline elapses. */
  private async deliverToOne(
    subscriber: GrainId,
    streamKey: string,
    event: unknown,
    token: number,
    signal: AbortSignal | undefined,
  ): Promise<void> {
    const deadline = this.time.now() + this.maxEventDeliveryTimeMs;
    let attemptsMade = 0;
    try {
      await executeWithRetries(
        async () => {
          attemptsMade++;
          await this.attempt(subscriber, streamKey, event, token, signal);
        },
        {
          maxRetries: INFINITE_RETRIES,
          backoff: (attempt) => this.retryBackoffMs(attempt + 1),
          timeProvider: abortableTimeProvider(this.time, signal),
          shouldRetry: (_attempt, outcome) =>
            outcome.kind === "error" &&
            !(outcome.error instanceof RecoverableStreamDeliveryError) &&
            signal?.aborted !== true &&
            this.time.now() < deadline,
        },
      );
      recordStreamDelivered({ "thresh.stream.key": streamKey });
    } catch (err) {
      if (err instanceof RecoverableStreamDeliveryError) throw err;
      // Shutting down: not a delivery failure, so no skip is reported.
      if (signal?.aborted === true) throw new GrainCallAbortedError();
      // Retry budget exhausted: notify the failure handler and return
      // normally — only THIS subscriber is skipped, not the whole event.
      recordStreamFailed({ "thresh.stream.key": streamKey });
      try {
        await this.failureHandler?.onDeliveryFailure(streamKey, event, token, err, attemptsMade);
      } catch {
        // Swallow handler errors — observability must not itself block delivery.
      }
    }
  }

  /**
   * Races one delivery attempt against `deliveryResponseTimeoutMs`. The
   * callee is never interrupted — JS has no cooperative cancellation for an
   * in-flight turn — so `deliver` may still be running when the deadline
   * fires; its eventual settlement is swallowed once the deadline has
   * already decided the outcome (same trade as `GrainFactory.raceResponseDeadline`).
   */
  private attempt(
    subscriber: GrainId,
    streamKey: string,
    event: unknown,
    token: number,
    signal: AbortSignal | undefined,
  ): Promise<void> {
    if (signal?.aborted === true) return Promise.reject(new GrainCallAbortedError());
    const call = this.deliver(subscriber, streamKey, event, token);
    return new Promise<void>((resolve, reject) => {
      let settled = false;
      const finish = (): boolean => {
        if (settled) return false;
        settled = true;
        this.time.clearTimer(timer);
        signal?.removeEventListener("abort", onAbort);
        return true;
      };
      const abandon = (error: Error): void => {
        if (!finish()) return;
        call.catch(() => {});
        reject(error);
      };
      const onAbort = (): void => abandon(new GrainCallAbortedError());
      const timer = this.time.setTimer(
        () =>
          abandon(
            new GrainCallTimeoutError(
              `stream delivery to ${subscriber.toString()} exceeded its ${this.deliveryResponseTimeoutMs}ms response deadline`,
            ),
          ),
        this.deliveryResponseTimeoutMs,
      );
      signal?.addEventListener("abort", onAbort, { once: true });
      call.then(
        () => {
          if (finish()) resolve();
        },
        (error: unknown) => {
          if (finish()) reject(error);
        },
      );
    });
  }
}

/**
 * A view of `time` whose timers fire early (and are cleared) when `signal`
 * aborts, so `executeWithRetries`' backoff sleep observes shutdown the way
 * Orleans' `Task.Delay(d, ct)` does instead of leaving an unkillable wait
 * (docs/orleans-to-thresh-port.md, "Concurrency and cancellation").
 */
function abortableTimeProvider(time: TimeProvider, signal: AbortSignal | undefined): TimeProvider {
  if (signal === undefined) return time;
  return {
    now: () => time.now(),
    setTimer: (handler, delayMs) => {
      let fired = false;
      const fire = (): void => {
        if (fired) return;
        fired = true;
        time.clearTimer(handle);
        signal.removeEventListener("abort", fire);
        handler();
      };
      const handle = time.setTimer(fire, delayMs);
      if (signal.aborted) fire();
      else signal.addEventListener("abort", fire, { once: true });
      return handle;
    },
    clearTimer: (handle) => time.clearTimer(handle),
  };
}
