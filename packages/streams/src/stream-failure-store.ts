import type { StreamFailureHandler } from "@tsva/streams/queue-pulling-agent";

/** One permanently-failed (poison) delivery, as recorded by `DurableStreamFailureHandler`. */
export interface StreamDeliveryFailure {
  readonly streamKey: string;
  readonly event: unknown;
  readonly token: number;
  readonly error: string;
  readonly attempts: number;
  readonly failedAt: number;
}

/**
 * Durably records poison events a pulling agent gave up delivering — Orleans'
 * `AzureTableStorageStreamFailureHandler`, generalized to any backing store.
 * `DurableStreamFailureHandler` writes to one of these instead of only
 * notifying an in-memory callback, so a poison event's history survives the
 * silo that observed it and can be inspected/replayed later (e.g. an operator
 * dashboard, or a dead-letter reprocessing job — neither built here, this is
 * just the durable record they would read from).
 */
export interface DurableStreamFailureStore {
  recordFailure(failure: StreamDeliveryFailure): Promise<void>;
  /** All recorded failures, optionally narrowed to one stream. */
  listFailures(streamKey?: string): Promise<StreamDeliveryFailure[]>;
}

/** In-memory `DurableStreamFailureStore` — the default for tests and single-process silos. */
export class MemoryStreamFailureStore implements DurableStreamFailureStore {
  private readonly failures: StreamDeliveryFailure[] = [];

  async recordFailure(failure: StreamDeliveryFailure): Promise<void> {
    this.failures.push(failure);
  }

  async listFailures(streamKey?: string): Promise<StreamDeliveryFailure[]> {
    if (streamKey === undefined) return [...this.failures];
    return this.failures.filter((f) => f.streamKey === streamKey);
  }
}

function errorToString(err: unknown): string {
  if (err instanceof Error) return err.message;
  return String(err);
}

/**
 * `StreamFailureHandler` implementation that persists each permanently-failed
 * delivery to a `DurableStreamFailureStore` instead of only observing it
 * transiently — the durable counterpart to a handler that just logs or
 * increments a counter. Wired the same way any other `StreamFailureHandler`
 * is: passed as `RedisPullingStreamProviderOptions.failureHandler` (forwarded
 * to every queue's pulling agent).
 */
export class DurableStreamFailureHandler implements StreamFailureHandler {
  constructor(
    private readonly store: DurableStreamFailureStore,
    private readonly now: () => number = Date.now,
  ) {}

  async onDeliveryFailure(
    streamKey: string,
    event: unknown,
    token: number,
    error: unknown,
    attempts: number,
  ): Promise<void> {
    await this.store.recordFailure({
      streamKey,
      event,
      token,
      error: errorToString(error),
      attempts,
      failedAt: this.now(),
    });
  }
}
