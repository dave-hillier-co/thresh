/**
 * What `KafkaPartitionOwner` needs from the Kafka side to acquire/release a
 * partition — `KafkaTopicQueues.acquire`/`release` in production. Acquire is
 * asynchronous (seeks the consumer, reads the durable cursor store) and can
 * fail (a cursor-store or admin blip); release is synchronous and cannot
 * fail (just pauses the consumer).
 */
export interface PartitionOwnershipClient {
  acquire(idx: number): Promise<void>;
  release(idx: number): void;
}

export interface KafkaPartitionOwnerOptions {
  /** Backoff before retrying a failed acquire (defaults to 2^attempt * 50ms, capped at 5s). */
  retryBackoffMs?: (attempt: number) => number;
  /** Sleep injection — the clock is a true boundary, fake it in tests. */
  sleep?: (ms: number) => Promise<void>;
  /** Notified when an acquire attempt fails (before it's retried). */
  onAcquireError?: (idx: number, error: unknown) => void;
}

const defaultBackoff = (attempt: number): number => Math.min(50 * 2 ** attempt, 5000);
const defaultSleep = (ms: number): Promise<void> => new Promise((r) => setTimeout(r, ms));

/**
 * Drives `PartitionOwnershipClient.acquire`/`release` to track a desired set
 * of partition indices ("wanted"), keeping the client's actual ownership in
 * sync even though `acquire` is asynchronous and a new desired set can arrive
 * while one is still in flight — `KafkaPullingStreamProvider.startAgentsFor`
 * is called again on every membership change, and Kafka's consumer-group
 * acquire (seek + cursor-store read) is not instantaneous.
 *
 * Two failure modes this specifically closes (issue #113):
 *
 * - **Stale acquire**: a membership change drops partition `i` from the
 *   wanted set while its acquire is still in flight. Naively adding `i` to
 *   "owned" once that acquire resolves — regardless of whether it's still
 *   wanted — would start an agent for a partition this silo no longer owns
 *   on the ring, so two silos could deliver it concurrently until the next
 *   membership change. Instead, `wanted` is re-checked against its *current*
 *   value once the acquire settles, not the value captured when it started;
 *   if `i` is no longer wanted, it's released immediately instead of adopted.
 * - **Failed acquire**: a rejected acquire (cursor-store or admin blip) used
 *   to just get logged, leaving the partition ownerless everywhere until the
 *   next membership change. This retries with backoff for as long as the
 *   partition is still wanted.
 */
export class KafkaPartitionOwner {
  private wanted = new Set<number>();
  private readonly owned = new Set<number>();
  private readonly acquiring = new Set<number>();
  private readonly retryBackoffMs: (attempt: number) => number;
  private readonly sleep: (ms: number) => Promise<void>;
  private readonly onAcquireError: (idx: number, error: unknown) => void;
  private stopped = false;

  constructor(
    private readonly client: PartitionOwnershipClient,
    /** Called synchronously whenever the owned set changes, so the caller can start/stop agents. */
    private readonly onOwnedChanged: (owned: ReadonlySet<number>) => void,
    options: KafkaPartitionOwnerOptions = {},
  ) {
    this.retryBackoffMs = options.retryBackoffMs ?? defaultBackoff;
    this.sleep = options.sleep ?? defaultSleep;
    this.onAcquireError =
      options.onAcquireError ??
      ((idx, err) => console.error(`kafka partition owner: failed to acquire queue ${idx}`, err));
  }

  /** The partitions currently owned (acquire has resolved and it's still wanted). */
  currentlyOwned(): ReadonlySet<number> {
    return this.owned;
  }

  /**
   * Adopt this desired set of partitions: release whatever's no longer
   * wanted immediately, and kick off (or leave running) an acquire for
   * everything newly wanted. Idempotent — safe to call again with the same
   * set, or while acquires from a previous call are still in flight.
   */
  setWanted(indices: Iterable<number>): void {
    this.wanted = new Set(indices);

    let changed = false;
    for (const i of this.owned) {
      if (!this.wanted.has(i)) {
        this.owned.delete(i);
        this.client.release(i);
        changed = true;
      }
    }
    if (changed) this.onOwnedChanged(this.owned);

    for (const i of this.wanted) {
      if (!this.owned.has(i) && !this.acquiring.has(i)) this.beginAcquire(i, 0);
    }
  }

  /** Stop acquiring/retrying — no further `client` calls after this settles. Does not release what's owned. */
  stop(): void {
    this.stopped = true;
  }

  private beginAcquire(idx: number, attempt: number): void {
    this.acquiring.add(idx);
    void this.client.acquire(idx).then(
      () => {
        this.acquiring.delete(idx);
        if (this.stopped || !this.wanted.has(idx)) {
          // A membership change dropped `idx` while this acquire was in
          // flight (or the owner stopped) — never adopt ownership of a
          // partition the current desired set doesn't want.
          this.client.release(idx);
          return;
        }
        this.owned.add(idx);
        this.onOwnedChanged(this.owned);
      },
      (err) => {
        this.acquiring.delete(idx);
        this.onAcquireError(idx, err);
        if (this.stopped || !this.wanted.has(idx)) return;
        void this.sleep(this.retryBackoffMs(attempt)).then(() => {
          if (this.stopped || !this.wanted.has(idx) || this.owned.has(idx) || this.acquiring.has(idx)) {
            return;
          }
          this.beginAcquire(idx, attempt + 1);
        });
      },
    );
  }
}
