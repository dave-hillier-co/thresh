import type { Pool } from "pg";
import type { GrainKey } from "@thresh/core/grain-key";
import type { GrainType } from "@thresh/core/grain-type";
import type { Duration } from "@thresh/core/duration";
import { DEFAULT_SERVICE_ID } from "@thresh/core/default-service-id";
import type {
  ActivationBoundStreamProvider,
  AsyncStream,
  StreamActivationBinding,
  StreamProducerHandle,
  StreamProvider,
} from "@thresh/core/stream";
import {
  PullingStreamProviderCore,
  validateQueueCount,
} from "@thresh/streams/pulling-stream-provider-core";
import type { StreamFailureHandler } from "@thresh/streams/queue-pulling-agent";
import type { HashRange } from "@thresh/streams/queue-ownership";
import { PostgresStreamQueue } from "@thresh/streams/postgres-stream-queue";
import { PostgresSubscriptionRegistry } from "@thresh/streams/postgres-subscription-registry";
import type { StreamDeliver } from "@thresh/streams/stream-deliver";

export type { StreamDeliver } from "@thresh/streams/stream-deliver";
export type { StreamFailureHandler } from "@thresh/streams/queue-pulling-agent";

export interface PostgresPullingStreamProviderOptions {
  /** Table prefix the four backing tables share (defaults to `"thresh_stream"`). */
  tablePrefix?: string;
  /** Number of physical queues streams are multiplexed over (defaults to 8). */
  queueCount?: number;
  pollIntervalMs?: number;
  /**
   * Notified when a pulling agent exhausts its retry budget on a delivery
   * (Orleans `IStreamFailureHandler`), forwarded to every queue's agent this
   * provider starts. See `PostgresStreamFailureStore` for a durable store to
   * back it with.
   */
  failureHandler?: StreamFailureHandler;
  /**
   * How long to keep delivered rows around before the opportunistic
   * per-commit trim deletes them (a replay window). Omitted trims eagerly:
   * every row at or below the committed cursor is deleted on the next commit.
   */
  retainFor?: Duration;
  /**
   * Logical service identity this provider's queues, registry and cursors
   * belong to (Orleans' `ClusterOptions.ServiceId`). Two clusters pointed at
   * the same Postgres and provider name stay partitioned by it (issue #64).
   * Defaults to `"default"`; `SiloBuilder` threads the silo's `serviceId ??
   * DEFAULT_SERVICE_ID`.
   */
  serviceId?: string;
}

/**
 * Durable, cluster-shared stream provider built on pulling agents, backed by
 * Postgres instead of Redis. All streams are multiplexed over a fixed set of
 * physical queues sharing `<prefix>_events`/`<prefix>_cursors`
 * (`PostgresStreamQueue`, partitioned by `(provider, queue_idx)`);
 * subscriptions live in `<prefix>_subscriptions`
 * (`PostgresSubscriptionRegistry`). Good for a Postgres-only deployment that
 * would otherwise need Redis solely for streams
 * (docs/stream-backings-postgres-kafka.md Phase 1).
 *
 * A thin composition over `PullingStreamProviderCore` (see Phase 0 of the
 * same doc): this class only builds the Postgres-specific queues and
 * subscription registry and delegates everything else — agent lifecycle,
 * publish→queue selection, fan-out, producer registry, failure-handler
 * forwarding, config validation — to the core. `start()` provisions the
 * backing tables (idempotent, `CREATE TABLE IF NOT EXISTS`); the host runs it
 * before agents start (mirrors `PostgresGrainStorage.start()`).
 */
export class PostgresPullingStreamProvider implements ActivationBoundStreamProvider {
  private readonly core: PullingStreamProviderCore;
  private readonly queues: readonly PostgresStreamQueue[];
  private readonly registry: PostgresSubscriptionRegistry;

  constructor(
    pool: Pool,
    private readonly name = "default",
    options: PostgresPullingStreamProviderOptions = {},
  ) {
    const queueCount = validateQueueCount(name, options.queueCount);
    const tablePrefix = options.tablePrefix ?? "thresh_stream";
    const serviceId = options.serviceId ?? DEFAULT_SERVICE_ID;
    this.registry = new PostgresSubscriptionRegistry(pool, tablePrefix, name, serviceId);
    this.queues = Array.from(
      { length: queueCount },
      (_unused, i) =>
        new PostgresStreamQueue(pool, tablePrefix, name, i, options.retainFor, serviceId),
    );
    this.core = new PullingStreamProviderCore(name, this.queues, this.registry, {
      ...(options.pollIntervalMs !== undefined ? { pollIntervalMs: options.pollIntervalMs } : {}),
      ...(options.failureHandler !== undefined ? { failureHandler: options.failureHandler } : {}),
    });
  }

  /** Idempotently provisions the events/cursors/subscriptions tables. */
  async start(): Promise<void> {
    await Promise.all([...this.queues.map((q) => q.start()), this.registry.start()]);
  }

  /** Total physical queues; queue ownership is assigned over `[0, physicalQueueCount)`. */
  get physicalQueueCount(): number {
    return this.core.physicalQueueCount;
  }

  /** Wire how a pulled event reaches a subscriber's activation (the cluster node). */
  setDeliver(deliver: StreamDeliver): void {
    this.core.setDeliver(deliver);
  }

  /**
   * Wire the implicit-subscription table (`namespace → grain types`). An agent
   * fans each event out to these grain types' matching-key activations as well as
   * to the registry's explicit subscribers (Orleans' `[ImplicitStreamSubscription]`).
   */
  setImplicitSubscribers(typesFor: (namespace: string) => Iterable<GrainType>): void {
    this.core.setImplicitSubscribers(typesFor);
  }

  /**
   * Adopt the hash ranges this silo owns on the ring: run agents for exactly the
   * queues whose ring point falls in them, stopping any no longer owned. Called by
   * the host on start and on every membership change — mirrors how reminders take
   * over hash ranges, so a queue (and its committed cursor) hands off losslessly.
   */
  refreshOwnership(ranges: readonly HashRange[]): void {
    this.core.refreshOwnership(ranges);
  }

  /** Run pulling agents for exactly these queue indices (idempotent); stop the rest. */
  startAgentsFor(indices: Iterable<number>): void {
    this.core.startAgentsFor(indices);
  }

  /** Stop every agent (silo shutdown). Cursors and subscriptions stay in Postgres. */
  stop(): void {
    this.core.stop();
  }

  getStream<T>(namespace: string, key: GrainKey): AsyncStream<T> {
    return this.core.getStream<T>(namespace, key);
  }

  bindActivation(binding: StreamActivationBinding): StreamProvider {
    return this.core.bindActivation(binding);
  }

  /** Orleans' pub-sub `RegisterProducer` — see `StreamProducerHandle`. */
  async registerProducer(namespace: string, key: GrainKey): Promise<StreamProducerHandle> {
    return this.core.registerProducer(namespace, key);
  }
}
