import type { Kafka } from "kafkajs";
import type { GrainKey } from "@thresh/core/grain-key";
import type { GrainType } from "@thresh/core/grain-type";
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
  type SubscriptionRegistry,
} from "@thresh/streams/pulling-stream-provider-core";
import type { StreamFailureHandler } from "@thresh/streams/queue-pulling-agent";
import { ownedQueueIndices, type HashRange } from "@thresh/streams/queue-ownership";
import { KafkaStreamQueue, KafkaTopicQueues } from "@thresh/streams/kafka-stream-queue";
import type { StreamCursorStore } from "@thresh/streams/stream-cursor-store";
import type { StreamDeliver } from "@thresh/streams/stream-deliver";
import { StreamProviderConfigurationError } from "@thresh/streams/stream-provider-config-error";

export type { StreamDeliver } from "@thresh/streams/stream-deliver";
export type { StreamFailureHandler } from "@thresh/streams/queue-pulling-agent";

/** Anything the Kafka provider provisions lazily on `start()` — Postgres-backed metadata does. */
interface Startable {
  start?: () => Promise<void>;
}

export interface KafkaPullingStreamProviderOptions {
  /** Prefix for the provider's topic, `<prefix>.<name>` (defaults to `"thresh.streams"`). */
  topicPrefix?: string;
  /**
   * Number of physical queues streams are multiplexed over (defaults to 8);
   * must equal the topic's partition count, checked on `start()`.
   */
  queueCount?: number;
  pollIntervalMs?: number;
  /**
   * Durable subscription registry — cursors, subscriptions and failures live
   * in a metadata store rather than Kafka itself
   * (docs/stream-backings-postgres-kafka.md Phase 2), so a Postgres or Redis
   * implementation is supplied here (`PostgresSubscriptionRegistry` /
   * `RedisSubscriptionRegistry`).
   */
  registry: SubscriptionRegistry;
  /** Durable cursor store backing every queue's committed offset (see `registry`). */
  cursorStore: StreamCursorStore;
  /**
   * Notified when a pulling agent exhausts its retry budget on a delivery,
   * and on the retention-gap edge case (a partition's committed cursor fell
   * below its earliest available offset).
   */
  failureHandler?: StreamFailureHandler;
  /** In-memory per-partition buffer size before the partition is paused (defaults to 1000). */
  highWaterMark?: number;
}

/**
 * Durable, cluster-shared stream provider built on pulling agents, backed by
 * a Kafka topic instead of Redis/Postgres storage
 * (docs/stream-backings-postgres-kafka.md Phase 2). One topic per provider
 * (`<topicPrefix>.<name>`); one partition per physical queue — `queueCount`
 * must equal the topic's partition count, validated on `start()` via the
 * admin API (topic auto-creation is not assumed; mismatch or a missing topic
 * throws `StreamProviderConfigurationError`).
 *
 * The ring stays in charge of queue ownership, not Kafka consumer groups
 * (see `KafkaTopicQueues`'s doc comment for why): `startAgentsFor` first
 * acquires/releases the corresponding Kafka partitions — seeking a newly
 * acquired partition to its durable cursor — and only then delegates to
 * `PullingStreamProviderCore` (Phase 0) to start/stop the pulling agents
 * themselves, exactly mirroring `RedisPullingStreamProvider` /
 * `PostgresPullingStreamProvider` otherwise.
 */
export class KafkaPullingStreamProvider implements ActivationBoundStreamProvider {
  private readonly core: PullingStreamProviderCore;
  private readonly client: KafkaTopicQueues;
  private readonly registry: SubscriptionRegistry;
  private readonly cursorStore: StreamCursorStore;
  private readonly queueCount: number;
  readonly topic: string;
  private owned = new Set<number>();

  constructor(
    kafka: Kafka,
    private readonly name = "default",
    options: KafkaPullingStreamProviderOptions,
  ) {
    this.queueCount = validateQueueCount(name, options.queueCount);
    const topicPrefix = options.topicPrefix ?? "thresh.streams";
    this.topic = `${topicPrefix}.${name}`;
    this.registry = options.registry;
    this.cursorStore = options.cursorStore;
    this.client = new KafkaTopicQueues(kafka, this.topic, name, this.cursorStore, {
      ...(options.highWaterMark !== undefined ? { highWaterMark: options.highWaterMark } : {}),
      ...(options.failureHandler !== undefined ? { failureHandler: options.failureHandler } : {}),
    });
    const queues = Array.from(
      { length: this.queueCount },
      (_unused, i) => new KafkaStreamQueue(this.client, i),
    );
    this.core = new PullingStreamProviderCore(name, queues, this.registry, {
      ...(options.pollIntervalMs !== undefined ? { pollIntervalMs: options.pollIntervalMs } : {}),
      ...(options.failureHandler !== undefined ? { failureHandler: options.failureHandler } : {}),
    });
  }

  /**
   * Provisions the metadata store (if it needs it, e.g. Postgres tables),
   * then validates the topic exists with exactly `queueCount` partitions,
   * connects the producer/consumer and starts consuming (paused, until
   * ownership is assigned via `startAgentsFor`/`refreshOwnership`). A
   * validation failure is surfaced as `StreamProviderConfigurationError`.
   */
  async start(): Promise<void> {
    await Promise.all([
      (this.registry as Startable).start?.(),
      (this.cursorStore as Startable).start?.(),
    ]);
    try {
      await this.client.start(this.queueCount);
    } catch (err) {
      throw new StreamProviderConfigurationError(this.name, (err as Error).message);
    }
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
    this.startAgentsFor(ownedQueueIndices(this.name, this.queueCount, ranges));
  }

  /**
   * Run pulling agents for exactly these queue indices (idempotent); stop the
   * rest. Releases the corresponding Kafka partitions immediately (pausing
   * them); newly wanted partitions are acquired (seek to durable cursor +
   * resume) asynchronously, and their agents only start once that completes —
   * a later successor re-seeks from the durably committed cursor, so handoff
   * loses nothing.
   */
  startAgentsFor(indices: Iterable<number>): void {
    const wanted = new Set(indices);

    for (const i of this.owned) {
      if (!wanted.has(i)) {
        this.owned.delete(i);
        this.client.release(i);
      }
    }
    this.core.startAgentsFor(this.owned);

    const toAcquire = [...wanted].filter((i) => !this.owned.has(i));
    if (toAcquire.length === 0) return;
    void Promise.all(toAcquire.map((i) => this.client.acquire(i).then(() => this.owned.add(i))))
      .catch((err) => {
        console.error(`kafka stream provider ${this.name}: failed to acquire queue`, err);
      })
      .finally(() => {
        this.core.startAgentsFor(this.owned);
      });
  }

  /** Stop every agent (silo shutdown). Cursors and subscriptions stay in the metadata store. */
  stop(): void {
    this.core.stop();
  }

  /** Disconnects the Kafka producer/consumer/admin client. Call after `stop()` on shutdown. */
  async disconnect(): Promise<void> {
    await this.client.stop();
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
