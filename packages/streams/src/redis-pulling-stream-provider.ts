import type { createClient } from "redis";
import type { GrainKey } from "@thresh/core/grain-key";
import type { GrainType } from "@thresh/core/grain-type";
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
import { RedisStreamQueue } from "@thresh/streams/redis-stream-queue";
import { RedisSubscriptionRegistry } from "@thresh/streams/redis-subscription-registry";
import type { StreamDeliver } from "@thresh/streams/stream-deliver";

export type RedisClient = ReturnType<typeof createClient>;
export type { StreamDeliver } from "@thresh/streams/stream-deliver";
export type { StreamFailureHandler } from "@thresh/streams/queue-pulling-agent";

export interface RedisPullingStreamProviderOptions {
  keyPrefix?: string;
  /** Number of physical queues streams are multiplexed over (defaults to 8). */
  queueCount?: number;
  pollIntervalMs?: number;
  /**
   * Notified when a pulling agent exhausts its retry budget on a delivery
   * (Orleans `IStreamFailureHandler`), forwarded to every queue's agent this
   * provider starts (`startAgentsFor`). See `DurableStreamFailureHandler` for
   * a store-backed implementation.
   */
  failureHandler?: StreamFailureHandler;
  /**
   * Logical service identity this provider's queues, registry and cursors
   * belong to (Orleans' `ClusterOptions.ServiceId`). Two clusters pointed at
   * the same Redis and provider name stay partitioned by it — the queues
   * carry it too (issue #64), not just the registry/cursors, since sharing
   * the same queue key would cross-deliver events regardless. Defaults to
   * `"default"`; `SiloBuilder` threads the silo's `serviceId ?? DEFAULT_SERVICE_ID`.
   *
   * Redis has no ALTER: a queue/registry/cursor written before this option
   * existed has no `{serviceId}` segment and is invisible to a
   * service-partitioned reader — a deliberate upgrade break, the same call
   * #59 made for `RedisGrainStorage` (see todo.md / docs/deviations.md).
   */
  serviceId?: string;
}

/**
 * Durable, cluster-shared stream provider built on pulling agents. All streams are
 * multiplexed over a fixed set of physical Redis-Stream queues; an agent per
 * queue pulls events and routes each to its stream's subscribers (discovered in
 * the durable registry) via the dispatcher, committing the queue cursor after
 * delivery. Subscriptions and cursors live in Redis, so they survive deactivation
 * and silo failure. Which queues a silo runs agents for is decided by the ring
 * (`startAgentsFor`), set by the host on every membership change.
 *
 * A thin composition over `PullingStreamProviderCore` (see
 * docs/stream-backings-postgres-kafka.md Phase 0): this class only builds the
 * Redis-specific queues and subscription registry and delegates everything
 * else — agent lifecycle, publish→queue selection, fan-out, producer
 * registry, failure-handler forwarding, config validation — to the core.
 */
export class RedisPullingStreamProvider implements ActivationBoundStreamProvider {
  private readonly core: PullingStreamProviderCore;

  constructor(
    client: RedisClient,
    private readonly name = "default",
    options: RedisPullingStreamProviderOptions = {},
  ) {
    const queueCount = validateQueueCount(name, options.queueCount);
    const keyPrefix = options.keyPrefix ?? "thresh";
    const serviceId = options.serviceId ?? DEFAULT_SERVICE_ID;
    const registry = new RedisSubscriptionRegistry(client, keyPrefix, name, serviceId);
    const queues = Array.from(
      { length: queueCount },
      (_unused, i) =>
        new RedisStreamQueue(client, `${keyPrefix}:${serviceId}:streamq:${name}:${i}`),
    );
    this.core = new PullingStreamProviderCore(name, queues, registry, {
      ...(options.pollIntervalMs !== undefined ? { pollIntervalMs: options.pollIntervalMs } : {}),
      ...(options.failureHandler !== undefined ? { failureHandler: options.failureHandler } : {}),
    });
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

  /** Stop every agent (silo shutdown). Cursors and subscriptions stay in Redis. */
  async stop(): Promise<void> {
    await this.core.stop();
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
