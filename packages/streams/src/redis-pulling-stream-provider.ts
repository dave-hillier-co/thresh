import type { createClient } from "redis";
import { keyToString, type GrainKey } from "@tsva/core/grain-key";
import type { GrainType } from "@tsva/core/grain-type";
import { stableHash32 } from "@tsva/core/hash";
import type {
  ActivationBoundStreamProvider,
  AsyncStream,
  StreamActivationBinding,
  StreamHandler,
  StreamId,
  StreamProducerHandle,
  StreamProvider,
  StreamSubscriptionHandle,
  SubscribeOptions,
} from "@tsva/core/stream";
import { implicitSubscriberIds } from "@tsva/streams/implicit-subscriptions";
import { QueuePullingAgent, type StreamFailureHandler } from "@tsva/streams/queue-pulling-agent";
import { ownedQueueIndices, type HashRange } from "@tsva/streams/queue-ownership";
import { RedisStreamQueue } from "@tsva/streams/redis-stream-queue";
import { RedisSubscriptionRegistry } from "@tsva/streams/redis-subscription-registry";
import type { StreamDeliver } from "@tsva/streams/stream-deliver";
import { StreamProducerRegistry } from "@tsva/streams/stream-producer-registry";
import { StreamProviderConfigurationError } from "@tsva/streams/stream-provider-config-error";

export type RedisClient = ReturnType<typeof createClient>;
export type { StreamDeliver } from "@tsva/streams/stream-deliver";
export type { StreamFailureHandler } from "@tsva/streams/queue-pulling-agent";

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
}

/**
 * Durable, cluster-shared stream provider built on pulling agents. All streams are
 * multiplexed over a fixed set of physical Redis-Stream queues; an agent per
 * queue pulls events and routes each to its stream's subscribers (discovered in
 * the durable registry) via the dispatcher, committing the queue cursor after
 * delivery. Subscriptions and cursors live in Redis, so they survive deactivation
 * and silo failure. Which queues a silo runs agents for is decided by the ring
 * (`startAgentsFor`), set by the host on every membership change.
 */
export class RedisPullingStreamProvider implements ActivationBoundStreamProvider {
  private readonly keyPrefix: string;
  private readonly queueCount: number;
  private readonly pollIntervalMs: number;
  private readonly failureHandler: StreamFailureHandler | undefined;
  private readonly registry: RedisSubscriptionRegistry;
  private readonly queues: RedisStreamQueue[];
  private readonly agents = new Map<number, QueuePullingAgent>();
  private readonly producers = new StreamProducerRegistry();
  private deliver: StreamDeliver = async () => undefined;
  private implicitTypesFor: (namespace: string) => Iterable<GrainType> = () => [];

  constructor(
    client: RedisClient,
    private readonly name = "default",
    options: RedisPullingStreamProviderOptions = {},
  ) {
    if (
      options.queueCount !== undefined &&
      (!Number.isInteger(options.queueCount) || options.queueCount < 1)
    ) {
      throw new StreamProviderConfigurationError(
        name,
        `queueCount must be a positive integer, got ${options.queueCount}`,
      );
    }
    if (
      options.pollIntervalMs !== undefined &&
      (!Number.isFinite(options.pollIntervalMs) || options.pollIntervalMs < 0)
    ) {
      throw new StreamProviderConfigurationError(
        name,
        `pollIntervalMs must be a non-negative number, got ${options.pollIntervalMs}`,
      );
    }
    this.keyPrefix = options.keyPrefix ?? "tsva";
    this.queueCount = options.queueCount ?? 8;
    this.pollIntervalMs = options.pollIntervalMs ?? 50;
    this.failureHandler = options.failureHandler;
    this.registry = new RedisSubscriptionRegistry(client, this.keyPrefix, this.name);
    this.queues = Array.from(
      { length: this.queueCount },
      (_unused, i) => new RedisStreamQueue(client, `${this.keyPrefix}:streamq:${this.name}:${i}`),
    );
  }

  /** Total physical queues; queue ownership is assigned over `[0, physicalQueueCount)`. */
  get physicalQueueCount(): number {
    return this.queueCount;
  }

  /** Wire how a pulled event reaches a subscriber's activation (the cluster node). */
  setDeliver(deliver: StreamDeliver): void {
    this.deliver = deliver;
  }

  /**
   * Wire the implicit-subscription table (`namespace → grain types`). An agent
   * fans each event out to these grain types' matching-key activations as well as
   * to the registry's explicit subscribers (Orleans' `[ImplicitStreamSubscription]`).
   */
  setImplicitSubscribers(typesFor: (namespace: string) => Iterable<GrainType>): void {
    this.implicitTypesFor = typesFor;
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

  /** Run pulling agents for exactly these queue indices (idempotent); stop the rest. */
  startAgentsFor(indices: Iterable<number>): void {
    const wanted = new Set(indices);
    for (const [i, agent] of this.agents) {
      if (!wanted.has(i)) {
        agent.stop();
        this.agents.delete(i);
      }
    }
    for (const i of wanted) {
      if (this.agents.has(i)) continue;
      const agent = new QueuePullingAgent(
        this.queues[i]!,
        (streamKey, event, token) => this.fanOut(streamKey, event, token),
        {
          pollIntervalMs: this.pollIntervalMs,
          ...(this.failureHandler !== undefined ? { failureHandler: this.failureHandler } : {}),
        },
      );
      this.agents.set(i, agent);
      agent.start();
    }
  }

  /** Stop every agent (silo shutdown). Cursors and subscriptions stay in Redis. */
  stop(): void {
    for (const agent of this.agents.values()) agent.stop();
    this.agents.clear();
  }

  getStream<T>(namespace: string, key: GrainKey): AsyncStream<T> {
    return this.streamFor<T>(namespace, key, undefined);
  }

  bindActivation(binding: StreamActivationBinding): StreamProvider {
    return { getStream: <T>(ns: string, key: GrainKey) => this.streamFor<T>(ns, key, binding) };
  }

  /** Orleans' pub-sub `RegisterProducer` — see `StreamProducerHandle`. */
  async registerProducer(namespace: string, key: GrainKey): Promise<StreamProducerHandle> {
    return this.producers.register(this.name, namespace, key);
  }

  private async fanOut(streamKey: string, event: unknown, token: number): Promise<void> {
    // Explicit subscribers (from the durable registry) plus implicit ones (grain
    // types bound to the stream's namespace), deduplicated so a grain that is both
    // gets the event once.
    const explicit = await this.registry.subscribers(streamKey);
    const implicit = implicitSubscriberIds(streamKey, this.implicitTypesFor);
    const seen = new Set<string>();
    for (const subscriber of [...explicit, ...implicit]) {
      const id = subscriber.toString();
      if (seen.has(id)) continue;
      seen.add(id);
      await this.deliver(subscriber, streamKey, event, token);
    }
  }

  private streamFor<T>(
    namespace: string,
    key: GrainKey,
    binding: StreamActivationBinding | undefined,
  ): AsyncStream<T> {
    const keyString = keyToString(key);
    const streamKey = `${namespace}/${keyString}`;
    const queue = this.queues[stableHash32(streamKey) % this.queueCount]!;
    return new PullingStream<T>(
      { provider: this.name, namespace, key: keyString },
      streamKey,
      queue,
      this.registry,
      binding,
    );
  }
}

class PullingStream<T> implements AsyncStream<T> {
  constructor(
    readonly id: StreamId,
    private readonly streamKey: string,
    private readonly queue: RedisStreamQueue,
    private readonly registry: RedisSubscriptionRegistry,
    private readonly binding: StreamActivationBinding | undefined,
  ) {}

  async publish(event: T): Promise<void> {
    await this.queue.append(this.streamKey, event);
  }

  async subscribe(
    handler: StreamHandler<T>,
    _options?: SubscribeOptions,
  ): Promise<StreamSubscriptionHandle<T>> {
    const binding = this.requireBinding();
    binding.setHandler(this.streamKey, handler as StreamHandler<unknown>);
    await this.registry.subscribe(this.streamKey, binding.grainId);
    return new PullingSubscription<T>(this.streamKey, this.registry, binding);
  }

  async getSubscriptions(): Promise<StreamSubscriptionHandle<T>[]> {
    if (this.binding === undefined) return [];
    const subs = await this.registry.subscribers(this.streamKey);
    return subs.some((g) => g.equals(this.binding!.grainId))
      ? [new PullingSubscription<T>(this.streamKey, this.registry, this.binding)]
      : [];
  }

  private requireBinding(): StreamActivationBinding {
    if (this.binding === undefined) {
      throw new Error("subscribing to a pulling stream requires a grain activation");
    }
    return this.binding;
  }
}

class PullingSubscription<T> implements StreamSubscriptionHandle<T> {
  constructor(
    private readonly streamKey: string,
    private readonly registry: RedisSubscriptionRegistry,
    private readonly binding: StreamActivationBinding,
  ) {}

  // Re-bind the handler on (possibly) a fresh activation after reactivation.
  async resume(handler: StreamHandler<T>): Promise<void> {
    this.binding.setHandler(this.streamKey, handler as StreamHandler<unknown>);
  }

  async unsubscribe(): Promise<void> {
    this.binding.clearHandler(this.streamKey);
    await this.registry.unsubscribe(this.streamKey, this.binding.grainId);
  }
}
