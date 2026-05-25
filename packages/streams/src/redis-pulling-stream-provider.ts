import type { createClient } from "redis";
import type { GrainId } from "@tsva/core/grain-id";
import { keyToString, type GrainKey } from "@tsva/core/grain-key";
import { stableHash32 } from "@tsva/core/hash";
import type {
  ActivationBoundStreamProvider,
  AsyncStream,
  StreamActivationBinding,
  StreamHandler,
  StreamId,
  StreamProvider,
  StreamSubscriptionHandle,
  SubscribeOptions,
} from "@tsva/core/stream";
import { QueuePullingAgent } from "@tsva/streams/queue-pulling-agent";
import { RedisStreamQueue } from "@tsva/streams/redis-stream-queue";
import { RedisSubscriptionRegistry } from "@tsva/streams/redis-subscription-registry";

export type RedisClient = ReturnType<typeof createClient>;

/** Delivers one event to a subscriber's activation; wired to `node.deliverStreamEvent`. */
export type StreamDeliver = (
  subscriber: GrainId,
  streamKey: string,
  event: unknown,
  token: number,
) => Promise<void>;

export interface RedisPullingStreamProviderOptions {
  keyPrefix?: string;
  /** Number of physical queues streams are multiplexed over (defaults to 8). */
  queueCount?: number;
  pollIntervalMs?: number;
}

/**
 * Durable, cluster-shared stream provider built on pulling agents
 * ([ADR 0007](../../docs/adr/0007-stream-pulling-agents.md)). All streams are
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
  private readonly registry: RedisSubscriptionRegistry;
  private readonly queues: RedisStreamQueue[];
  private readonly agents = new Map<number, QueuePullingAgent>();
  private deliver: StreamDeliver = async () => undefined;

  constructor(
    client: RedisClient,
    private readonly name = "default",
    options: RedisPullingStreamProviderOptions = {},
  ) {
    this.keyPrefix = options.keyPrefix ?? "tsva";
    this.queueCount = options.queueCount ?? 8;
    this.pollIntervalMs = options.pollIntervalMs ?? 50;
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
        { pollIntervalMs: this.pollIntervalMs },
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

  private async fanOut(streamKey: string, event: unknown, token: number): Promise<void> {
    for (const subscriber of await this.registry.subscribers(streamKey)) {
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

  async getSubscriptions(_consumerId?: string): Promise<StreamSubscriptionHandle<T>[]> {
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
