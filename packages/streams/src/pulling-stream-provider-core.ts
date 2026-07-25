import { keyToString, type GrainKey } from "@tsva/core/grain-key";
import type { GrainId } from "@tsva/core/grain-id";
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
import {
  QueuePullingAgent,
  type PullableQueue,
  type StreamFailureHandler,
} from "@tsva/streams/queue-pulling-agent";
import { ownedQueueIndices, type HashRange } from "@tsva/streams/queue-ownership";
import type { StreamDeliver } from "@tsva/streams/stream-deliver";
import { StreamProducerRegistry } from "@tsva/streams/stream-producer-registry";
import { StreamProviderConfigurationError } from "@tsva/streams/stream-provider-config-error";

/**
 * The shape a physical queue must offer to be multiplexed by
 * `PullingStreamProviderCore`: `PullableQueue`'s pull contract plus `append`
 * for publish. `RedisStreamQueue` is the durable implementation; a Postgres
 * or Kafka backing (docs/stream-backings-postgres-kafka.md) supplies its own
 * queue implementing this same interface.
 */
export interface AppendableQueue extends PullableQueue {
  /** Append an event for `streamKey`; returns its queue token. */
  append(streamKey: string, event: unknown): Promise<number>;
}

/**
 * Durable explicit-subscription surface a pulling stream provider needs —
 * extracted from `RedisSubscriptionRegistry`'s public API so a backing can
 * supply any store (e.g. Postgres) implementing the same shape.
 */
export interface SubscriptionRegistry {
  subscribe(streamKey: string, subscriber: GrainId): Promise<void>;
  unsubscribe(streamKey: string, subscriber: GrainId): Promise<void>;
  subscribers(streamKey: string): Promise<GrainId[]>;
}

export interface PullingStreamProviderCoreOptions {
  pollIntervalMs?: number;
  /**
   * Notified when a pulling agent exhausts its retry budget on a delivery
   * (Orleans `IStreamFailureHandler`), forwarded to every queue's agent this
   * provider starts.
   */
  failureHandler?: StreamFailureHandler;
}

/** Validates `queueCount` (Orleans-style fail-fast config check); defaults to 8. */
export function validateQueueCount(providerName: string, queueCount: number | undefined): number {
  if (queueCount !== undefined && (!Number.isInteger(queueCount) || queueCount < 1)) {
    throw new StreamProviderConfigurationError(
      providerName,
      `queueCount must be a positive integer, got ${queueCount}`,
    );
  }
  return queueCount ?? 8;
}

/** Validates `pollIntervalMs`; defaults to 50ms. */
export function validatePollIntervalMs(
  providerName: string,
  pollIntervalMs: number | undefined,
): number {
  if (
    pollIntervalMs !== undefined &&
    (!Number.isFinite(pollIntervalMs) || pollIntervalMs < 0)
  ) {
    throw new StreamProviderConfigurationError(
      providerName,
      `pollIntervalMs must be a non-negative number, got ${pollIntervalMs}`,
    );
  }
  return pollIntervalMs ?? 50;
}

/**
 * Backend-neutral core of a pulling-agent-backed stream provider: everything
 * `RedisPullingStreamProvider` did except constructing its Redis-specific
 * queues and registry. Agent start/stop per owned queue index,
 * publish→queue selection (`stableHash32(streamKey) % queueCount`), fan-out
 * to the durable registry plus implicit subscribers, producer registration,
 * failure-handler forwarding and config validation all live here — mirrors
 * Orleans' `PersistentStreamProvider` sitting on top of `IQueueAdapter`. A
 * backing (Redis, Postgres, Kafka, …) supplies `{ queues, registry }`
 * implementing `AppendableQueue`/`SubscriptionRegistry` and composes this
 * core as a thin wrapper — see `RedisPullingStreamProvider`.
 */
export class PullingStreamProviderCore implements ActivationBoundStreamProvider {
  private readonly pollIntervalMs: number;
  private readonly failureHandler: StreamFailureHandler | undefined;
  private readonly agents = new Map<number, QueuePullingAgent>();
  private readonly producers = new StreamProducerRegistry();
  private deliver: StreamDeliver = async () => undefined;
  private implicitTypesFor: (namespace: string) => Iterable<GrainType> = () => [];

  constructor(
    private readonly name: string,
    private readonly queues: readonly AppendableQueue[],
    private readonly registry: SubscriptionRegistry,
    options: PullingStreamProviderCoreOptions = {},
  ) {
    this.pollIntervalMs = validatePollIntervalMs(name, options.pollIntervalMs);
    this.failureHandler = options.failureHandler;
  }

  /** Total physical queues; queue ownership is assigned over `[0, physicalQueueCount)`. */
  get physicalQueueCount(): number {
    return this.queues.length;
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
    this.startAgentsFor(ownedQueueIndices(this.name, this.queues.length, ranges));
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

  /** Stop every agent (silo shutdown). Cursors and subscriptions stay in the backing store. */
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
    const queue = this.queues[stableHash32(streamKey) % this.queues.length]!;
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
    private readonly queue: AppendableQueue,
    private readonly registry: SubscriptionRegistry,
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
    private readonly registry: SubscriptionRegistry,
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
