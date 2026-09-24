import { keyToString, type GrainKey } from "@thresh/core/grain-key";
import type { GrainType } from "@thresh/core/grain-type";
import {
  STREAM_GENERATOR_COMMAND_CONFIGURE,
  type AsyncStream,
  type Controllable,
  type StreamHandler,
  type StreamId,
  type StreamProvider,
  type StreamSubscriptionHandle,
  type SubscribeOptions,
} from "@thresh/core/stream";
import { FanOutDelivery } from "@thresh/streams/fan-out-delivery";
import { implicitSubscriberIds } from "@thresh/streams/implicit-subscriptions";
import {
  GeneratorStreamQueue,
  type StreamGeneratorConfig,
} from "@thresh/streams/generator-stream-queue";
import {
  QueuePullingAgent,
  type PullingStreamProviderHost,
  type StreamFailureHandler,
} from "@thresh/streams/queue-pulling-agent";
import { ownedQueueIndices, type HashRange } from "@thresh/streams/queue-ownership";
import type { StreamDeliver } from "@thresh/streams/stream-deliver";
import { StreamProviderConfigurationError } from "@thresh/streams/stream-provider-config-error";

export interface GeneratorPullingStreamProviderOptions {
  /** Number of physical queues, each generating one synthetic stream (defaults to 4). */
  queueCount?: number;
  pollIntervalMs?: number;
  /** Notified when a subscriber's delivery exhausts its retry budget (Orleans `IStreamFailureHandler`). */
  failureHandler?: StreamFailureHandler;
  /**
   * Total time (ms) to keep retrying delivery to ONE subscriber before
   * skipping it — every other subscriber of the same event is retried and
   * timed out independently (Orleans `MaxEventDeliveryTime`; default 1 minute).
   */
  maxEventDeliveryTimeMs?: number;
  /**
   * Bounds a single delivery attempt to one subscriber (Orleans
   * `ResponseTimeout`; default 30s) so a hung `onNext` cannot stall this
   * provider's queues, or silo shutdown, forever.
   */
  deliveryResponseTimeoutMs?: number;
  /** Backoff between retries to the same subscriber; defaults to 2^attempt * 50ms, capped at 5s. */
  retryBackoffMs?: (attempt: number) => number;
}

/**
 * Test-only stream provider whose "queues" synthesize events rather than
 * reading a real backing store — Orleans' `GeneratorAdapterFactory`. Each of
 * `queueCount` physical queues generates exactly one stream (a fresh random
 * id under `config.streamNamespace`) of `config.eventsInStream` events; a
 * pulling agent per owned queue drives generation and delivers each event to
 * the stream's implicit subscribers, identical in shape to
 * `RedisPullingStreamProvider` but backed by `GeneratorStreamQueue` instead
 * of `RedisStreamQueue`. `Direction` is read-only (Orleans': generated
 * streams are never published to by a client), so `getStream` only supports
 * inspection, not publish/subscribe.
 */
export class GeneratorPullingStreamProvider
  implements StreamProvider, PullingStreamProviderHost, Controllable
{
  private readonly queueCount: number;
  private readonly pollIntervalMs: number;
  private readonly queues: GeneratorStreamQueue[];
  private readonly agents = new Map<number, QueuePullingAgent>();
  private readonly fanOutDelivery: FanOutDelivery;
  private deliver: StreamDeliver = async () => undefined;
  private implicitTypesFor: (namespace: string) => Iterable<GrainType> = () => [];

  constructor(
    private readonly name: string,
    config: StreamGeneratorConfig,
    options: GeneratorPullingStreamProviderOptions = {},
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
    this.queueCount = options.queueCount ?? 4;
    this.pollIntervalMs = options.pollIntervalMs ?? 20;
    this.queues = Array.from({ length: this.queueCount }, () => new GeneratorStreamQueue(config));
    this.fanOutDelivery = new FanOutDelivery(
      (subscriber, streamKey, event, token) => this.deliver(subscriber, streamKey, event, token),
      {
        ...(options.failureHandler !== undefined ? { failureHandler: options.failureHandler } : {}),
        ...(options.maxEventDeliveryTimeMs !== undefined
          ? { maxEventDeliveryTimeMs: options.maxEventDeliveryTimeMs }
          : {}),
        ...(options.deliveryResponseTimeoutMs !== undefined
          ? { deliveryResponseTimeoutMs: options.deliveryResponseTimeoutMs }
          : {}),
        ...(options.retryBackoffMs !== undefined ? { retryBackoffMs: options.retryBackoffMs } : {}),
      },
    );
  }

  /** Total physical queues; queue ownership is assigned over `[0, physicalQueueCount)`. */
  get physicalQueueCount(): number {
    return this.queueCount;
  }

  setDeliver(deliver: StreamDeliver): void {
    this.deliver = deliver;
  }

  setImplicitSubscribers(typesFor: (namespace: string) => Iterable<GrainType>): void {
    this.implicitTypesFor = typesFor;
  }

  /** See `RedisPullingStreamProvider.refreshOwnership`: run agents for exactly the owned queues. */
  refreshOwnership(ranges: readonly HashRange[]): void {
    this.startAgentsFor(ownedQueueIndices(this.name, this.queueCount, ranges));
  }

  /**
   * Reconfigure every queue's generator live (Orleans'
   * `GeneratorAdapterFactory.ExecuteCommand(Configure, config)` /
   * `IControllable`): each owned queue starts generating a fresh stream from
   * the new config on its next pull.
   */
  reconfigure(config: StreamGeneratorConfig): void {
    for (const queue of this.queues) queue.reconfigure(config);
    // Any agent already running past the queue's now-reset cursor must forget
    // it too, or it never notices the fresh stream (see `QueuePullingAgent.resetCursor`).
    for (const agent of this.agents.values()) agent.resetCursor();
  }

  /**
   * Orleans `IControllable.ExecuteCommand`, reached via
   * `IManagementGrain.SendControlCommandToProvider`: the only command this
   * provider understands is `StreamGeneratorCommand.Configure`, which
   * reconfigures every queue live (see `reconfigure`) and reports success.
   */
  async executeCommand(command: number, arg: unknown): Promise<unknown> {
    if (command === STREAM_GENERATOR_COMMAND_CONFIGURE) {
      this.reconfigure(arg as StreamGeneratorConfig);
      return true;
    }
    throw new Error(`GeneratorPullingStreamProvider: unsupported control command ${command}`);
  }

  startAgentsFor(indices: Iterable<number>): void {
    const wanted = new Set(indices);
    for (const [i, agent] of this.agents) {
      if (!wanted.has(i)) {
        // Fire-and-forget: stop() never rejects (pump failures are contained).
        void agent.stop();
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

  async stop(): Promise<void> {
    const agents = [...this.agents.values()];
    this.agents.clear();
    await Promise.all(agents.map((agent) => agent.stop()));
  }

  getStream<T>(namespace: string, key: GrainKey): AsyncStream<T> {
    const id: StreamId = { provider: this.name, namespace, key: keyToString(key) };
    return new ReadOnlyGeneratedStream<T>(id);
  }

  private async fanOut(streamKey: string, event: unknown, token: number): Promise<void> {
    const implicit = implicitSubscriberIds(streamKey, this.implicitTypesFor);
    const seen = new Set<string>();
    const subscribers = [];
    for (const subscriber of implicit) {
      const id = subscriber.toString();
      if (seen.has(id)) continue;
      seen.add(id);
      subscribers.push(subscriber);
    }
    await this.fanOutDelivery.deliverToAll(subscribers, streamKey, event, token);
  }
}

/** `getStream` on a read-only (generator) provider: inspection only, no publish/subscribe. */
class ReadOnlyGeneratedStream<T> implements AsyncStream<T> {
  constructor(readonly id: StreamId) {}

  async publish(_event: T): Promise<void> {
    throw new Error(
      "generator stream provider is read-only: streams are synthesized, not published",
    );
  }

  async subscribe(
    _handler: StreamHandler<T>,
    _options?: SubscribeOptions,
  ): Promise<StreamSubscriptionHandle<T>> {
    throw new Error(
      "generator stream provider delivers only to implicit subscribers; explicit subscribe is not supported",
    );
  }

  async getSubscriptions(): Promise<StreamSubscriptionHandle<T>[]> {
    return [];
  }
}
