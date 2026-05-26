import type {
  BroadcastChannelProvider,
  BroadcastChannelWriter,
  ChannelId,
} from "@tsva/core/broadcast-channel";

/**
 * A named broadcast-channel provider. Its writers are thin: each `publish`
 * delegates to a silo-level fan-out (`ClusterNode.publishToBroadcastChannel`),
 * which resolves the channel's implicit subscribers and dispatches the item to
 * each. There is no per-channel state — broadcast channels are direct in-cluster
 * pub/sub without the pulling-agent / cursor machinery of streams (ADR 0015).
 */
export class BroadcastChannelProviderImpl implements BroadcastChannelProvider {
  constructor(
    private readonly providerName: string,
    private readonly publish: (provider: string, channel: ChannelId, item: unknown) => Promise<void>,
  ) {}

  getChannelWriter<T>(channel: ChannelId): BroadcastChannelWriter<T> {
    const provider = this.providerName;
    const publish = this.publish;
    return {
      publish: (item: T) => publish(provider, channel, item),
    };
  }
}
