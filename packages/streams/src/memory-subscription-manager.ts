import type { GrainId } from "@thresh/core/grain-id";
import { Guid } from "@thresh/core/guid";
import type { StreamId, StreamSubscription, StreamSubscriptionManager } from "@thresh/core/stream";

/**
 * Confirms an administratively-added subscription with the target grain's
 * activation (Orleans `IStreamSubscriptionObserver.OnSubscribed`), returning
 * whether it accepted. Wired to `ClusterNode.confirmStreamSubscription` by
 * `SiloBuilder.build()`, the same way `StreamDeliver` is wired to
 * `deliverStreamEvent`.
 */
export type ConfirmStreamSubscription = (grainId: GrainId, streamKey: string) => Promise<boolean>;

function streamKeyOf(streamId: StreamId): string {
  return `${streamId.namespace}/${streamId.key}`;
}

/**
 * In-memory backing store for `MemoryStreamProvider`'s administrative
 * subscription manager (Orleans `Streams.Core.StreamSubscriptionManager`,
 * `StreamSubscriptionManagerType.ExplicitSubscribeOnly`): tracks which grain
 * ids are subscribed to a stream, independent of any implicit-subscription
 * declaration or the grain calling `subscribe()` itself.
 * `MemoryStreamProvider` fans a published event out to every subscriber this
 * registry lists, alongside its implicit subscribers (see
 * `memory-stream-provider.ts`).
 */
export class MemorySubscriptionManager implements StreamSubscriptionManager {
  private readonly byStream = new Map<string, Map<string, StreamSubscription>>();
  private confirm: ConfirmStreamSubscription = async () => true;

  /** Wired by `MemoryStreamProvider` once the delivery path is available. */
  setConfirm(confirm: ConfirmStreamSubscription): void {
    this.confirm = confirm;
  }

  async addSubscription(streamId: StreamId, grainId: GrainId): Promise<StreamSubscription> {
    const streamKey = streamKeyOf(streamId);
    const subscription: StreamSubscription = {
      subscriptionId: Guid.newGuid().toString(),
      streamId,
      grainId,
    };
    let subs = this.byStream.get(streamKey);
    if (subs === undefined) {
      subs = new Map();
      this.byStream.set(streamKey, subs);
    }
    subs.set(subscription.subscriptionId, subscription);
    // Confirm with the target grain (Orleans `OnSubscribed`): declining drops
    // the subscription before any event is ever delivered to it.
    const accepted = await this.confirm(grainId, streamKey);
    if (!accepted) subs.delete(subscription.subscriptionId);
    return subscription;
  }

  async removeSubscription(streamId: StreamId, subscriptionId: string): Promise<void> {
    this.byStream.get(streamKeyOf(streamId))?.delete(subscriptionId);
  }

  async getSubscriptions(streamId: StreamId): Promise<StreamSubscription[]> {
    return [...(this.byStream.get(streamKeyOf(streamId))?.values() ?? [])];
  }

  /** Grain ids currently subscribed to `streamKey` (consulted by publish fan-out). */
  subscribersFor(streamKey: string): GrainId[] {
    return [...(this.byStream.get(streamKey)?.values() ?? [])].map((s) => s.grainId);
  }
}
