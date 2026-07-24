import { keyToString, type GrainKey } from "@tsva/core/grain-key";
import type { StreamId, StreamProducerHandle } from "@tsva/core/stream";

/**
 * Tracks which producers are currently registered against a stream (Orleans'
 * pub-sub `RegisterProducer` side, mirrored explicitly here — see
 * `StreamProducerHandle`). Shared by `MemoryStreamProvider` and
 * `RedisPullingStreamProvider`'s `registerProducer` implementations; purely
 * in-process bookkeeping, so no durability is needed even for the Redis
 * provider (a producer that dies simply drops its handle, same as Orleans'
 * producer registration does not survive the producing grain's deactivation
 * either).
 */
export class StreamProducerRegistry {
  private readonly producers = new Map<string, Set<number>>();
  private nextId = 0;

  register(providerName: string, namespace: string, key: GrainKey): StreamProducerHandle {
    const keyString = keyToString(key);
    const streamKey = `${namespace}/${keyString}`;
    const producerId = ++this.nextId;
    let ids = this.producers.get(streamKey);
    if (ids === undefined) {
      ids = new Set();
      this.producers.set(streamKey, ids);
    }
    ids.add(producerId);
    const streamId: StreamId = { provider: providerName, namespace, key: keyString };
    let unregistered = false;
    return {
      streamId,
      unregister: async () => {
        if (unregistered) return;
        unregistered = true;
        const remaining = this.producers.get(streamKey);
        remaining?.delete(producerId);
        if (remaining !== undefined && remaining.size === 0) this.producers.delete(streamKey);
      },
    };
  }

  /** Number of live (not-yet-unregistered) producer handles for a stream. */
  producerCount(namespace: string, key: GrainKey): number {
    return this.producers.get(`${namespace}/${keyToString(key)}`)?.size ?? 0;
  }
}
