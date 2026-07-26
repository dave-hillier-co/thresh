import type { createClient } from "redis";
import type { GrainId } from "@thresh/core/grain-id";
import { deserializeValue, serializeValue } from "@thresh/core/value-codec";

export type RedisClient = ReturnType<typeof createClient>;

/**
 * Durable pub-sub registry mapping a stream (`namespace/key`) to the set of
 * grains subscribed to it. A subscription survives the subscriber's deactivation
 * and its silo, so a pulling agent on whichever silo owns the stream's queue can
 * always discover who to deliver to. The subscriber's full identity (kind, key)
 * is stored via the value codec so the agent can route to it. Mirrors Orleans'
 * pub-sub store.
 */
export class RedisSubscriptionRegistry {
  constructor(
    private readonly client: RedisClient,
    private readonly keyPrefix: string,
    private readonly providerName: string,
  ) {}

  async subscribe(streamKey: string, subscriber: GrainId): Promise<void> {
    await this.client.sAdd(this.key(streamKey), serializeValue(subscriber));
  }

  async unsubscribe(streamKey: string, subscriber: GrainId): Promise<void> {
    await this.client.sRem(this.key(streamKey), serializeValue(subscriber));
  }

  async subscribers(streamKey: string): Promise<GrainId[]> {
    const members = await this.client.sMembers(this.key(streamKey));
    return members.map((m) => deserializeValue<GrainId>(m));
  }

  private key(streamKey: string): string {
    return `${this.keyPrefix}:subs:${this.providerName}:${streamKey}`;
  }
}
