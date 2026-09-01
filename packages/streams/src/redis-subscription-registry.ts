import type { createClient } from "redis";
import type { GrainId } from "@thresh/core/grain-id";
import { deserializeValue, serializeValue } from "@thresh/core/value-codec";
import { DEFAULT_SERVICE_ID } from "@thresh/core/default-service-id";

export type RedisClient = ReturnType<typeof createClient>;

/**
 * Durable pub-sub registry mapping a stream (`namespace/key`) to the set of
 * grains subscribed to it. A subscription survives the subscriber's deactivation
 * and its silo, so a pulling agent on whichever silo owns the stream's queue can
 * always discover who to deliver to. The subscriber's full identity (kind, key)
 * is stored via the value codec so the agent can route to it. Mirrors Orleans'
 * pub-sub store. Partitioned by `serviceId` (issue #64, following #59's
 * `RedisGrainStorage` pattern) so several services sharing one Redis and
 * provider name don't share subscribers; Redis has no ALTER, so this is a
 * deliberate upgrade break — a set written before this dimension existed
 * orphans (see todo.md / docs/deviations.md).
 */
export class RedisSubscriptionRegistry {
  constructor(
    private readonly client: RedisClient,
    private readonly keyPrefix: string,
    private readonly providerName: string,
    private readonly serviceId: string = DEFAULT_SERVICE_ID,
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
    return `${this.keyPrefix}:${this.serviceId}:subs:${this.providerName}:${streamKey}`;
  }
}
