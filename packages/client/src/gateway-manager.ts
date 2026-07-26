import type { SiloAddress } from "@thresh/core/silo-address";
import type { GatewayListProvider } from "@thresh/client/gateway-provider";

/**
 * Tracks the live gateways a client may use and hands them out round-robin
 * (Orleans' `GatewayManager`). `refresh` pulls the current set from the
 * `GatewayListProvider`; `markAsDead` drops an unreachable gateway from rotation
 * until the next refresh re-learns it. Selection is round-robin so a client
 * spreads its calls across gateways rather than pinning one.
 */
export class GatewayManager {
  private live: SiloAddress[] = [];
  private counter = 0;

  constructor(private readonly provider: GatewayListProvider) {}

  /** Re-pull the gateway list from the provider; previously-dead gateways reappear if still listed. */
  async refresh(): Promise<void> {
    this.live = [...(await this.provider.getGateways())];
  }

  /** The next live gateway in round-robin order, or `undefined` if none remain. */
  next(): SiloAddress | undefined {
    if (this.live.length === 0) return undefined;
    // Atomic increment-then-modulo in a single statement so concurrent callers
    // (e.g. several `Promise.all` invocations resuming on the same microtask
    // queue) cannot interleave between reading the counter and writing it back.
    return this.live[this.counter++ % this.live.length];
  }

  /** Drop an unreachable gateway from rotation until the next `refresh` (Orleans `MarkAsDead`). */
  markAsDead(gateway: SiloAddress): void {
    this.live = this.live.filter((g) => g.endpoint !== gateway.endpoint);
  }

  /** How many gateways are currently in rotation. */
  get liveCount(): number {
    return this.live.length;
  }
}
