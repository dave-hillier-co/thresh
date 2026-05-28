import { describe, expect, it } from "vitest";
import { SiloAddress } from "@tsva/core/silo-address";
import { GatewayManager } from "@tsva/client/gateway-manager";
import {
  membershipGatewayProvider,
  staticGatewayProvider,
  urlGatewayProvider,
  type GatewayListProvider,
} from "@tsva/client/gateway-provider";

const silo = (n: number) => new SiloAddress(`silo-${n}`, `uid-${n}`, `silo-${n}:1111${n}`);

describe("gateway providers", () => {
  it("staticGatewayProvider returns the fixed list", async () => {
    const provider = staticGatewayProvider([silo(0), silo(1)]);
    expect((await provider.getGateways()).map((g) => g.endpoint)).toEqual([
      "silo-0:11110",
      "silo-1:11111",
    ]);
  });

  it("membershipGatewayProvider tracks the active silos and follows changes", async () => {
    let active = [silo(0), silo(1)];
    const provider = membershipGatewayProvider({
      current: () => ({
        version: 1,
        silos: active.map((s) => ({ address: s, status: "active" as const })),
      }),
      updates: async function* () {},
      localSilo: () => silo(0),
    });
    expect((await provider.getGateways()).length).toBe(2);
    active = [silo(0)]; // a silo left
    expect((await provider.getGateways()).map((g) => g.endpoint)).toEqual(["silo-0:11110"]);
  });

  it("urlGatewayProvider maps URLs to gateway endpoints, ignoring the scheme", async () => {
    const provider = urlGatewayProvider(["ws://10.0.0.1:5000", "10.0.0.2:5000/path"]);
    expect((await provider.getGateways()).map((g) => g.endpoint)).toEqual([
      "10.0.0.1:5000",
      "10.0.0.2:5000",
    ]);
  });
});

describe("GatewayManager", () => {
  const provider = (silos: SiloAddress[]): GatewayListProvider => staticGatewayProvider(silos);

  it("hands out gateways round-robin after a refresh", async () => {
    const mgr = new GatewayManager(provider([silo(0), silo(1), silo(2)]));
    await mgr.refresh();
    const picks = [mgr.next(), mgr.next(), mgr.next(), mgr.next()].map((g) => g?.endpoint);
    expect(picks).toEqual(["silo-0:11110", "silo-1:11111", "silo-2:11112", "silo-0:11110"]);
  });

  it("returns nothing before the first refresh", () => {
    const mgr = new GatewayManager(provider([silo(0)]));
    expect(mgr.next()).toBeUndefined();
    expect(mgr.liveCount).toBe(0);
  });

  it("drops a dead gateway from rotation until the next refresh re-learns it", async () => {
    const mgr = new GatewayManager(provider([silo(0), silo(1)]));
    await mgr.refresh();
    mgr.markAsDead(silo(0));
    expect(mgr.liveCount).toBe(1);
    // Only the survivor is handed out now.
    expect(mgr.next()!.endpoint).toBe("silo-1:11111");
    expect(mgr.next()!.endpoint).toBe("silo-1:11111");
    // A refresh re-learns the gateway that was marked dead.
    await mgr.refresh();
    expect(mgr.liveCount).toBe(2);
  });

  it("returns undefined when every gateway is dead", async () => {
    const mgr = new GatewayManager(provider([silo(0)]));
    await mgr.refresh();
    mgr.markAsDead(silo(0));
    expect(mgr.next()).toBeUndefined();
  });

  it("hands out gateways without duplicates within a round under concurrent callers", async () => {
    const silos = [silo(0), silo(1), silo(2), silo(3), silo(4)];
    const mgr = new GatewayManager(provider(silos));
    await mgr.refresh();
    const rounds = 200;
    const totalCalls = silos.length * rounds;
    // Hammer next() concurrently — every gateway must be visited the same
    // number of times, which is only true if the counter increment is atomic
    // (no read-modify-write race).
    const picks = await Promise.all(
      Array.from({ length: totalCalls }, () => Promise.resolve().then(() => mgr.next()!.endpoint)),
    );
    const counts = new Map<string, number>();
    for (const endpoint of picks) counts.set(endpoint, (counts.get(endpoint) ?? 0) + 1);
    expect(counts.size).toBe(silos.length);
    for (const s of silos) {
      expect(counts.get(s.endpoint)).toBe(rounds);
    }
  });
});
