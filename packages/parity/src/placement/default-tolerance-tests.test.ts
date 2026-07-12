// Ported from dotnet/orleans test/Orleans.Placement.Tests/ActivationRepartitioningTests/DefaultToleranceTests.cs @ v10.1.0 (MIT).
//
// Exercises the activation *repartitioner*: a background protocol that moves
// individual grain activations near their most frequent communication
// partners (tracked via a blocked-bloom-filter-backed frequent-edge counter),
// distinct from the activation *rebalancer* (load-leveling, already covered
// under `UnitTests.ActivationRebalancingTests`, GAP-REBALANCER-CONTROL).
//
// `Receivers_ShouldMoveCloseTo_PullingAgent` (upstream `[SkippableFact]`,
// streaming-pulling-agent-driven) is adapted to a stand-in `PullingAgent`
// grain since this port's streaming has no pulling-agent placement-affinity
// subsystem to move — see `default-tolerance-tests-support.ts`'s doc on
// `PullingAgent`/`SR` for the full explanation.
//
// See `default-tolerance-tests-support.ts` for the shared grain
// interfaces/implementations and `RepartitioningTestBase`-equivalent helpers.
import { beforeAll, afterAll, describe, expect } from "vitest";
import { orleansTest } from "@tsva/testing/orleans-test";
import { TestCluster } from "@tsva/testing/test-cluster";
import { PLACEMENT_HINT_KEY, RequestContext } from "@tsva/core/request-context";
import type { SiloAddress } from "@tsva/core/silo-address";
import {
  A,
  B,
  C,
  CImmovable,
  D,
  IA,
  IB,
  IC,
  ICImmovable,
  ID,
  IPullingAgent,
  ISR,
  PullingAgent,
  SR,
} from "./default-tolerance-tests-support";

const TEST_TIMEOUT_MS = 20_000;

/**
 * Poll `check` until it returns `true`, yielding between attempts. Bounded
 * (throws after `maxAttempts`) so a protocol bug fails the test fast with a
 * clear error instead of spinning the event loop forever — the upstream
 * tests' unbounded `do { ... } while (...)` loops assume the real Orleans
 * protocol always converges; this port keeps that assumption honest by
 * failing loudly instead of hanging when it doesn't.
 */
async function pollUntil(check: () => Promise<boolean>, maxAttempts = 500): Promise<void> {
  for (let attempt = 0; attempt < maxAttempts; attempt++) {
    if (await check()) return;
    await new Promise((resolve) => setTimeout(resolve, 5));
  }
  throw new Error(`pollUntil: condition not met after ${maxAttempts} attempts`);
}

describe("UnitTests.ActivationRepartitioningTests.DefaultToleranceTests", () => {
  let cluster: TestCluster;
  let silo1: SiloAddress;
  let silo2: SiloAddress;

  beforeAll(async () => {
    cluster = await TestCluster.start({
      initialSilos: 2,
      grains: [
        { ctor: A, interfaces: [IA] },
        { ctor: B, interfaces: [IB] },
        { ctor: C, interfaces: [IC] },
        { ctor: CImmovable, interfaces: [ICImmovable] },
        { ctor: D, interfaces: [ID] },
        { ctor: PullingAgent, interfaces: [IPullingAgent] },
        { ctor: SR, interfaces: [ISR] },
      ],
      configureSilo: (builder) =>
        builder.useActivationRepartitioning({
          // Practically never fire on a timer; the tests invoke the
          // protocol manually (mirrors the upstream fixture's comment).
          minRoundPeriodMs: 299_000,
          maxRoundPeriodMs: 300_000,
          recoveryPeriodMs: 1,
        }),
    });
    const silos = [...cluster.silos].sort((a, b) =>
      a.address.ringKey.localeCompare(b.address.ringKey),
    );
    silo1 = silos[0]!.address;
    silo2 = silos[1]!.address;
  });

  afterAll(async () => {
    await cluster.dispose();
  });

  const silo1Host = () => cluster.silos.find((s) => s.address.equals(silo1))!.host;
  const silo2Host = () => cluster.silos.find((s) => s.address.equals(silo2))!.host;

  async function resetCounters(): Promise<void> {
    silo1Host().resetRepartitionCounters();
    silo2Host().resetRepartitionCounters();
  }

  /** Ported from `RepartitioningTestBase.AdjustActivationCountOffsets`. */
  async function adjustActivationCountOffsets(): Promise<void> {
    const counts = cluster.silos.map((s) => s.host.getRepartitionActivationCount());
    const max = Math.max(...counts);
    cluster.silos.forEach((s, i) => {
      s.host.setRepartitionActivationCountOffset(max - counts[i]!);
    });
  }

  beforeAll(async () => {
    await resetCounters();
    await adjustActivationCountOffsets();
  });

  orleansTest(
    "UnitTests.ActivationRepartitioningTests.DefaultToleranceTests.A_ShouldMoveToSilo2__B_And_C_ShouldStayOnSilo2",
    async () => {
      RequestContext.set(PLACEMENT_HINT_KEY, silo1.toString());

      const scenario = "1";
      const a = cluster.getGrain(IA, `a${scenario}`);
      await a.firstPing(scenario, silo1.toString(), silo2.toString());

      for (let i = 0; i < 3; i++) await a.ping(scenario);

      const b = cluster.getGrain(IB, `b${scenario}`);
      const c = cluster.getGrain(IC, `c${scenario}`);

      expect((await a.getAddress()).toString()).toBe(silo1.toString());
      expect((await b.getAddress()).toString()).toBe(silo2.toString());
      expect((await c.getAddress()).toString()).toBe(silo2.toString());

      await silo1Host().triggerRepartitionExchange();

      let aHost: SiloAddress = await a.getAddress();
      await pollUntil(async () => {
        aHost = await a.getAddress();
        return aHost.toString() !== silo1.toString();
      });

      expect(aHost.toString()).toBe(silo2.toString());
      expect((await b.getAddress()).toString()).toBe(silo2.toString());
      expect((await c.getAddress()).toString()).toBe(silo2.toString());

      await resetCounters();
    },
    TEST_TIMEOUT_MS,
  );

  orleansTest(
    "UnitTests.ActivationRepartitioningTests.DefaultToleranceTests.C_ShouldMoveToSilo1__A_And_B_ShouldStayOnSilo1",
    async () => {
      RequestContext.set(PLACEMENT_HINT_KEY, silo1.toString());

      const scenario = "2";
      const a = cluster.getGrain(IA, `a${scenario}`);
      const b = cluster.getGrain(IB, `b${scenario}`);

      await a.firstPing(scenario, silo1.toString(), silo2.toString());
      await b.ping(scenario);

      for (let i = 0; i < 3; i++) {
        await a.ping(scenario);
        await b.ping(scenario);
      }

      const c = cluster.getGrain(IC, `c${scenario}`);

      expect((await a.getAddress()).toString()).toBe(silo1.toString());
      expect((await b.getAddress()).toString()).toBe(silo1.toString());
      expect((await c.getAddress()).toString()).toBe(silo2.toString());

      await silo1Host().triggerRepartitionExchange();

      let cHost: SiloAddress = await c.getAddress();
      await pollUntil(async () => {
        cHost = await c.getAddress();
        return cHost.toString() !== silo2.toString();
      });

      expect((await a.getAddress()).toString()).toBe(silo1.toString());
      expect((await b.getAddress()).toString()).toBe(silo1.toString());
      expect(cHost.toString()).toBe(silo1.toString());

      await resetCounters();
    },
    TEST_TIMEOUT_MS,
  );

  orleansTest(
    "UnitTests.ActivationRepartitioningTests.DefaultToleranceTests.Immovable_C_ShouldStayOnSilo2__A_And_B_ShouldMoveToSilo2",
    async () => {
      RequestContext.set(PLACEMENT_HINT_KEY, silo1.toString());

      const scenario = "3";
      const a = cluster.getGrain(IA, `a${scenario}`);
      const b = cluster.getGrain(IB, `b${scenario}`);

      await a.firstPing(scenario, silo1.toString(), silo2.toString());
      await b.ping(scenario);

      for (let i = 0; i < 3; i++) {
        await a.ping(scenario);
        await b.ping(scenario);
      }

      const c = cluster.getGrain(ICImmovable, `c${scenario}`);

      expect((await a.getAddress()).toString()).toBe(silo1.toString());
      expect((await b.getAddress()).toString()).toBe(silo1.toString());
      expect((await c.getAddress()).toString()).toBe(silo2.toString());

      await silo1Host().triggerRepartitionExchange();

      let aHost: SiloAddress = await a.getAddress();
      let bHost: SiloAddress = await b.getAddress();
      await pollUntil(async () => {
        aHost = await a.getAddress();
        bHost = await b.getAddress();
        return aHost.toString() !== silo1.toString() && bHost.toString() !== silo1.toString();
      });

      expect(aHost.toString()).toBe(silo2.toString());
      expect(bHost.toString()).toBe(silo2.toString());
      expect((await c.getAddress()).toString()).toBe(silo2.toString());

      await resetCounters();
    },
    TEST_TIMEOUT_MS,
  );

  orleansTest(
    "UnitTests.ActivationRepartitioningTests.DefaultToleranceTests.A_ShouldMoveToSilo2_Or_C_ShouldMoveToSilo1__B_And_D_ShouldStayOnTheirSilos",
    async () => {
      RequestContext.set(PLACEMENT_HINT_KEY, silo1.toString());

      const scenario = "4";
      const a = cluster.getGrain(IA, `a${scenario}`);

      await a.firstPing(scenario, silo1.toString(), silo2.toString());

      for (let i = 0; i < 3; i++) await a.ping(scenario);

      const b = cluster.getGrain(IB, `b${scenario}`);
      const c = cluster.getGrain(IC, `c${scenario}`);
      const d = cluster.getGrain(ID, `d${scenario}`);

      expect((await a.getAddress()).toString()).toBe(silo1.toString());
      expect((await b.getAddress()).toString()).toBe(silo1.toString());
      expect((await c.getAddress()).toString()).toBe(silo2.toString());
      expect((await d.getAddress()).toString()).toBe(silo2.toString());

      await silo1Host().triggerRepartitionExchange();

      let aHost: SiloAddress = await a.getAddress();
      let cHost: SiloAddress = await c.getAddress();
      await pollUntil(async () => {
        aHost = await a.getAddress();
        cHost = await c.getAddress();
        return !(aHost.toString() === silo1.toString() && cHost.toString() === silo2.toString());
      });

      const bHost = await b.getAddress();
      const dHost = await d.getAddress();

      if (aHost.toString() === silo2.toString() && cHost.toString() === silo2.toString()) {
        expect(bHost.toString()).toBe(silo1.toString());
        expect(dHost.toString()).toBe(silo2.toString());
      } else {
        expect(aHost.toString()).toBe(silo1.toString());
        expect(bHost.toString()).toBe(silo1.toString());
        expect(cHost.toString()).toBe(silo1.toString());
        expect(dHost.toString()).toBe(silo2.toString());
      }

      await resetCounters();
    },
    TEST_TIMEOUT_MS,
  );

  orleansTest(
    "UnitTests.ActivationRepartitioningTests.DefaultToleranceTests.Receivers_ShouldMoveCloseTo_PullingAgent",
    async () => {
      // The stand-in `PullingAgent` (Immovable) lives on Silo2, mirroring the
      // upstream comment that the real pulling agent ends up there.
      RequestContext.set(PLACEMENT_HINT_KEY, silo2.toString());
      const agent = cluster.getGrain(IPullingAgent, "pa");
      await agent.ping();

      const sr1 = cluster.getGrain(ISR, "s1");
      const sr2 = cluster.getGrain(ISR, "s2");
      const sr3 = cluster.getGrain(ISR, "s3");

      // sr1/sr2 start on Silo1 (like their upstream counterparts, whose
      // agent-hosting silo they don't yet share); sr3 already shares the
      // agent's silo.
      RequestContext.set(PLACEMENT_HINT_KEY, silo1.toString());
      await sr1.pingAgent();
      await sr2.pingAgent();

      RequestContext.set(PLACEMENT_HINT_KEY, silo2.toString());
      await sr3.pingAgent();

      for (let i = 0; i < 3; i++) {
        await sr1.pingAgent();
        await sr2.pingAgent();
        await sr3.pingAgent();
      }

      expect((await sr1.getAddress()).toString()).toBe(silo1.toString());
      expect((await sr2.getAddress()).toString()).toBe(silo1.toString());
      expect((await sr3.getAddress()).toString()).toBe(silo2.toString());

      await silo1Host().triggerRepartitionExchange();

      let sr1Host: SiloAddress = await sr1.getAddress();
      let sr2Host: SiloAddress = await sr2.getAddress();
      await pollUntil(async () => {
        sr1Host = await sr1.getAddress();
        sr2Host = await sr2.getAddress();
        return sr1Host.toString() !== silo1.toString() && sr2Host.toString() !== silo1.toString();
      });

      expect(sr1Host.toString()).toBe(silo2.toString());
      expect(sr2Host.toString()).toBe(silo2.toString());
      expect((await sr3.getAddress()).toString()).toBe(silo2.toString());

      await resetCounters();
    },
    TEST_TIMEOUT_MS,
  );
});
