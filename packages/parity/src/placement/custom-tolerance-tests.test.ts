// Ported from dotnet/orleans test/Orleans.Placement.Tests/ActivationRepartitioningTests/CustomToleranceTests.cs @ v10.1.0 (MIT).
//
// Drives the same activation-repartitioning protocol as `DefaultToleranceTests`
// but with a custom `ImbalanceToleranceRule` (`imbalance <= 2`), asserting the
// repartitioner eventually converts every remote call into a local one while
// respecting the configured tolerance for "acceptable" remote calls, and that
// re-triggering after convergence yields no further migrations.
import { describe, expect } from "vitest";
import { orleansTest } from "@thresh/testing/orleans-test";
import { TestCluster } from "@thresh/testing/test-cluster";
import { PLACEMENT_HINT_KEY, RequestContext } from "@thresh/core/request-context";
import type { SiloAddress } from "@thresh/core/silo-address";
import type { ImbalanceToleranceRule } from "@thresh/runtime/placement/repartitioning/activation-repartitioner";
import { E, F, IE, IF, IX, X } from "./custom-tolerance-tests-support";

const TEST_TIMEOUT_MS = 20_000;

/** Ported from `CustomToleranceTests.Fixture.HardLimitRule`. */
const hardLimitRule: ImbalanceToleranceRule = { isSatisfiedBy: (imbalance) => imbalance <= 2 };

describe("UnitTests.ActivationRepartitioningTests.CustomToleranceTests", () => {
  orleansTest(
    "UnitTests.ActivationRepartitioningTests.CustomToleranceTests.Should_ConvertAllRemoteCalls_ToLocalCalls_WhileRespectingTolerance",
    async () => {
      const cluster = await TestCluster.start({
        initialSilos: 2,
        grains: [
          { ctor: E, interfaces: [IE] },
          { ctor: F, interfaces: [IF] },
          { ctor: X, interfaces: [IX] },
        ],
        configureSilo: (builder) =>
          builder.useActivationRepartitioning(
            {
              minRoundPeriodMs: 299_000,
              maxRoundPeriodMs: 300_000,
              recoveryPeriodMs: 0,
              // Remove the remote possibility of false positives from a
              // probabilistic filter this port doesn't implement anyway (see
              // `activation-repartitioner.ts`'s module doc: no anchoring
              // filter is ported).
              anchoringFilterEnabled: false,
            },
            hardLimitRule,
          ),
      });
      try {
        const silos = [...cluster.silos].sort((a, b) =>
          a.address.ringKey.localeCompare(b.address.ringKey),
        );
        const silo1: SiloAddress = silos[0]!.address;
        const silo2: SiloAddress = silos[1]!.address;
        const silo1Host = () => cluster.silos.find((s) => s.address.equals(silo1))!.host;
        const silo2Host = () => cluster.silos.find((s) => s.address.equals(silo2))!.host;

        // Ported from `RepartitioningTestBase.AdjustActivationCountOffsets`.
        const adjustActivationCountOffsets = (): void => {
          const counts = cluster.silos.map((s) => s.host.getRepartitionActivationCount());
          const max = Math.max(...counts);
          cluster.silos.forEach((s, i) => {
            s.host.setRepartitionActivationCountOffset(max - counts[i]!);
          });
        };
        adjustActivationCountOffsets();

        const e1 = cluster.getGrain(IE, 1n);
        const e2 = cluster.getGrain(IE, 2n);
        const e3 = cluster.getGrain(IE, 3n);

        RequestContext.set(PLACEMENT_HINT_KEY, silo1.toString());

        await e1.firstPing(silo2.toString());
        await e2.firstPing(silo2.toString());
        await e3.firstPing(silo2.toString());

        RequestContext.set(PLACEMENT_HINT_KEY, silo2.toString());
        const x = cluster.getGrain(IX, 0n);
        await x.ping();

        for (let i = 0; i < 3; i++) {
          await e1.ping();
          await e2.ping();
          await e3.ping();
          await x.ping();
        }

        const f1 = cluster.getGrain(IF, 1n);
        const f2 = cluster.getGrain(IF, 2n);
        const f3 = cluster.getGrain(IF, 3n);

        expect((await e1.getAddress()).toString()).toBe(silo1.toString());
        expect((await e2.getAddress()).toString()).toBe(silo1.toString());
        expect((await e3.getAddress()).toString()).toBe(silo1.toString());

        expect((await f1.getAddress()).toString()).toBe(silo2.toString());
        expect((await f2.getAddress()).toString()).toBe(silo2.toString());
        expect((await f3.getAddress()).toString()).toBe(silo2.toString());

        expect((await x.getAddress()).toString()).toBe(silo2.toString());

        await silo1Host().triggerRepartitionExchange();

        const test = async (): Promise<void> => {
          const [e1Host, e2Host, e3Host, f1Host, f2Host, f3Host] = await Promise.all([
            e1.getAddress(),
            e2.getAddress(),
            e3.getAddress(),
            f1.getAddress(),
            f2.getAddress(),
            f3.getAddress(),
          ]);

          expect(f1Host.toString()).toBe(e1Host.toString());
          expect(f2Host.toString()).toBe(e2Host.toString());
          expect(f3Host.toString()).toBe(e3Host.toString());

          const locations = [e1Host, e2Host, e3Host];
          expect(locations.every((h) => h.toString() === silo1.toString())).toBe(false);
          expect(locations.every((h) => h.toString() === silo2.toString())).toBe(false);

          expect((await x.getAddress()).toString()).toBe(silo2.toString());
        };

        await test();

        for (let i = 0; i < 3; i++) {
          await e1.ping();
          await e2.ping();
          await e3.ping();
          await x.ping();
        }

        await silo1Host().triggerRepartitionExchange();
        await test();

        for (let i = 0; i < 3; i++) {
          await e1.ping();
          await e2.ping();
          await e3.ping();
          await x.ping();
        }

        await silo2Host().triggerRepartitionExchange();
        await test();
      } finally {
        await cluster.dispose();
      }
    },
    TEST_TIMEOUT_MS,
  );
});
