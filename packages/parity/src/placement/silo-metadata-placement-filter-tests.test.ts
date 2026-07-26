// Ported from dotnet/orleans test/Orleans.Placement.Tests/PlacementFilterTests/SiloMetadataPlacementFilterTests.cs @ v10.1.0 (MIT).
//
// Deploys a 3-silo cluster via `TestCluster`'s `siloMetadata` option (each
// silo advertising a distinct "unique" metadata value) and exercises grains
// declaring `{ kind: "requiredMatchSiloMetadata" }` /
// `{ kind: "preferredMatchSiloMetadata" }` `placementFilters` entries: a
// required-match filter always places on the calling silo (the only match);
// a preferred-match filter with enough matching candidates does too; and a
// preferred-match filter whose `minCandidates` cannot be satisfied by real
// matches instead falls back to spreading placement across every silo. Calls
// are issued from each silo's own `SiloHost.getGrain` (rather than always
// through `TestCluster`'s primary silo) so the "calling silo" the filters
// match against varies, mirroring upstream's per-silo
// `GetSiloServiceProvider(...).GetRequiredService<IClusterClient>()`.
import { afterAll, beforeAll, describe, expect } from "vitest";
import { orleansTest } from "@thresh/testing/orleans-test";
import type { GrainInterface } from "@thresh/core/grain-interface";
import type { SiloAddress } from "@thresh/core/silo-address";
import { TestCluster } from "@thresh/testing/test-cluster";
import type { IHostReportingGrain } from "@thresh/parity/grains/interfaces/silo-metadata-placement-filter-test-grain-interfaces";
import {
  IPreferredMatchFilteredGrain,
  IPreferredMatchMin2FilteredGrain,
  IPreferredMatchMultipleFilteredGrain,
  IPreferredMatchNoMetadataFilteredGrain,
  IUniqueRequiredMatchFilteredGrain,
  PreferredMatchFilteredGrain,
  PreferredMatchMinTwoFilteredGrain,
  PreferredMatchMultipleFilteredGrain,
  PreferredMatchNoMetadataFilteredGrain,
  UniqueRequiredMatchFilteredGrain,
} from "@thresh/parity/grains/impl/silo-metadata-placement-filter-test-grain";
import {
  IUnfilteredGrain,
  UnfilteredGrain,
} from "@thresh/parity/grains/impl/placement-filter-test-grain";
import { randomIntegerKey } from "@thresh/parity/support/keys";

describe("UnitTests.PlacementFilterTests.SiloMetadataPlacementFilterTests", () => {
  let cluster: TestCluster;

  beforeAll(async () => {
    cluster = await TestCluster.start({
      initialSilos: 3,
      siloMetadata: ({ index }) => ({
        first: "1",
        second: "2",
        third: "3",
        unique: `${index}`,
      }),
      grains: [
        { ctor: UnfilteredGrain, interfaces: [IUnfilteredGrain] },
        { ctor: UniqueRequiredMatchFilteredGrain, interfaces: [IUniqueRequiredMatchFilteredGrain] },
        { ctor: PreferredMatchFilteredGrain, interfaces: [IPreferredMatchFilteredGrain] },
        {
          ctor: PreferredMatchMinTwoFilteredGrain,
          interfaces: [IPreferredMatchMin2FilteredGrain],
        },
        {
          ctor: PreferredMatchMultipleFilteredGrain,
          interfaces: [IPreferredMatchMultipleFilteredGrain],
        },
        {
          ctor: PreferredMatchNoMetadataFilteredGrain,
          interfaces: [IPreferredMatchNoMetadataFilteredGrain],
        },
      ],
    });
  });

  afterAll(async () => {
    await cluster.dispose();
  });

  orleansTest(
    "UnitTests.PlacementFilterTests.SiloMetadataPlacementFilterTests.PlacementFilter_GrainWithoutFilterCanBeCalled",
    async () => {
      await cluster.primary.host.getGrain(IUnfilteredGrain, randomIntegerKey()).ping();
    },
  );

  /**
   * Unique silo metadata is set up to be different for each silo, so this
   * requires that placement happens on the calling silo (the only match).
   */
  orleansTest(
    "UnitTests.PlacementFilterTests.SiloMetadataPlacementFilterTests.PlacementFilter_RequiredFilterCanBeCalled",
    async () => {
      for (const hostedSilo of cluster.silos) {
        for (let i = 0; i < 10; i++) {
          const hostingSilo = await hostedSilo.host
            .getGrain(IUniqueRequiredMatchFilteredGrain, randomIntegerKey())
            .getHostingSilo();
          expect(hostingSilo).toBeDefined();
          expect(hostingSilo.equals(hostedSilo.address)).toBe(true);
        }
      }
    },
  );

  /**
   * Unique silo metadata is set up to be different for each silo, so this
   * requires that placement happens on the calling silo because it is the
   * only one that matches.
   */
  orleansTest(
    "UnitTests.PlacementFilterTests.SiloMetadataPlacementFilterTests.PlacementFilter_PreferredFilterCanBeCalled",
    async () => {
      for (const hostedSilo of cluster.silos) {
        for (let i = 0; i < 10; i++) {
          const hostingSilo = await hostedSilo.host
            .getGrain(IPreferredMatchFilteredGrain, randomIntegerKey())
            .getHostingSilo();
          expect(hostingSilo).toBeDefined();
          expect(hostingSilo.equals(hostedSilo.address)).toBe(true);
        }
      }
    },
  );

  /**
   * Unique silo metadata differs per silo, so the single calling-silo match
   * is not enough to satisfy `minCandidates` (2): placement falls back to
   * spreading across every silo.
   */
  orleansTest(
    "UnitTests.PlacementFilterTests.SiloMetadataPlacementFilterTests.PlacementFilter_PreferredMin2FilterCanBeCalled",
    async () => {
      await assertSpreadsAcrossEverySilo(cluster, IPreferredMatchMin2FilteredGrain);
    },
  );

  /**
   * Neither "unique" nor "other" alone reaches `minCandidates` (2), so
   * placement falls back to spreading across every silo.
   */
  orleansTest(
    "UnitTests.PlacementFilterTests.SiloMetadataPlacementFilterTests.PlacementFilter_PreferredMultipleFilterCanBeCalled",
    async () => {
      await assertSpreadsAcrossEverySilo(cluster, IPreferredMatchMultipleFilteredGrain);
    },
  );

  orleansTest(
    "UnitTests.PlacementFilterTests.SiloMetadataPlacementFilterTests.PlacementFilter_PreferredMin2FilterCanBeCalledWithLargerCluster",
    async () => {
      await assertSpreadsAcrossEverySilo(cluster, IPreferredMatchMin2FilteredGrain);
    },
  );

  /** If no metadata key is defined then it should fall back to matching all silos. */
  orleansTest(
    "UnitTests.PlacementFilterTests.SiloMetadataPlacementFilterTests.PlacementFilter_PreferredNoMetadataFilterCanBeCalled",
    async () => {
      await assertSpreadsAcrossEverySilo(cluster, IPreferredMatchNoMetadataFilteredGrain);
    },
  );
});

async function assertSpreadsAcrossEverySilo(
  cluster: TestCluster,
  iface: GrainInterface<IHostReportingGrain>,
): Promise<void> {
  for (const hostedSilo of cluster.silos) {
    const counts = new Map<string, number>(cluster.silos.map((s) => [s.address.toString(), 0]));
    for (let i = 0; i < 30; i++) {
      const hostingSilo: SiloAddress = await hostedSilo.host
        .getGrain(iface, randomIntegerKey())
        .getHostingSilo();
      expect(hostingSilo).toBeDefined();
      const key = hostingSilo.toString();
      counts.set(key, (counts.get(key) ?? 0) + 1);
    }
    for (const [siloKey, count] of counts) {
      expect(count, `silo ${siloKey} did not host at least 1 grain`).toBeGreaterThanOrEqual(1);
    }
  }
}
