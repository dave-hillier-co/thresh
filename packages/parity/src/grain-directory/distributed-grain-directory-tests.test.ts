// Ported from dotnet/orleans test/Orleans.GrainDirectory.Tests/GrainDirectory/DistributedGrainDirectoryTests.cs
// and its base class test/Orleans.Runtime.Tests/Directories/GrainDirectoryTests.cs @ v10.1.0 (MIT).
//
// `DefaultGrainDirectoryTests` runs the shared `GrainDirectoryTests<IGrainDirectory>`
// base-class facts against the silo's `GrainDirectoryResolver.DefaultGrainDirectory` —
// Orleans' pluggable, CAS-based `IGrainDirectory` abstraction (Register/Lookup/Unregister,
// with a `previousAddress` compare-and-set on Register) obtained from a running
// `TestCluster`'s primary silo via DI.
//
// This is distinct from `runtime/grain-directory-tests.test.ts`, which ports the same
// base-class facts against a miniature in-package reimplementation of the CAS contract
// (because at the time it was written, `@thresh/parity` had no dependency on `@thresh/directory`
// and no accessor onto a running silo's real directory instance). Both gaps are closed here:
// `@thresh/parity` now depends on `@thresh/directory` for the `GrainDirectory` type, and
// `ClusterNode.grainDirectory()` / `SiloHost.directory` expose the real
// `DistributedGrainDirectory` a silo runs — the TS analogue of
// `Primary.SiloHost.Services.GetRequiredService<GrainDirectoryResolver>().DefaultGrainDirectory`.
// Every call below goes through the primary silo's directory instance; `DistributedGrainDirectory`
// transparently routes to whichever silo's partition actually owns a given grain id (via the
// same peer/transport plumbing production placement uses), so this exercises the full CAS
// contract exactly as `DefaultClusterFixture`'s 2-silo `TestCluster` does upstream.
import { afterEach, beforeEach, describe, expect } from "vitest";
import { newActivationId } from "@thresh/core/activation-id";
import { grainAddressEquals, type GrainAddress } from "@thresh/core/grain-address";
import { GrainId } from "@thresh/core/grain-id";
import type { SiloAddress } from "@thresh/core/silo-address";
import type { GrainDirectory } from "@thresh/directory/grain-directory";
import { orleansTest } from "@thresh/testing/orleans-test";
import { TestCluster } from "@thresh/testing/test-cluster";

function randomGrainId(): GrainId {
  return new GrainId("user", `somerandomuser_${Math.random().toString(16).slice(2)}`);
}

/**
 * `GrainAddress` equality (`grainAddressEquals`, not `toEqual`): a round trip
 * through a peer silo's transport re-deserializes `grainId`, whose
 * `getUniformHashCode()` memoization is a private cache field rather than
 * observable state — `toEqual`'s structural comparison would see that as a
 * difference even though the two addresses are equal by every public field.
 */
function expectAddress(actual: GrainAddress | undefined, expected: GrainAddress): void {
  expect(actual).toBeDefined();
  expect(grainAddressEquals(actual!, expected)).toBe(true);
}

describe("UnitTests.GrainDirectory.DefaultGrainDirectoryTests", () => {
  const NS = "UnitTests.GrainDirectory.DefaultGrainDirectoryTests";

  let cluster: TestCluster;
  let directory: GrainDirectory;
  // Real silo addresses drawn from the running cluster's membership — the
  // partition's lookup treats an entry pointing at a silo outside the live
  // membership view as gone (Orleans `IsSiloDead`), so fixtures need addresses
  // the cluster actually knows about, unlike upstream's hardcoded IPs (C#'s
  // `IGrainDirectory` implementation under test has no membership awareness).
  let silo1: SiloAddress;
  let silo2: SiloAddress;

  beforeEach(async () => {
    cluster = await TestCluster.start({ initialSilos: 2 });
    directory = cluster.primary.host.directory;
    silo1 = cluster.silos[0]!.address;
    silo2 = cluster.silos[1]!.address;
  });

  afterEach(async () => {
    await cluster.dispose();
  });

  orleansTest(`${NS}.RegisterLookupUnregisterLookup`, async () => {
    const expected = { activationId: newActivationId(), grainId: randomGrainId(), silo: silo1 };

    expectAddress(await directory.register(expected), expected);
    expectAddress(await directory.lookup(expected.grainId), expected);

    await directory.unregister(expected);

    expect(await directory.lookup(expected.grainId)).toBeUndefined();
  });

  orleansTest(`${NS}.DoNotOverwriteEntry`, async () => {
    const grainId = randomGrainId();
    const expected = { activationId: newActivationId(), grainId, silo: silo1 };
    const differentActivation = { activationId: newActivationId(), grainId, silo: silo1 };
    const differentSilo = { activationId: expected.activationId, grainId, silo: silo2 };

    expectAddress(await directory.register(expected), expected);
    expectAddress(await directory.register(differentActivation), expected);
    expectAddress(await directory.register(differentSilo), expected);

    expectAddress(await directory.lookup(expected.grainId), expected);
  });

  // Overwrite an existing entry if the register call includes a matching "previous" address.
  orleansTest(`${NS}.OverwriteEntryIfMatch`, async () => {
    const grainId = randomGrainId();
    const initial = { activationId: newActivationId(), grainId, silo: silo1 };
    const differentActivation = {
      activationId: newActivationId(),
      grainId,
      silo: initial.silo,
    };
    const differentSilo = { activationId: initial.activationId, grainId, silo: silo2 };

    // Success, no registration exists, so the previous address is ignored.
    expectAddress(await directory.register(initial, differentSilo), initial);

    // Success, the previous address matches the existing registration.
    expectAddress(await directory.register(differentActivation, initial), differentActivation);

    // Failure, the previous address does not match the existing registration.
    expectAddress(await directory.register(differentSilo, initial), differentActivation);

    expectAddress(await directory.lookup(initial.grainId), differentActivation);
  });

  orleansTest(`${NS}.DoNotDeleteDifferentActivationIdEntry`, async () => {
    const grainId = randomGrainId();
    const expected = { activationId: newActivationId(), grainId, silo: silo1 };
    const otherEntry = { activationId: newActivationId(), grainId, silo: silo1 };

    expectAddress(await directory.register(expected), expected);
    await directory.unregister(otherEntry);
    expectAddress(await directory.lookup(expected.grainId), expected);
  });

  orleansTest(`${NS}.LookupNotFound`, async () => {
    expect(await directory.lookup(randomGrainId())).toBeUndefined();
  });
});
