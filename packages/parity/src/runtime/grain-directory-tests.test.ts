// Ported from dotnet/orleans test/Orleans.Runtime.Tests/Directories/GrainDirectoryTests.cs @ v10.1.0 (MIT).
// Upstream is a generic base class (`GrainDirectoryTests<TGrainDirectory>`)
// exercising a shared CAS contract against external backends (Azure Table,
// Redis, ADO.NET) supplied by concrete subclasses in other test projects, none
// of which are in scope here. This harness's own directory CAS contract
// (`@tsva/directory`'s `GrainDirectory`) is not a declared dependency of the
// parity package, so the contract is re-implemented in miniature here purely
// to exercise the same register/lookup/unregister semantics the upstream
// tests assert on: register wins on first write, later writes lose unless
// their `previous` matches the current entry exactly (a real CAS), and
// unregister only removes an entry that still matches the caller's address.
// Upstream's `GrainAddress` also carries a `MembershipVersion`; this
// framework's `GrainAddress` has no such field, so it is dropped from the
// ported fixtures since none of these tests assert on it.
import { describe, expect } from "vitest";
import { orleansTest } from "@tsva/testing/orleans-test";
import { newActivationId } from "@tsva/core/activation-id";
import { grainAddressEquals, type GrainAddress } from "@tsva/core/grain-address";
import { GrainId } from "@tsva/core/grain-id";
import { SiloAddress } from "@tsva/core/silo-address";

/** Miniature stand-in for `LocalDirectoryPartition`'s CAS contract. */
class InMemoryGrainDirectory {
  private readonly entries = new Map<string, GrainAddress>();

  lookup(grainId: GrainId): GrainAddress | undefined {
    return this.entries.get(grainId.toString());
  }

  register(addr: GrainAddress, previous?: GrainAddress): GrainAddress {
    const key = addr.grainId.toString();
    const existing = this.entries.get(key);
    if (existing === undefined) {
      this.entries.set(key, addr);
      return addr;
    }
    if (previous !== undefined && grainAddressEquals(existing, previous)) {
      this.entries.set(key, addr);
      return addr;
    }
    return existing;
  }

  unregister(addr: GrainAddress): void {
    const key = addr.grainId.toString();
    const existing = this.entries.get(key);
    if (existing !== undefined && grainAddressEquals(existing, addr)) {
      this.entries.delete(key);
    }
  }
}

function randomGrainId(): GrainId {
  return new GrainId("user", `somerandomuser_${Math.random().toString(16).slice(2)}`);
}

const silo1 = new SiloAddress("10.0.23.12", "5678", "10.0.23.12:1000");
const silo2 = new SiloAddress("10.0.23.14", "4583", "10.0.23.14:1000");

describe("Tester.Directories.GrainDirectoryTests", () => {
  orleansTest("Tester.Directories.GrainDirectoryTests.RegisterLookupUnregisterLookup", async () => {
    const directory = new InMemoryGrainDirectory();
    const expected: GrainAddress = {
      activationId: newActivationId(),
      grainId: randomGrainId(),
      silo: silo1,
    };

    expect(grainAddressEquals(directory.register(expected), expected)).toBe(true);
    expect(grainAddressEquals(directory.lookup(expected.grainId)!, expected)).toBe(true);

    directory.unregister(expected);

    expect(directory.lookup(expected.grainId)).toBeUndefined();
  });

  orleansTest("Tester.Directories.GrainDirectoryTests.DoNotOverwriteEntry", async () => {
    const directory = new InMemoryGrainDirectory();
    const grainId = randomGrainId();
    const expected: GrainAddress = { activationId: newActivationId(), grainId, silo: silo1 };
    const differentActivation: GrainAddress = {
      activationId: newActivationId(),
      grainId,
      silo: silo1,
    };
    const differentSilo: GrainAddress = {
      activationId: newActivationId(),
      grainId,
      silo: silo2,
    };

    expect(grainAddressEquals(directory.register(expected), expected)).toBe(true);
    expect(grainAddressEquals(directory.register(differentActivation), expected)).toBe(true);
    expect(grainAddressEquals(directory.register(differentSilo), expected)).toBe(true);

    expect(grainAddressEquals(directory.lookup(expected.grainId)!, expected)).toBe(true);
  });

  orleansTest("Tester.Directories.GrainDirectoryTests.OverwriteEntryIfMatch", async () => {
    const directory = new InMemoryGrainDirectory();
    const grainId = randomGrainId();
    const initial: GrainAddress = { activationId: newActivationId(), grainId, silo: silo1 };
    const differentActivation: GrainAddress = {
      activationId: newActivationId(),
      grainId,
      silo: initial.silo,
    };
    const differentSilo: GrainAddress = {
      activationId: initial.activationId,
      grainId,
      silo: silo2,
    };

    // Success, no registration exists, so the previous address is ignored.
    expect(grainAddressEquals(directory.register(initial, differentSilo), initial)).toBe(true);

    // Success, the previous address matches the existing registration.
    expect(
      grainAddressEquals(directory.register(differentActivation, initial), differentActivation),
    ).toBe(true);

    // Failure, the previous address does not match the existing registration.
    expect(
      grainAddressEquals(directory.register(differentSilo, initial), differentActivation),
    ).toBe(true);

    expect(grainAddressEquals(directory.lookup(initial.grainId)!, differentActivation)).toBe(true);
  });

  orleansTest(
    "Tester.Directories.GrainDirectoryTests.DoNotDeleteDifferentActivationIdEntry",
    async () => {
      const directory = new InMemoryGrainDirectory();
      const grainId = randomGrainId();
      const expected: GrainAddress = { activationId: newActivationId(), grainId, silo: silo1 };
      const otherEntry: GrainAddress = {
        activationId: newActivationId(),
        grainId,
        silo: silo1,
      };

      expect(grainAddressEquals(directory.register(expected), expected)).toBe(true);
      directory.unregister(otherEntry);
      expect(grainAddressEquals(directory.lookup(expected.grainId)!, expected)).toBe(true);
    },
  );

  orleansTest("Tester.Directories.GrainDirectoryTests.LookupNotFound", async () => {
    const directory = new InMemoryGrainDirectory();
    expect(directory.lookup(randomGrainId())).toBeUndefined();
  });
});
