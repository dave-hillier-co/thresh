import { describe, expect, it } from "vitest";
import { grain } from "@thresh/core/decorators";
import { Grain } from "@thresh/core/grain";
import { GrainId } from "@thresh/core/grain-id";
import { defineGrainInterface } from "@thresh/core/grain-interface";
import type { GrainWithStringKey } from "@thresh/core/key-kinds";
import { SiloAddress } from "@thresh/core/silo-address";
import { FakeTimeProvider } from "@thresh/core/test-support/fake-time-provider";
import { InProcessNetwork } from "@thresh/messaging/in-process-transport";
import { createSilo } from "@thresh/hosting/silo-builder";

interface IPing extends GrainWithStringKey {
  ping(): Promise<string>;
}

// A long decorator age, so any collection inside the test window can only come
// from a silo-level override (Orleans `GrainCollectionOptions.ClassSpecificCollectionAge`).
const IPatient = defineGrainInterface<IPing>("IPatient.classage");
@grain({ collectionAgeSeconds: 600 })
class PatientGrain extends Grain implements IPing {
  async ping(): Promise<string> {
    return "pong";
  }
}

// No decorator age at all: falls through to the silo's default.
const IPlain = defineGrainInterface<IPing>("IPlain.classage");
@grain()
class PlainGrain extends Grain implements IPing {
  async ping(): Promise<string> {
    return "pong";
  }
}

// A second age-less grain type, so a test can show a constructor-configured entry and a
// builder-time one landing in the SAME map without either standing in for the other.
const IOther = defineGrainInterface<IPing>("IOther.classage");
@grain()
class OtherGrain extends Grain implements IPing {
  async ping(): Promise<string> {
    return "pong";
  }
}

const patientId = new GrainId("Patient", "p");
const plainId = new GrainId("Plain", "p");
const otherId = new GrainId("Other", "p");

const flush = () => new Promise((r) => setTimeout(r, 0));
async function settle(): Promise<void> {
  for (let i = 0; i < 50; i++) await flush();
}

function siloBuilder(
  name: string,
  time: FakeTimeProvider,
  classSpecificCollectionAgeSeconds?: Readonly<Record<string, number>>,
) {
  const local = new SiloAddress(name, `uid-${name}`, `${name}:11111`);
  return createSilo({
    clusterId: name,
    local,
    time,
    collectionAgeSeconds: 900,
    collectionIntervalSeconds: 1,
    ...(classSpecificCollectionAgeSeconds !== undefined
      ? { classSpecificCollectionAgeSeconds }
      : {}),
  })
    .useStaticMembership([local])
    .useInProcessTransport(new InProcessNetwork())
    .registerGrain(PatientGrain, { interfaces: [IPatient] })
    .registerGrain(PlainGrain, { interfaces: [IPlain] })
    .registerGrain(OtherGrain, { interfaces: [IOther] });
}

function buildSilo(
  name: string,
  time: FakeTimeProvider,
  classSpecificCollectionAgeSeconds?: Readonly<Record<string, number>>,
) {
  return siloBuilder(name, time, classSpecificCollectionAgeSeconds).build();
}

describe("per-silo class-specific collection age (#57)", () => {
  it("lets two silos in one process hold different ages for the same grain type", async () => {
    const time = new FakeTimeProvider();
    // Same grain CLASS, same process: only a per-silo map can separate them. `PlainGrain` declares
    // no age of its own, which is the shape a grain type uses when it wants per-silo control -
    // a decorator age would win over the map, as it does in Orleans.
    const overridden = buildSilo("silo-a", time, { Plain: 5 });
    const untouched = buildSilo("silo-b", time);
    await overridden.start();
    await untouched.start();
    try {
      await overridden.getGrain(IPlain, "p").ping();
      await untouched.getGrain(IPlain, "p").ping();
      expect(overridden.isActive(plainId)).toBe(true);
      expect(untouched.isActive(plainId)).toBe(true);

      time.advance(10_000);
      await settle();

      expect(overridden.isActive(plainId)).toBe(false);
      // The silo default still governs the silo that configured nothing.
      expect(untouched.isActive(plainId)).toBe(true);
    } finally {
      await overridden.stop();
      await untouched.stop();
    }
  });

  it("overrides the silo default for a grain type with no decorator age", async () => {
    const time = new FakeTimeProvider();
    const silo = buildSilo("silo-c", time, { Plain: 5 });
    await silo.start();
    try {
      await silo.getGrain(IPlain, "p").ping();
      time.advance(10_000);
      await settle();
      expect(silo.isActive(plainId)).toBe(false);
    } finally {
      await silo.stop();
    }
  });

  it("rejects a non-positive override at build, not at first activation", () => {
    const time = new FakeTimeProvider();
    expect(() => buildSilo("silo-e", time, { Patient: 0 })).toThrow(/Patient/);
    expect(() => buildSilo("silo-e", time, { Patient: Number.NaN })).toThrow(/Patient/);
  });

  /**
   * Orleans' precedence, from `GrainTypeSharedContext.GetCollectionAgeLimit`: the grain class's
   * own `[CollectionAgeLimit]` is read FIRST and returns immediately, so `ClassSpecificCollectionAge`
   * never applies to a type that declares its own age. Getting this backwards is silent - the
   * activation simply collects on the wrong schedule - so it is pinned explicitly.
   */
  it("lets a grain's own decorator age win over the per-silo map, as Orleans does", async () => {
    const time = new FakeTimeProvider();
    // `PatientGrain` declares 600s; the map asks for 5s and must NOT win.
    const silo = buildSilo("silo-f", time, { Patient: 5 });
    await silo.start();
    try {
      await silo.getGrain(IPatient, "p").ping();
      time.advance(10_000);
      await settle();
      expect(silo.isActive(patientId)).toBe(true);
    } finally {
      await silo.stop();
    }
  });

  it("leaves an unlisted grain type on its decorator age", async () => {
    const time = new FakeTimeProvider();
    const silo = buildSilo("silo-d", time, { Plain: 5 });
    await silo.start();
    try {
      await silo.getGrain(IPatient, "p").ping();
      time.advance(10_000);
      await settle();
      expect(silo.isActive(patientId)).toBe(true);
    } finally {
      await silo.stop();
    }
  });
});

/**
 * #66: the same per-silo map, reachable AFTER construction. Registration helpers that resolve a
 * grain's collection age at the point they register it (BeneDB's `addSpiceportGrainServices`) only
 * ever see an already-constructed builder, which is exactly when `SiloConfig` is closed - so
 * without a mutator those callers fall back to rewriting the process-wide grain metadata and lose
 * the per-silo separation #57 added. Every assertion here goes through the real collector, because
 * the property under test is that the entry reaches the node's map, not that a field was set.
 *
 * The mutator inherits #57's precedence, so it reaches only grain classes that declare no
 * `collectionAgeSeconds`; the two tests at the end of this block pin that limit rather than leave
 * a caller to discover it as a silent no-op.
 */
describe("builder-time class-specific collection age (#66)", () => {
  it("collects on an age set after construction", async () => {
    const time = new FakeTimeProvider();
    const silo = siloBuilder("silo-66a", time).useClassSpecificCollectionAge("Plain", 5).build();
    await silo.start();
    try {
      await silo.getGrain(IPlain, "p").ping();
      time.advance(10_000);
      await settle();
      expect(silo.isActive(plainId)).toBe(false);
    } finally {
      await silo.stop();
    }
  });

  it("merges builder-time entries with the constructor map", async () => {
    const time = new FakeTimeProvider();
    // `Plain` comes from the constructor, `Other` from registration time: a merge keeps both,
    // whereas a replacing setter would drop one of them.
    const silo = siloBuilder("silo-66b", time, { Plain: 5 })
      .useClassSpecificCollectionAge("Other", 5)
      .build();
    await silo.start();
    try {
      await silo.getGrain(IPlain, "p").ping();
      await silo.getGrain(IOther, "p").ping();
      time.advance(10_000);
      await settle();
      expect(silo.isActive(plainId)).toBe(false);
      expect(silo.isActive(otherId)).toBe(false);
    } finally {
      await silo.stop();
    }
  });

  it("lets a builder-time entry override the constructor's for the same grain type", async () => {
    const time = new FakeTimeProvider();
    const silo = siloBuilder("silo-66c", time, { Plain: 900 })
      .useClassSpecificCollectionAge("Plain", 5)
      .build();
    await silo.start();
    try {
      await silo.getGrain(IPlain, "p").ping();
      time.advance(10_000);
      await settle();
      expect(silo.isActive(plainId)).toBe(false);
    } finally {
      await silo.stop();
    }
  });

  it("lets the last builder-time entry win for the same grain type", async () => {
    const time = new FakeTimeProvider();
    const silo = siloBuilder("silo-66d", time)
      .useClassSpecificCollectionAge("Plain", 900)
      .useClassSpecificCollectionAge("Plain", 5)
      .build();
    await silo.start();
    try {
      await silo.getGrain(IPlain, "p").ping();
      time.advance(10_000);
      await settle();
      expect(silo.isActive(plainId)).toBe(false);
    } finally {
      await silo.stop();
    }
  });

  it("merges a whole map of ages without dropping earlier entries", async () => {
    const time = new FakeTimeProvider();
    const silo = siloBuilder("silo-66e", time)
      .useClassSpecificCollectionAge("Plain", 5)
      .useClassSpecificCollectionAges({ Other: 5 })
      .build();
    await silo.start();
    try {
      await silo.getGrain(IPlain, "p").ping();
      await silo.getGrain(IOther, "p").ping();
      time.advance(10_000);
      await settle();
      expect(silo.isActive(plainId)).toBe(false);
      expect(silo.isActive(otherId)).toBe(false);
    } finally {
      await silo.stop();
    }
  });

  /**
   * The mutator inherits the constructor route's precedence, so it is a NO-OP for a grain class
   * that declares its own `collectionAgeSeconds` - Orleans'
   * `GrainTypeSharedContext.GetCollectionAgeLimit` reads the class attribute first and returns.
   * Pinned deliberately rather than left implied: a caller reaching for a builder-time mutator is
   * usually trying to steer a specific grain class, and if that class declares an age the call
   * silently changes nothing.
   */
  it("is a no-op for a grain class that declares its own decorator age", async () => {
    const time = new FakeTimeProvider();
    const silo = siloBuilder("silo-66f", time).useClassSpecificCollectionAge("Patient", 5).build();
    await silo.start();
    try {
      await silo.getGrain(IPatient, "p").ping();
      time.advance(10_000);
      await settle();
      expect(silo.isActive(patientId)).toBe(true);
    } finally {
      await silo.stop();
    }
  });

  /**
   * The limit of what #66 buys, stated executably: because the decorator wins, two silos in one
   * process still CANNOT diverge on a grain class that declares its own age, whichever route
   * configures the map. The escape hatch is on the grain class - declare no `collectionAgeSeconds`
   * (the shape `PlainGrain` has) and the per-silo map takes charge; anything else needs a
   * separate change to how a decorator-declared age is overridden, not this mutator.
   */
  it("cannot diverge two silos on a grain class that declares its own age", async () => {
    const time = new FakeTimeProvider();
    const short = siloBuilder("silo-66l", time).useClassSpecificCollectionAge("Patient", 5).build();
    const shorter = siloBuilder("silo-66m", time)
      .useClassSpecificCollectionAge("Patient", 1)
      .build();
    await short.start();
    await shorter.start();
    try {
      await short.getGrain(IPatient, "p").ping();
      await shorter.getGrain(IPatient, "p").ping();

      time.advance(10_000);
      await settle();

      // Both hold PatientGrain's own 600s: the map never reached either activation.
      expect(short.isActive(patientId)).toBe(true);
      expect(shorter.isActive(patientId)).toBe(true);
    } finally {
      await short.stop();
      await shorter.stop();
    }
  });

  // Both routes feed one map, so they must reject one bad age identically - a caller that moves an
  // age from the constructor into a registration hook should not see the diagnostic change.
  it("rejects a bad age with the same error as the constructor route", () => {
    const time = new FakeTimeProvider();
    const viaConstructor = (): unknown => buildSilo("silo-66g", time, { Plain: 0 });
    const viaMutator = (): unknown =>
      siloBuilder("silo-66h", time).useClassSpecificCollectionAge("Plain", 0).build();
    const expected = /classSpecificCollectionAgeSeconds\["Plain"\] is 0/;
    expect(viaConstructor).toThrow(expected);
    expect(viaMutator).toThrow(expected);

    let fromConstructor = "";
    let fromMutator = "";
    try {
      viaConstructor();
    } catch (error) {
      fromConstructor = (error as Error).message;
    }
    try {
      viaMutator();
    } catch (error) {
      fromMutator = (error as Error).message;
    }
    expect(fromMutator).toBe(fromConstructor);
  });

  it("rejects a bad age in a merged map too", () => {
    const time = new FakeTimeProvider();
    expect(() =>
      siloBuilder("silo-66i", time).useClassSpecificCollectionAges({ Plain: Number.NaN }).build(),
    ).toThrow(/classSpecificCollectionAgeSeconds\["Plain"\] is NaN/);
  });

  // The motivating case from #66: two builders configured after construction, one process, same
  // grain class, different ages. This is what rewriting the process-wide metadata registry cannot do.
  it("lets two builders in one process hold different ages for the same grain type", async () => {
    const time = new FakeTimeProvider();
    const overridden = siloBuilder("silo-66j", time)
      .useClassSpecificCollectionAge("Plain", 5)
      .build();
    const untouched = siloBuilder("silo-66k", time)
      .useClassSpecificCollectionAge("Plain", 900)
      .build();
    await overridden.start();
    await untouched.start();
    try {
      await overridden.getGrain(IPlain, "p").ping();
      await untouched.getGrain(IPlain, "p").ping();

      time.advance(10_000);
      await settle();

      expect(overridden.isActive(plainId)).toBe(false);
      expect(untouched.isActive(plainId)).toBe(true);
    } finally {
      await overridden.stop();
      await untouched.stop();
    }
  });
});
