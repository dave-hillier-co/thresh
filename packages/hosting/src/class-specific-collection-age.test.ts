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

const patientId = new GrainId("Patient", "p");
const plainId = new GrainId("Plain", "p");

const flush = () => new Promise((r) => setTimeout(r, 0));
async function settle(): Promise<void> {
  for (let i = 0; i < 50; i++) await flush();
}

function buildSilo(
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
    .build();
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
