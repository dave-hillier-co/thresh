import { describe, expect, it } from "vitest";
import { GrainId } from "@tsva/core/grain-id";
import { GRAIN_REF, type GrainReferenceIdentity } from "@tsva/core/grain-reference";
import { Guid } from "@tsva/core/guid";
import { SiloAddress } from "@tsva/core/silo-address";
import { JsonSerializer } from "@tsva/messaging/json-serializer";
import { MessagePackSerializer } from "@tsva/messaging/msgpack-serializer";
import type { Serializer, SerializerOptions } from "@tsva/messaging/serializer";

const serializers: Array<[string, (o?: SerializerOptions) => Serializer]> = [
  ["JsonSerializer", (o) => new JsonSerializer(o)],
  ["MessagePackSerializer", (o) => new MessagePackSerializer(o)],
];

describe.each(serializers)("%s", (_name, make) => {
  const s = make();

  it("round-trips plain JSON-shaped data", () => {
    const value = { a: 1, b: "two", c: true, d: null, e: [1, 2, 3], f: { g: "h" } };
    expect(s.deserialize(s.serialize(value))).toEqual(value);
  });

  it("round-trips bigint", () => {
    expect(s.deserialize(s.serialize(42n))).toBe(42n);
  });

  it("round-trips a Guid", () => {
    const g = Guid.newGuid();
    expect((s.deserialize(s.serialize(g)) as Guid).equals(g)).toBe(true);
  });

  it("round-trips a Date (e.g. a reminder TickStatus)", () => {
    const d = new Date("2026-05-25T12:34:56.000Z");
    const back = s.deserialize<Date>(s.serialize(d));
    expect(back).toBeInstanceOf(Date);
    expect(back.getTime()).toBe(d.getTime());
  });

  it("round-trips a GrainId of each key kind", () => {
    for (const id of [
      new GrainId("Counter", "tenant-42"),
      new GrainId("Counter", 42n),
      new GrainId("Device", Guid.newGuid()),
    ]) {
      const back = s.deserialize<GrainId>(s.serialize(id));
      expect(back.equals(id)).toBe(true);
    }
  });

  it("round-trips a SiloAddress", () => {
    const silo = new SiloAddress("silo-2", "uid-abc", "silo-2.svc:11111");
    const back = s.deserialize<SiloAddress>(s.serialize(silo));
    expect(back.equals(silo)).toBe(true);
  });

  it("round-trips value types nested inside structures", () => {
    const value = { ids: [new GrainId("Counter", 1n)], owner: new SiloAddress("s", "u", "e") };
    const back = s.deserialize<typeof value>(s.serialize(value));
    expect(back.ids[0]!.equals(value.ids[0]!)).toBe(true);
    expect(back.owner.equals(value.owner)).toBe(true);
  });

  it("reduces a grain reference to its identity and rehydrates via the resolver", () => {
    const grainId = new GrainId("Counter", "abc");
    const fakeRef = { [GRAIN_REF]: { grainId, interfaceId: 7 } as GrainReferenceIdentity };

    let resolved: GrainReferenceIdentity | undefined;
    const withResolver = make({
      resolveGrainReference: (identity) => {
        resolved = identity;
        return `proxy:${identity.grainId.toString()}`;
      },
    });

    const back = withResolver.deserialize(withResolver.serialize(fakeRef));
    expect(resolved?.grainId.equals(grainId)).toBe(true);
    expect(resolved?.interfaceId).toBe(7);
    expect(back).toBe("proxy:Counter/abc");
  });

  it("yields a bare identity for a grain reference when no resolver is given", () => {
    const grainId = new GrainId("Counter", "abc");
    const fakeRef = { [GRAIN_REF]: { grainId, interfaceId: 7 } as GrainReferenceIdentity };
    const back = s.deserialize<GrainReferenceIdentity>(s.serialize(fakeRef));
    expect(back.grainId.equals(grainId)).toBe(true);
    expect(back.interfaceId).toBe(7);
  });
});
