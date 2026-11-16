import { describe, expect, it } from "vitest";
import { GrainId } from "@thresh/core/grain-id";
import type { StateHolder } from "@thresh/core/grain-storage";
import { UnsupportedValueError } from "@thresh/core/value-codec";
import { MemoryGrainStorage } from "@thresh/persistence/memory-grain-storage";

const id = new GrainId("Account", "a");

function holder<T>(value: T): StateHolder<T> {
  return { value, exists: false, etag: undefined };
}

async function roundTrip<T>(storage: MemoryGrainStorage, value: T): Promise<T> {
  await storage.write("s", id, holder(value));
  const out = holder<T | undefined>(undefined);
  await storage.read("s", id, out);
  return out.value as T;
}

// The in-memory store stands in for the durable ones in tests, so it must show a test the same
// values and the same failures the Redis and Postgres stores would: it round-trips state through
// the value codec rather than structuredClone.
describe("MemoryGrainStorage", () => {
  it("reads a stored GrainId back as a GrainId", async () => {
    const back = await roundTrip(new MemoryGrainStorage(), { owner: new GrainId("User", "u1") });
    expect(back.owner).toBeInstanceOf(GrainId);
    expect(back.owner.equals(new GrainId("User", "u1"))).toBe(true);
  });

  it("refuses a RegExp with the UnsupportedValueError the durable stores raise", async () => {
    const storage = new MemoryGrainStorage();
    await expect(storage.write("s", id, holder({ re: /x+/g }))).rejects.toBeInstanceOf(
      UnsupportedValueError,
    );
  });

  it("does not alias the caller's object on write or on read", async () => {
    const storage = new MemoryGrainStorage();
    const written = { items: [1, 2] };
    const state = holder(written);
    await storage.write("s", id, state);
    written.items.push(3);

    const first = holder<{ items: number[] } | undefined>(undefined);
    await storage.read("s", id, first);
    expect(first.value?.items).toEqual([1, 2]);

    first.value?.items.push(99);
    const second = holder<{ items: number[] } | undefined>(undefined);
    await storage.read("s", id, second);
    expect(second.value?.items).toEqual([1, 2]);
  });

  it("keeps Map, Set, Date, bigint and reserved-key data intact", async () => {
    const value = {
      m: new Map([["k", 1]]),
      s: new Set([1]),
      d: new Date(5),
      n: 10n,
      tagged: { $thresh: "date", value: 5 },
    };
    expect(await roundTrip(new MemoryGrainStorage(), value)).toEqual(value);
  });
});
