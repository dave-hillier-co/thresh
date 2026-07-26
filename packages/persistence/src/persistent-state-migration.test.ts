import { describe, expect, it } from "vitest";
import { GrainId } from "@thresh/core/grain-id";
import type { GrainStorage } from "@thresh/core/grain-storage";
import { MigrationBag } from "@thresh/core/grain-migration-participant";
import { MemoryGrainStorage } from "@thresh/persistence/memory-grain-storage";
import { PersistentStateImpl } from "@thresh/persistence/persistent-state-impl";

interface Balance {
  cents: number;
}

const id = new GrainId("Account", "a1");
const makeState = (storage: GrainStorage, name = "balance") =>
  new PersistentStateImpl<Balance>(name, id, storage, () => ({ cents: 0 }));

describe("PersistentState migration participation", () => {
  it("carries in-memory value and etag through the bag, even when unflushed", async () => {
    const storage = new MemoryGrainStorage();
    const source = makeState(storage);
    source.value.cents = 100;
    await source.write(); // establishes an etag
    source.value.cents = 250; // mutate again WITHOUT writing

    const bag = new MigrationBag();
    source.onDehydrate(bag);

    // A fresh facet on the "target" bound without reading storage restores the
    // unflushed value and the source's etag from the bag.
    const target = makeState(storage);
    target.onRehydrate(new MigrationBag(bag.data));
    expect(target.value.cents).toBe(250);
    expect(target.exists).toBe(true);
    expect(target.etag).toBe(source.etag);
  });

  it("leaves the facet untouched when the bag has no entry for it", () => {
    const target = makeState(new MemoryGrainStorage());
    target.value.cents = 7;
    target.onRehydrate(new MigrationBag());
    expect(target.value.cents).toBe(7);
  });
});
