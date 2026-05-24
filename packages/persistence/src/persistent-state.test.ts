import { describe, expect, it } from "vitest";
import { InconsistentStateError } from "@tsva/core/errors";
import { GrainId } from "@tsva/core/grain-id";
import type { GrainStorage } from "@tsva/core/grain-storage";
import { MemoryGrainStorage } from "@tsva/persistence/memory-grain-storage";
import { PersistentStateImpl } from "@tsva/persistence/persistent-state-impl";

interface Balance {
  cents: number;
}

const id = new GrainId("Account", "a1");
const makeState = (storage: GrainStorage, name = "balance") =>
  new PersistentStateImpl<Balance>(name, id, storage, () => ({ cents: 0 }));

describe("PersistentState over MemoryGrainStorage", () => {
  it("starts empty with a default value", async () => {
    const state = makeState(new MemoryGrainStorage());
    await state.read();
    expect(state.exists).toBe(false);
    expect(state.value.cents).toBe(0);
    expect(state.etag).toBeUndefined();
  });

  it("persists a write and reloads it on a fresh activation", async () => {
    const storage = new MemoryGrainStorage();
    const first = makeState(storage);
    first.value.cents = 100;
    await first.write();
    expect(first.exists).toBe(true);
    expect(first.etag).toBeDefined();

    const reactivated = makeState(storage);
    await reactivated.read();
    expect(reactivated.value.cents).toBe(100);
  });

  it("raises InconsistentStateError when a stale writer loses the race", async () => {
    const storage = new MemoryGrainStorage();
    const a = makeState(storage);
    await a.read();
    a.value.cents = 1;
    await a.write(); // etag v1

    const b = makeState(storage);
    await b.read(); // also at v1

    a.value.cents = 2;
    await a.write(); // bumps to v2

    b.value.cents = 3;
    await expect(b.write()).rejects.toBeInstanceOf(InconsistentStateError);
  });

  it("rejects a blind write over an existing record", async () => {
    const storage = new MemoryGrainStorage();
    const a = makeState(storage);
    a.value.cents = 1;
    await a.write();

    const blind = makeState(storage); // never read -> no etag
    blind.value.cents = 9;
    await expect(blind.write()).rejects.toBeInstanceOf(InconsistentStateError);
  });

  it("clears the record and resets to the default", async () => {
    const storage = new MemoryGrainStorage();
    const state = makeState(storage);
    state.value.cents = 50;
    await state.write();
    await state.clear();
    expect(state.exists).toBe(false);
    expect(state.value.cents).toBe(0);

    const reloaded = makeState(storage);
    await reloaded.read();
    expect(reloaded.exists).toBe(false);
  });

  it("does not let in-memory mutation leak into the stored copy", async () => {
    const storage = new MemoryGrainStorage();
    const a = makeState(storage);
    a.value.cents = 10;
    await a.write();
    a.value.cents = 999; // mutate without writing

    const b = makeState(storage);
    await b.read();
    expect(b.value.cents).toBe(10);
  });

  it("keeps named states independent", async () => {
    const storage = new MemoryGrainStorage();
    const balance = makeState(storage, "balance");
    balance.value.cents = 5;
    await balance.write();

    const limit = makeState(storage, "limit");
    await limit.read();
    expect(limit.exists).toBe(false);
  });
});
