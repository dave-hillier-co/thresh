import { describe, expect, it } from "vitest";
import { GrainId } from "@thresh/core/grain-id";
import { MemoryGrainStorage } from "@thresh/persistence/memory-grain-storage";
import { ReducerStateImpl } from "@thresh/persistence/reducer-state-impl";

interface Counter {
  total: number;
  ops: number;
}
type CounterEvent = { by: number };

const reduce = (s: Counter, e: CounterEvent): Counter => ({
  total: s.total + e.by,
  ops: s.ops + 1,
});
const initial = (): Counter => ({ total: 0, ops: 0 });
const id = new GrainId("Counter", "c1");

describe("ReducerStateImpl (snapshot mode)", () => {
  it("folds an event into new immutable state without mutating the prior value", () => {
    const state = new ReducerStateImpl<Counter, CounterEvent>(
      "counter",
      id,
      new MemoryGrainStorage(),
      initial,
      reduce,
    );
    const before = state.value;
    const after = state.raise({ by: 5 });

    expect(after).toEqual({ total: 5, ops: 1 });
    expect(state.value).toBe(after);
    expect(before).toEqual({ total: 0, ops: 0 }); // the previous reference is untouched
    expect(before).not.toBe(after);
  });

  it("persists and reloads the folded snapshot — events are transient", async () => {
    const storage = new MemoryGrainStorage();
    const writer = new ReducerStateImpl<Counter, CounterEvent>(
      "counter",
      id,
      storage,
      initial,
      reduce,
    );
    writer.raise({ by: 3 });
    writer.raise({ by: 4 });
    await writer.write();

    // A fresh facet over the same store reads only the snapshot, not the events.
    const reader = new ReducerStateImpl<Counter, CounterEvent>(
      "counter",
      id,
      storage,
      initial,
      reduce,
    );
    expect(reader.value).toEqual({ total: 0, ops: 0 });
    await reader.read();
    expect(reader.value).toEqual({ total: 7, ops: 2 });
  });
});
