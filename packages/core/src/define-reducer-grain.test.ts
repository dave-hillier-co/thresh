import { describe, expect, expectTypeOf, it } from "vitest";
import {
  call,
  defineReducerGrain,
  type ReducerClient,
  type ReducerGrain,
  type ReducerResult,
} from "@thresh/core/define-reducer-grain";
import type { GrainDefinition } from "@thresh/core/define-grain";
import type { GrainKeyKind } from "@thresh/core/grain-key";
import { Grain } from "@thresh/core/grain";
import type { GrainContext } from "@thresh/core/grain-context";
import { GrainId } from "@thresh/core/grain-id";
import { getGrainInterface } from "@thresh/core/grain-interface";
import { getGrainMetadata } from "@thresh/core/grain-metadata";
import type { GrainRuntime } from "@thresh/core/grain-runtime";
import { stableHash32 } from "@thresh/core/hash";
import { getPersistentFields } from "@thresh/core/persistent-state-metadata";
import type { PersistentState } from "@thresh/core/persistent-state";

interface Counter {
  count: number;
}

type CounterAction =
  | { type: "add"; by: number }
  | { type: "noop" }
  | { type: "forward"; to: string; by: number };

const initial = (): Counter => ({ count: 0 });

function reduce(state: Counter, action: CounterAction): ReducerResult<Counter> {
  switch (action.type) {
    case "add":
      return { state: { count: state.count + action.by } };
    case "noop":
      return { state };
    case "forward":
      return {
        state,
        effects: [call(Forwarding, action.to, { type: "add", by: action.by })],
      };
  }
}

const Forwarding = defineReducerGrain<Counter, CounterAction>("ReducerForwarding", {
  initial,
  reduce,
});

/**
 * A stand-in for the facet the runtime binds before `onActivate`, recording the
 * snapshot writes the fold triggers. `@thresh/core` cannot import the runtime, so
 * the end-to-end path is covered by `examples/bank/src/bank-reducer-dispatch.test.ts`.
 */
interface FakeState extends PersistentState<Counter> {
  readonly writes: number;
}

function fakeState(value: Counter): FakeState {
  let writes = 0;
  return {
    value,
    etag: undefined,
    exists: true,
    read: async () => {},
    write: async () => {
      writes += 1;
    },
    clear: async () => {},
    get writes() {
      return writes;
    },
  };
}

/**
 * Activate a reducer grain the way the catalog does — construct, bind the
 * context, then bind the persistent facet the factory registered — and hand back
 * the client surface plus the state it folds into.
 */
function activate(
  definition: ReducerGrain<Counter, CounterAction>,
  key: string,
  runtime: GrainRuntime = {} as GrainRuntime,
): {
  client: { dispatch(a: CounterAction): Promise<Counter>; query(): Promise<Counter> };
  state: FakeState;
} {
  const instance = new definition.grain();
  const context: GrainContext = { id: new GrainId("Reducer", key), runtime };
  instance.setContext(context);
  const [field] = getPersistentFields(instance);
  const state = fakeState(initial());
  (instance as unknown as Record<string, unknown>)[field!.fieldName] = state;
  return {
    client: instance as unknown as {
      dispatch(a: CounterAction): Promise<Counter>;
      query(): Promise<Counter>;
    },
    state,
  };
}

describe("defineReducerGrain — the fused definition", () => {
  it("returns a GrainDefinition: the interface itself, with the constructor under `grain`", () => {
    const Counters = defineReducerGrain<Counter, CounterAction>("ReducerFused", {
      initial,
      reduce,
    });

    expect(Counters.name).toBe("ReducerFused");
    expect(Counters.id).toBe(stableHash32("ReducerFused"));
    expect(Counters.version).toBe(1);
    expect(new Counters.grain()).toBeInstanceOf(Grain);
    expect(getGrainMetadata(Counters.grain)?.grainType).toBe("ReducerFused");
    // A reducer grain *is* a grain definition — the fixed `dispatch`/`query`
    // surface is the only thing that makes it a special case.
    expectTypeOf<ReducerGrain<Counter, CounterAction>>().toExtend<
      GrainDefinition<ReducerClient<Counter, CounterAction>, GrainKeyKind>
    >();
  });

  it("publishes itself in the interface registry, so the definition a caller holds is the entry a receiving silo resolves", () => {
    const Counters = defineReducerGrain<Counter, CounterAction>("ReducerRegistered", {
      initial,
      reduce,
    });

    expect(getGrainInterface(Counters.id)).toBe(Counters);
  });

  it("marks query() read-only so it does not take a write turn", () => {
    const Counters = defineReducerGrain<Counter, CounterAction>("ReducerReadOnly", {
      initial,
      reduce,
    });

    expect(Counters.options.query?.readOnly).toBe(true);
    expect(Counters.options.dispatch).toBeUndefined();
    expect(getGrainInterface(Counters.id)?.options.query?.readOnly).toBe(true);
  });

  it("forwards grain options and keeps the reducer's own options out of them", () => {
    const Counters = defineReducerGrain<Counter, CounterAction>("ReducerOptions", {
      initial,
      reduce,
      stateName: "counter-snapshot",
      provider: "memory",
      placement: "preferLocal",
    });

    const options = getGrainMetadata(Counters.grain)?.options;
    expect(options?.placement).toBe("preferLocal");
    expect(options).not.toHaveProperty("initial");
    expect(options).not.toHaveProperty("reduce");
    expect(options).not.toHaveProperty("stateName");
    expect(options).not.toHaveProperty("provider");
  });
});

describe("defineReducerGrain — the activation", () => {
  it("registers a persistence facet named after the grain, under the default provider", () => {
    const Counters = defineReducerGrain<Counter, CounterAction>("ReducerDefaultState", {
      initial,
      reduce,
    });
    const instance = new Counters.grain();

    instance.setContext({ id: new GrainId("Reducer", "a"), runtime: {} as GrainRuntime });

    expect(getPersistentFields(instance).map((f) => f.stateName)).toEqual(["ReducerDefaultState"]);
    expect(getPersistentFields(instance)[0]?.provider).toBeUndefined();
  });

  it("registers the facet under the given state name and provider", () => {
    const Counters = defineReducerGrain<Counter, CounterAction>("ReducerNamedState", {
      initial,
      reduce,
      stateName: "snapshot",
      provider: "memory",
    });
    const instance = new Counters.grain();

    instance.setContext({ id: new GrainId("Reducer", "a"), runtime: {} as GrainRuntime });

    const [field] = getPersistentFields(instance);
    expect(field?.stateName).toBe("snapshot");
    expect(field?.provider).toBe("memory");
  });

  it("folds an action into the snapshot and persists it", async () => {
    const Counters = defineReducerGrain<Counter, CounterAction>("ReducerFold", {
      initial,
      reduce,
    });
    const { client, state } = activate(Counters, "a");

    expect(await client.dispatch({ type: "add", by: 3 })).toEqual({ count: 3 });
    expect(await client.query()).toEqual({ count: 3 });
    expect(state.writes).toBe(1);
  });

  it("skips the write when the reducer returns the state unchanged", async () => {
    const Counters = defineReducerGrain<Counter, CounterAction>("ReducerUnchanged", {
      initial,
      reduce,
    });
    const { client, state } = activate(Counters, "a");

    await client.dispatch({ type: "noop" });

    expect(state.writes).toBe(0);
  });

  it("runs the effects the pure reducer returned after the fold", async () => {
    const dispatched: { key: unknown; action: unknown }[] = [];
    const runtime = {
      getGrain: (_def: unknown, key: unknown) => ({
        dispatch: async (action: unknown) => {
          dispatched.push({ key, action });
        },
      }),
    } as unknown as GrainRuntime;
    const { client } = activate(Forwarding, "a", runtime);

    await client.dispatch({ type: "forward", to: "b", by: 7 });

    expect(dispatched).toEqual([{ key: "b", action: { type: "add", by: 7 } }]);
  });
});

describe("the query read-only default", () => {
  it("survives a caller setting another option on query", () => {
    // Regression: the default was spread as `{ query: { readOnly: true }, ...interfaceOptions }`,
    // a shallow merge — so any caller entry for `query` replaced the default wholesale and
    // silently dropped `readOnly`, costing the read-only turn and its dev-mode mutation guard.
    const R = defineReducerGrain<{ n: number }, { type: "inc" }>("QueryDefaultKept", {
      initial: () => ({ n: 0 }),
      reduce: (s) => ({ state: s }),
      interfaceOptions: { query: { responseTimeout: { seconds: 5 } } },
    });

    expect(R.options?.query).toEqual({ readOnly: true, responseTimeout: { seconds: 5 } });
  });

  it("still lets a caller override it explicitly", () => {
    const R = defineReducerGrain<{ n: number }, { type: "inc" }>("QueryDefaultOverridden", {
      initial: () => ({ n: 0 }),
      reduce: (s) => ({ state: s }),
      interfaceOptions: { query: { readOnly: false } },
    });

    expect(R.options?.query).toEqual({ readOnly: false });
  });
});
