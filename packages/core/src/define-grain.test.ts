import { describe, expect, expectTypeOf, it } from "vitest";
import {
  defineGrain,
  setupDepth,
  useContext,
  usePersistentState,
  type GrainSetup,
  type InferredSurface,
} from "@thresh/core/define-grain";
import { Grain } from "@thresh/core/grain";
import {
  grainIncomingFilter,
  INCOMING_CALL_FILTER,
  type IncomingGrainCallContext,
} from "@thresh/core/grain-call-filter";
import type { GrainContext } from "@thresh/core/grain-context";
import { GrainId } from "@thresh/core/grain-id";
import { defineGrainInterface, getGrainInterface } from "@thresh/core/grain-interface";
import type { GrainRuntime } from "@thresh/core/grain-runtime";
import { stableHash32 } from "@thresh/core/hash";
import { getGrainMetadata } from "@thresh/core/grain-metadata";
import { getPersistentFields } from "@thresh/core/persistent-state-metadata";

// The context the runtime binds is an `ActivationData` (see
// `packages/runtime/src/catalog.ts`); `@thresh/core` cannot import the runtime,
// so this is the same shape reduced to what `GrainContext` declares. The
// end-to-end path through the real catalog is characterized in
// `packages/runtime/src/define-grain.activation.test.ts`.
function context(key: string): GrainContext {
  return { id: new GrainId("Fn", key), runtime: {} as GrainRuntime };
}

/** The installed members are own properties of the instance, keyed by method name. */
function member(instance: object, name: PropertyKey): unknown {
  return (instance as Record<PropertyKey, unknown>)[name];
}

/**
 * Stand in for the runtime's facet binding: install a minimal `PersistentState`
 * shaped object on the field the hook registered, so the handle has something to
 * resolve to.
 */
function bindFacet(instance: object, fieldName: string, value: unknown): void {
  (instance as Record<string, unknown>)[fieldName] = { value };
}

describe("defineGrain — the factory", () => {
  it("does not run the factory until the context is bound", () => {
    let runs = 0;
    const Counter = defineGrain("FactoryTiming", () => {
      runs += 1;
      return {};
    }).grain;

    const instance = new Counter();
    expect(runs).toBe(0);

    instance.setContext(context("a"));
    expect(runs).toBe(1);
  });

  it("runs the factory once per activation, before onActivate", async () => {
    const events: string[] = [];
    const Counter = defineGrain("FactoryOnce", () => {
      events.push("factory");
      return {
        onActivate: () => {
          events.push("onActivate");
        },
        ping: async () => "pong",
      };
    }).grain;

    const instance = new Counter();
    instance.setContext(context("a"));
    await instance.onActivate("incoming-call");

    expect(events).toEqual(["factory", "onActivate"]);
  });

  it("gives the factory the bound identity and a getGrain bridge", () => {
    let seen: GrainSetup | undefined;
    const Probe = defineGrain("FactorySetup", (ctx) => {
      seen = ctx;
      return {};
    }).grain;

    new Probe().setContext(context("k1"));

    expect(seen?.id.key).toBe("k1");
    expect(typeof seen?.getGrain).toBe("function");
  });

  it("gives each activation its own closure state", async () => {
    interface ICounter {
      increment(): Promise<number>;
    }
    const Counter = defineGrain<ICounter>("FactoryPerActivation", () => {
      let count = 0;
      return { increment: async () => (count += 1) };
    }).grain;

    const a = new Counter() as unknown as ICounter & { setContext(c: GrainContext): void };
    const b = new Counter() as unknown as ICounter & { setContext(c: GrainContext): void };
    a.setContext(context("a"));
    b.setContext(context("b"));

    expect(await a.increment()).toBe(1);
    expect(await a.increment()).toBe(2);
    expect(await b.increment()).toBe(1);
  });
});

describe("defineGrain — installed members", () => {
  it("installs string-keyed function members as callable own properties", async () => {
    interface IGreeter {
      greet(who: string): Promise<string>;
    }
    const Greeter = defineGrain<IGreeter>("MembersString", () => ({
      greet: async (who: string) => `hello ${who}`,
    })).grain;

    const instance = new Greeter();
    instance.setContext(context("a"));

    expect(Object.hasOwn(instance, "greet")).toBe(true);
    expect(await (instance as unknown as IGreeter).greet("world")).toBe("hello world");
  });

  it("installs members non-enumerably, so they do not show up as own keys", () => {
    const Greeter = defineGrain("MembersNonEnumerable", () => ({ greet: async () => "hi" })).grain;
    const instance = new Greeter();
    instance.setContext(context("a"));

    expect(Object.keys(instance)).not.toContain("greet");
    expect(Object.getOwnPropertyDescriptor(instance, "greet")?.enumerable).toBe(false);
  });

  it("installs symbol-keyed members such as the self incoming-call filter", async () => {
    const filtered: string[] = [];
    const Filtered = defineGrain("MembersSymbol", () => ({
      [INCOMING_CALL_FILTER]: async (ctx: IncomingGrainCallContext) => {
        filtered.push(ctx.methodName);
        await ctx.invoke();
      },
      ping: async () => "pong",
    })).grain;

    const instance = new Filtered();
    instance.setContext(context("a"));

    const filter = grainIncomingFilter(instance);
    expect(filter).toBeDefined();
    await filter!({
      methodName: "ping",
      invoke: async () => undefined,
    } as unknown as IncomingGrainCallContext);
    expect(filtered).toEqual(["ping"]);
  });

  it("skips non-function members: they are not installed on the instance", () => {
    const Mixed = defineGrain("MembersNonFunction", () => ({
      version: 3,
      label: "counter",
      nested: { deep: true },
      ping: async () => "pong",
    })).grain;

    const instance = new Mixed();
    instance.setContext(context("a"));

    expect(Object.hasOwn(instance, "version")).toBe(false);
    expect(Object.hasOwn(instance, "label")).toBe(false);
    expect(Object.hasOwn(instance, "nested")).toBe(false);
    expect(member(instance, "ping")).toBeTypeOf("function");
  });

  it("leaves the base-class lifecycle in place when the factory returns no hooks", async () => {
    const Bare = defineGrain("MembersNoHooks", () => ({ ping: async () => "pong" })).grain;
    const instance = new Bare();
    instance.setContext(context("a"));

    await expect(instance.onActivate("incoming-call")).resolves.toBeUndefined();
    await expect(
      instance.onDeactivate({ code: "idle", description: "collected" }),
    ).resolves.toBeUndefined();
  });
});

describe("defineGrain — options and metadata", () => {
  it("records the grain name and options as class-grain metadata", () => {
    const Worker = defineGrain("OptionsWorker", () => ({}), {
      reentrant: true,
      implicitSubscriptions: ["chat"],
      implicitChannelSubscriptions: ["fleet"],
    }).grain;

    const metadata = getGrainMetadata(Worker);
    expect(metadata?.grainType).toBe("OptionsWorker");
    expect(metadata?.options.name).toBe("OptionsWorker");
    expect(metadata?.reentrant).toBe(true);
    expect(metadata?.implicitSubscriptions).toEqual(["chat"]);
    expect(metadata?.broadcastSubscriptions).toEqual(["fleet"]);
  });

  it("registers facet fields against the activation instance from a hook", () => {
    const Stateful = defineGrain("OptionsFacet", () => {
      const state = usePersistentState<number>("count", { defaultValue: () => 0 });
      return { get: async () => state.value };
    }).grain;

    const instance = new Stateful();
    instance.setContext(context("a"));

    expect(getPersistentFields(instance).map((f) => f.stateName)).toEqual(["count"]);
  });
});

// The hooks read an implicit setup stack pushed around the factory call, so the
// window in which they are legal is exactly the synchronous body of a factory.
// These pin the failure modes that scoping must produce.
describe("defineGrain — failure modes", () => {
  it("throws a clear error when the factory is async", () => {
    const Async = defineGrain("AsyncFactory", async () => ({ ping: async () => "pong" })).grain;
    expect(() => new Async().setContext(context("a"))).toThrow("factories must be synchronous");
  });

  it("names the grain in the async-factory error", () => {
    const Async = defineGrain("AsyncFactoryNamed", async () => ({})).grain;
    expect(() => new Async().setContext(context("a"))).toThrow(
      'defineGrain factory for "AsyncFactoryNamed" returned a Promise',
    );
  });

  it("throws a scope error naming the hook when called outside a factory", () => {
    expect(() => usePersistentState<number>("count")).toThrow(
      /^usePersistentState\(\) may only be called synchronously, at the top level of a defineGrain factory/,
    );
    expect(() => usePersistentState<number>("count")).toThrow(
      /called outside a factory, after an await, or from a method body \/ onActivate/,
    );
  });

  it("throws the scope error when a hook is called after an await inside a factory", async () => {
    let pending: Promise<void> | undefined;
    let hookError: unknown;
    const Late = defineGrain("HookAfterAwait", () => {
      pending = (async () => {
        await Promise.resolve();
        try {
          usePersistentState<number>("late");
        } catch (error) {
          hookError = error;
        }
      })();
      return { ping: async () => "pong" };
    }).grain;

    new Late().setContext(context("a"));
    await pending;

    expect(hookError).toBeInstanceOf(Error);
    expect((hookError as Error).message).toMatch(/may only be called synchronously/);
  });

  it("leaves the setup stack empty when a factory throws, so the next one still registers", () => {
    const Boom = defineGrain("ThrowingFactory", (): Record<string, never> => {
      throw new Error("boom");
    }).grain;
    expect(() => new Boom().setContext(context("a"))).toThrow("boom");
    expect(setupDepth()).toBe(0);

    const After = defineGrain("AfterThrowingFactory", () => {
      const state = usePersistentState<number>("count");
      return { get: async () => state.value };
    }).grain;
    const instance = new After();
    instance.setContext(context("b"));

    expect(getPersistentFields(instance).map((f) => f.stateName)).toEqual(["count"]);
  });

  it("leaves the setup stack empty when the factory is async", () => {
    const Async = defineGrain("AsyncFactoryStack", async () => ({})).grain;
    expect(() => new Async().setContext(context("a"))).toThrow();
    expect(setupDepth()).toBe(0);
  });

  // The synchronous guard rejects the activation, but the async factory's body
  // keeps running detached and hits the hook's scope error after its first
  // await. Nobody holds that promise, so its failure surfaces as a process-level
  // unhandled rejection — fatal under `--unhandled-rejections=throw`, which is
  // how a silo process runs. The activation failure is the synchronous throw;
  // the detached echo of it must not escalate.
  it("does not leak an unhandled rejection from an async factory's detached body", async () => {
    const Async = defineGrain("AsyncFactoryDetached", async () => {
      await Promise.resolve();
      usePersistentState<number>("late");
      return {};
    }).grain;

    // Own the process handler for the duration: with vitest's listeners left in
    // place a leaked rejection fails the whole file instead of this assertion.
    const existing = process.listeners("unhandledRejection");
    process.removeAllListeners("unhandledRejection");
    const unhandled: unknown[] = [];
    const capture = (reason: unknown): void => {
      unhandled.push(reason);
    };
    process.on("unhandledRejection", capture);
    try {
      expect(() => new Async().setContext(context("a"))).toThrow("factories must be synchronous");
      // Unhandled rejections are detected after the microtask queue drains.
      await new Promise((resolve) => setTimeout(resolve, 10));
    } finally {
      process.off("unhandledRejection", capture);
      for (const listener of existing) {
        process.on("unhandledRejection", listener as (reason: unknown) => void);
      }
    }

    expect(unhandled).toEqual([]);
  });
});

describe("defineGrain — implicit setup scope", () => {
  it("binds each activation's facet handle to its own instance", async () => {
    const Stateful = defineGrain("ScopePerInstance", () => {
      const state = usePersistentState<string>("label");
      return { get: async () => state.value };
    }).grain;

    const a = new Stateful();
    const b = new Stateful();
    a.setContext(context("a"));
    b.setContext(context("b"));

    expect(getPersistentFields(a).map((f) => f.stateName)).toEqual(["label"]);
    expect(getPersistentFields(b).map((f) => f.stateName)).toEqual(["label"]);

    // Stand in for the runtime's facet binding: the handles must read the field
    // on the instance they were created under, not on whichever ran last.
    bindFacet(a, "__thresh_state$label", "from-a");
    bindFacet(b, "__thresh_state$label", "from-b");

    expect(await (member(a, "get") as () => Promise<string>)()).toBe("from-a");
    expect(await (member(b, "get") as () => Promise<string>)()).toBe("from-b");
  });

  it("restores the outer setup when a factory activates another grain", () => {
    const Inner = defineGrain("ScopeNestedInner", () => {
      usePersistentState<number>("inner");
      return {};
    }).grain;
    let inner: Grain | undefined;
    const Outer = defineGrain("ScopeNestedOuter", () => {
      inner = new Inner();
      inner.setContext(context("inner"));
      usePersistentState<number>("outer");
      return {};
    }).grain;

    const outer = new Outer();
    outer.setContext(context("outer"));

    expect(getPersistentFields(outer).map((f) => f.stateName)).toEqual(["outer"]);
    expect(getPersistentFields(inner!).map((f) => f.stateName)).toEqual(["inner"]);
    expect(setupDepth()).toBe(0);
  });

  it("exposes the active setup through useContext()", () => {
    let seen: GrainSetup | undefined;
    const Probe = defineGrain("ScopeUseContext", () => {
      seen = useContext();
      return {};
    }).grain;

    new Probe().setContext(context("k9"));

    expect(seen?.id.key).toBe("k9");
    expect(typeof seen?.getGrain).toBe("function");
  });

  it("throws a scope error from useContext() outside a factory", () => {
    expect(() => useContext()).toThrow(/useContext\(\) may only be called synchronously/);
  });
});

/**
 * `defineGrain` returns a `GrainDefinition`: the grain interface itself, with the
 * implementation constructor hanging off it under `grain`. Callers get one thing
 * to import and pass to `getGrain` / `registerGrain`, exactly as
 * `defineReducerGrain` already returns.
 */
describe("defineGrain — the fused interface", () => {
  interface IGreeter {
    greet(who: string): Promise<string>;
  }

  it("returns the interface, with the constructor under `grain`", () => {
    const Greeter = defineGrain<IGreeter>("FusedGreeter", () => ({
      greet: async (who: string) => `hello ${who}`,
    }));

    expect(Greeter.name).toBe("FusedGreeter");
    expect(Greeter.id).toBe(stableHash32("FusedGreeter"));
    expect(Greeter.version).toBe(1);
    expect(new Greeter.grain()).toBeInstanceOf(Grain);
  });

  it("registers the interface, so a receiving silo can resolve it by id", () => {
    const Greeter = defineGrain<IGreeter>("FusedRegistered", () => ({
      greet: async (who: string) => `hi ${who}`,
    }));

    expect(getGrainInterface(Greeter.id)?.name).toBe("FusedRegistered");
  });

  it("keeps the constructor's own name for the decorator diagnostics", () => {
    // `registerGrain` reports `"${ctor.name} is not decorated with @grain()"`,
    // so the class name must survive the fusion — which is why the interface is
    // not assigned onto the constructor.
    const Greeter = defineGrain<IGreeter>("FusedCtorName", () => ({
      greet: async () => "hi",
    }));

    expect(Greeter.grain.name).toBe("FunctionalGrain");
    expect(getGrainMetadata(Greeter.grain)?.grainType).toBe("FusedCtorName");
  });

  it("pins the wire interface name with `interfaceName`", () => {
    // A grain migrating off a separate `defineGrainInterface("example.IGreeter")`
    // must keep the old id: the id is name-derived and travels on the wire.
    const Greeter = defineGrain<IGreeter>("FusedPinned", () => ({ greet: async () => "hi" }), {
      interfaceName: "example.IFusedPinned",
    });

    expect(Greeter.name).toBe("example.IFusedPinned");
    expect(Greeter.id).toBe(stableHash32("example.IFusedPinned"));
    // The grain type name is unaffected — it is what placement addresses.
    expect(getGrainMetadata(Greeter.grain)?.grainType).toBe("FusedPinned");
  });

  it("carries per-method invoke options, version and extension", () => {
    const Greeter = defineGrain<IGreeter>("FusedOptions", () => ({ greet: async () => "hi" }), {
      interfaceOptions: { greet: { readOnly: true } },
      version: 4,
      extension: true,
    });

    expect(Greeter.options.greet?.readOnly).toBe(true);
    expect(Greeter.version).toBe(4);
    expect(Greeter.extension).toBe(true);
  });

  /**
   * The key kind is stated exactly once. With the contract inferred, both type
   * parameters are inferred too, so the `key` option is the single statement —
   * and it leaves runtime evidence of the declared kind, which the type argument
   * cannot. With `T` given explicitly, TypeScript has no partial type-argument
   * inference, so the kind moves to the second type argument.
   */
  it("infers the key kind from the option when the contract is inferred", () => {
    const Counter = defineGrain("FusedInferredKey", () => ({ get: async () => 1 }), {
      key: "integer",
    });

    expectTypeOf<NonNullable<(typeof Counter)["__k"]>>().toEqualTypeOf<"integer">();
    expect(Counter.key).toBe("integer");
  });

  it("takes the key kind as the second type argument when the contract is explicit", () => {
    const Counter = defineGrain<IGreeter, "integer">("FusedIntegerKey", () => ({
      greet: async () => "hi",
    }));

    expectTypeOf<NonNullable<(typeof Counter)["__k"]>>().toEqualTypeOf<"integer">();
    expect(Counter.key).toBeUndefined();
  });

  it("rejects a `key` option that disagrees with the type argument", () => {
    const Counter = defineGrain<IGreeter, "integer">(
      "FusedKeyConflict",
      () => ({ greet: async () => "hi" }),
      // @ts-expect-error the declared kind is "integer"; the option must agree
      { key: "guid" },
    );

    // The compiler is the guard: at runtime the option is taken as written.
    expect(Counter.key).toBe("guid");
  });

  /**
   * The regression this pairing exists to catch: an interface module declares
   * `extension: true` and per-method options, the implementation module fuses a
   * definition under the same name. A clobber leaves the registry — which the
   * *receiving* silo reads — without `extension`, and the call dispatches as an
   * ordinary grain method on a remote silo with no local type error anywhere.
   */
  it("merges with a same-named defineGrainInterface rather than clobbering it", () => {
    const declared = defineGrainInterface<IGreeter>("FusedMerged", {
      extension: true,
      options: { greet: { alwaysInterleave: true } },
    });
    const fused = defineGrain<IGreeter>("FusedMerged", () => ({ greet: async () => "hi" }));

    expect(fused.id).toBe(declared.id);
    expect(getGrainInterface(declared.id)?.extension).toBe(true);
    expect(getGrainInterface(declared.id)?.options.greet?.alwaysInterleave).toBe(true);
    expect(fused.extension).toBe(true);
    expect(fused.options.greet?.alwaysInterleave).toBe(true);
  });

  /**
   * The other side of that merge: two *implementations* fused under one name are
   * not a pairing, they are a collision. The merge would fold them onto a single
   * interface id, and `getGrain(A, key)` would route to whichever grain type
   * registered last — silently, since both definitions type-check and the id is
   * the wire identity.
   */
  it("rejects two fused definitions of one name with different constructors", () => {
    defineGrain<IGreeter>("FusedDuplicate", () => ({ greet: async () => "A" }));

    expect(() =>
      defineGrain<IGreeter>("FusedDuplicate", () => ({ greet: async () => "B" })),
    ).toThrow(/FusedDuplicate[\s\S]*distinct name[\s\S]*interfaceName/);
  });

  it("lets a colliding pair coexist once one pins a distinct interfaceName", () => {
    const a = defineGrain<IGreeter>("FusedDistinct", () => ({ greet: async () => "A" }));
    const b = defineGrain<IGreeter>("FusedDistinct", () => ({ greet: async () => "B" }), {
      interfaceName: "FusedDistinct.b",
    });

    expect(a.id).not.toBe(b.id);
    expect(getGrainInterface(a.id)?.name).toBe("FusedDistinct");
    expect(getGrainInterface(b.id)?.name).toBe("FusedDistinct.b");
  });
});

/**
 * Type-level: what the inferred overload turns a factory's return type into.
 * These assertions are checked by `pnpm typecheck`, not by the unit run — the
 * vitest config has no `typecheck` block, so the run only proves the module
 * loads (see the same note in `key-kinds.test.ts`).
 *
 * The risk being pinned: inference widens the public wire contract to whatever
 * the factory happens to return, so anything that is not an interface method
 * must drop out.
 */
describe("InferredSurface", () => {
  const OBSERVER = Symbol("observer");

  interface Returned {
    onActivate(): Promise<void>;
    onDeactivate(): void;
    greet(who: string): Promise<string>;
    count(): number;
    label: string;
    nested: { deep: boolean };
    [OBSERVER]: () => void;
  }

  it("keeps only the string-keyed function members", () => {
    expectTypeOf<keyof InferredSurface<Returned>>().toEqualTypeOf<"greet" | "count">();
    expect(true).toBe(true);
  });

  it("excludes the lifecycle hooks by name", () => {
    expectTypeOf<InferredSurface<Returned>>().not.toHaveProperty("onActivate");
    expectTypeOf<InferredSurface<Returned>>().not.toHaveProperty("onDeactivate");
    expect(true).toBe(true);
  });

  it("excludes symbol keys, so system hooks stay off the wire contract", () => {
    expectTypeOf<InferredSurface<Returned>>().not.toHaveProperty(OBSERVER);
    expect(true).toBe(true);
  });

  it("excludes non-function members, which are never installed anyway", () => {
    expectTypeOf<InferredSurface<Returned>>().not.toHaveProperty("label");
    expectTypeOf<InferredSurface<Returned>>().not.toHaveProperty("nested");
    expect(true).toBe(true);
  });

  it("promise-lifts a sync return, since every call crosses the wire", () => {
    expectTypeOf<InferredSurface<Returned>["count"]>().toEqualTypeOf<() => Promise<number>>();
    expect(true).toBe(true);
  });

  it("leaves an already-async member's return type alone", () => {
    expectTypeOf<InferredSurface<Returned>["greet"]>().toEqualTypeOf<
      (who: string) => Promise<string>
    >();
    expect(true).toBe(true);
  });

  it("is what an inferred defineGrain publishes as its interface type", () => {
    const Counter = defineGrain("InferredCounter", () => {
      let count = 0;
      return {
        onActivate: () => {
          count = 0;
        },
        increment: () => (count += 1),
        get: async () => count,
      };
    });

    expectTypeOf(Counter).toExtend<{ grain: new () => Grain }>();
    expectTypeOf<NonNullable<(typeof Counter)["__t"]>>().toEqualTypeOf<{
      increment: () => Promise<number>;
      get: () => Promise<number>;
    }>();
    expect(Counter.name).toBe("InferredCounter");
  });
});
