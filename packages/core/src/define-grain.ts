import { Grain } from "./grain";
import type { GrainContext } from "./grain-context";
import type { SelfFilteringGrain } from "./grain-call-filter";
import { defineGrainInterface, type GrainInterface } from "./grain-interface";
import type { GrainKeyKind } from "./grain-key";
import type { InvokeMethodOptions } from "./invoke-options";
import {
  markBroadcastSubscription,
  markImplicitSubscription,
  markReentrant,
  setGrainOptions,
  type GrainConstructor,
  type GrainOptions,
} from "./grain-metadata";
import { DURABLE_JOB_HANDLER, type DurableJobHandler } from "./durable-job";
import type { KeyKindFromMarker, KeyTypeOf } from "./key-kinds";
import type { PersistentState } from "./persistent-state";
import { registerPersistentField } from "./persistent-state-metadata";
import type {
  DurableDictionary,
  DurableList,
  DurableQueue,
  DurableSet,
  DurableValue,
} from "./durable-state";
import { registerDurableField, type DurableKind } from "./durable-state-metadata";
import type { Reducer, ReducerState } from "./reducer-state";
import { registerReducerField } from "./reducer-state-metadata";
import type { TransactionalState } from "./transactional-state";
import { registerTransactionalField } from "./transactional-state-metadata";
import type { ActivationReason, DeactivationReason } from "./reasons";

/**
 * The runtime services and identity a grain factory is handed, mirroring the
 * protected surface of the `Grain` base class — `id`, `runtime`, `getGrain` —
 * but passed in explicitly instead of reached through `this`. Facet hooks
 * (`useReducerState`, `usePersistentState`) also read it.
 */
export interface GrainSetup extends GrainContext {
  getGrain<T, K extends GrainKeyKind>(def: GrainInterface<T, K>, key: KeyTypeOf<K>): T;
}

/** Lifecycle hooks a behaviour may return; both are optional and awaited by the runtime. */
export interface GrainLifecycle {
  onActivate(reason: ActivationReason): void | Promise<void>;
  onDeactivate(reason: DeactivationReason, signal?: AbortSignal): void | Promise<void>;
}

/**
 * What a grain factory returns: the interface implementation plus optional
 * lifecycle hooks and an optional self incoming-call filter (under
 * `INCOMING_CALL_FILTER`).
 */
export type GrainBehaviour<T> = T & Partial<GrainLifecycle> & Partial<SelfFilteringGrain>;

/**
 * A grain definition that *is* its own interface — the shape `defineGrain`
 * returns, mirroring `defineReducerGrain`. Pass it straight to `getGrain`; the
 * implementation constructor hangs off `grain` for `registerGrain`.
 *
 * The interface is not assigned onto the constructor instead: `Function.name` is
 * non-writable (so `Object.assign` throws in strict mode) and `ctor.name` is
 * load-bearing in the `"… is not decorated with @grain()"` diagnostics.
 */
export interface GrainDefinition<T, K extends GrainKeyKind = GrainKeyKind> extends GrainInterface<
  T,
  K
> {
  /** The implementation constructor. */
  readonly grain: new () => Grain;
}

/**
 * A method as a caller sees it across the wire: every call is remote, so a
 * synchronous return is lifted into a promise.
 */
type Remote<F> = F extends (...args: infer A) => infer R
  ? [R] extends [Promise<unknown>]
    ? (...args: A) => R
    : (...args: A) => Promise<Awaited<R>>
  : never;

/**
 * The public message surface implied by a factory's return type `R`.
 *
 * Inference widens the wire contract to whatever the factory returns, so
 * everything that is not an interface method is subtracted: `onActivate` /
 * `onDeactivate` by name, symbol-keyed system hooks (`INCOMING_CALL_FILTER`,
 * `STREAM_SUBSCRIPTION_OBSERVER`, `BROADCAST_CHANNEL_OBSERVER`,
 * `DURABLE_JOB_HANDLER`) via `Extract<keyof R, string>`, and non-function
 * members — which `setContext` does not install either. Keep passing `T`
 * explicitly for a contract that has to stay stable.
 */
export type InferredSurface<R> = {
  [K in Extract<keyof R, string> as K extends keyof GrainLifecycle
    ? never
    : R[K] extends (...args: never[]) => unknown
      ? K
      : never]: Remote<R[K]>;
};

export interface DefineGrainOptions<
  K extends GrainKeyKind = GrainKeyKind,
  T = unknown,
> extends Omit<GrainOptions, "name"> {
  /**
   * The key kind callers must address this grain with. State it once: either
   * here (letting `T` and `K` both be inferred) or as the second type argument
   * (`defineGrain<IFoo, "integer">`). Supplying both is allowed but they must
   * agree — a disagreement is a type error here, and a throw at registration
   * when the mismatch is with a same-named `defineGrainInterface`.
   */
  key?: K;
  /** Per-method invocation flags for the fused interface; only non-default methods need one. */
  interfaceOptions?: Partial<Record<keyof T & string, InvokeMethodOptions>>;
  /**
   * Pin the wire interface name when it must differ from the grain type name.
   * The interface id is `stableHash32(name)`, so a grain migrating off a
   * separate `defineGrainInterface("example.IGreeter")` must pin the old name
   * or every deployed caller's id changes with no compile-time signal.
   */
  interfaceName?: string;
  /** Interface version for rolling upgrades (defaults to 1). */
  version?: number;
  /** See `GrainInterface.extension`. Defaults to `false`. */
  extension?: boolean;
  /** Mark every method reentrant (the functional equivalent of `@reentrant()`). */
  reentrant?: boolean;
  /**
   * Stream namespaces to implicitly subscribe to (the functional equivalent of
   * `@implicitStreamSubscription`). A grain with key `K` is auto-subscribed to
   * the stream `(namespace, K)`; return a `STREAM_SUBSCRIPTION_OBSERVER` member
   * from the factory to receive its events.
   */
  implicitSubscriptions?: readonly string[];
  /**
   * Broadcast-channel namespaces to implicitly subscribe to (the functional
   * equivalent of `@implicitChannelSubscription`). A grain with key `K` receives
   * every item published to the channel `(namespace, K)`; return a
   * `BROADCAST_CHANNEL_OBSERVER` member from the factory to receive them.
   */
  implicitChannelSubscriptions?: readonly string[];
}

/** The activation a factory is currently running for, and the setup handed to it. */
interface ActiveSetup {
  readonly instance: object;
  readonly setup: GrainSetup;
}

// A stack, not a single slot: a factory may legitimately activate another grain
// (and so run another factory) while it is itself running, and the inner
// activation must not steal the outer one's facet registrations. `setContext`
// pushes and pops in a `finally`, and the window is synchronous end to end —
// nothing awaits between the push and the pop — so at most one entry per
// activation is ever live.
const setupStack: ActiveSetup[] = [];

/**
 * How many grain factories are currently running. Zero everywhere outside the
 * synchronous body of a factory; the runtime asserts on it before creating an
 * activation (see `Catalog.create`) to catch a leaked setup early.
 */
export function setupDepth(): number {
  return setupStack.length;
}

function activeSetup(hookName: string): ActiveSetup {
  const top = setupStack[setupStack.length - 1];
  if (top === undefined) {
    throw new Error(
      `${hookName}() may only be called synchronously, at the top level of a defineGrain ` +
        `factory. It was called outside a factory, after an await, or from a method body / ` +
        `onActivate.`,
    );
  }
  return top;
}

/**
 * The setup of the factory currently running — the same value the factory
 * receives as its `ctx` argument. Additive sugar for helper functions that want
 * `id` / `runtime` / `getGrain` without threading `ctx` through; capturing `ctx`
 * from the factory parameter and using it inside a method body remains the way
 * to reach the runtime after setup has finished.
 */
export function useContext(): GrainSetup {
  return activeSetup("useContext").setup;
}

function createSetup(context: GrainContext): GrainSetup {
  return {
    id: context.id,
    runtime: context.runtime,
    getGrain: (def, key) => context.runtime.getGrain(def, key),
  };
}

/**
 * Register a grain implementation written as a factory closure rather than a
 * class — the functional counterpart of the `@grain()` decorator on a `Grain`
 * subclass. The factory runs once per activation, after the context is bound and
 * before any facet read or `onActivate`; it captures per-activation state in
 * closure variables (safe without locks under the single-turn model, exactly as
 * class fields are) and returns the interface methods plus optional lifecycle
 * hooks.
 *
 * The returned definition registers and activates through the same catalog,
 * scheduler and facet-binding machinery as a class grain, so the two styles
 * coexist; pass `definition.grain` to `registerGrain` like any other constructor.
 *
 * The definition *is* the grain interface — there is nothing else to declare for
 * an in-package grain. Reach for `defineGrainInterface` instead when the contract
 * crosses a package or process boundary: inferring it from the implementation
 * means every caller imports the implementation module (and transitively its
 * storage, stream and job dependencies), which is worse coupling than the
 * duplication it removes.
 */
export function defineGrain<
  R extends Partial<GrainLifecycle> & Partial<SelfFilteringGrain>,
  K extends GrainKeyKind = "string",
>(
  name: string,
  factory: (ctx: GrainSetup) => R,
  options?: DefineGrainOptions<K, InferredSurface<R>>,
): GrainDefinition<InferredSurface<R>, K>;
export function defineGrain<T extends object, K extends GrainKeyKind = KeyKindFromMarker<T>>(
  name: string,
  factory: (ctx: GrainSetup) => GrainBehaviour<T>,
  options?: DefineGrainOptions<K, T>,
): GrainDefinition<T, K>;
export function defineGrain<T extends object>(
  name: string,
  factory: (ctx: GrainSetup) => GrainBehaviour<T>,
  options: DefineGrainOptions<GrainKeyKind, Record<string, unknown>> = {},
): GrainDefinition<T, GrainKeyKind> {
  class FunctionalGrain extends Grain {
    // Run the factory once the runtime binds the context (before `preActivate`
    // reads facets), then install the returned methods/hooks as own members so
    // the catalog's method dispatch and lifecycle calls find them.
    override setContext(context: GrainContext): void {
      super.setContext(context);
      const setup = createSetup(context);
      setupStack.push({ instance: this, setup });
      let behaviour: GrainBehaviour<T>;
      try {
        behaviour = factory(setup);
      } finally {
        setupStack.pop();
      }
      const thenable = behaviour as { then?: unknown; catch?: unknown } | undefined;
      if (typeof thenable?.then === "function") {
        // The activation fails on the synchronous error below, but the async
        // body keeps running detached: its first hook call after an await throws
        // the scope error into a promise nobody holds, which Node reports as an
        // unhandled rejection — fatal under `--unhandled-rejections=throw`, how a
        // silo process runs. Swallow it. It is not dropped information: it is the
        // same misuse the synchronous error names, phrased as a symptom, and core
        // has no ambient logger to route it to (`Logger` is injected per host, and
        // no context is bound at this point), so logging it would mean either a
        // module-level singleton or console I/O that this package does without.
        if (typeof thenable.catch === "function") {
          (thenable as Promise<unknown>).catch(() => {});
        }
        throw new Error(
          `defineGrain factory for "${name}" returned a Promise; factories must be synchronous.`,
        );
      }
      // String keys are the interface methods; symbol keys carry system hooks
      // (e.g. a self incoming-call filter under `INCOMING_CALL_FILTER`).
      const keys = [...Object.keys(behaviour), ...Object.getOwnPropertySymbols(behaviour)];
      for (const key of keys) {
        const value = (behaviour as Record<PropertyKey, unknown>)[key];
        if (typeof value !== "function") continue;
        Object.defineProperty(this, key, {
          value,
          writable: true,
          enumerable: false,
          configurable: true,
        });
      }
    }
  }

  const {
    reentrant,
    implicitSubscriptions,
    implicitChannelSubscriptions,
    key,
    interfaceOptions,
    interfaceName,
    version,
    extension,
    ...grainOptions
  } = options;
  setGrainOptions(FunctionalGrain as GrainConstructor, name, { ...grainOptions, name });
  if (reentrant === true) markReentrant(FunctionalGrain as GrainConstructor);
  for (const namespace of implicitSubscriptions ?? []) {
    markImplicitSubscription(FunctionalGrain as GrainConstructor, namespace);
  }
  for (const namespace of implicitChannelSubscriptions ?? []) {
    markBroadcastSubscription(FunctionalGrain as GrainConstructor, namespace);
  }

  // The definition is its own interface. `defineGrainInterface` merges into any
  // entry already registered under this name, so pairing a fused definition with
  // a hand-written interface module of the same name keeps that module's
  // `extension` / per-method options in the registry a receiving silo reads.
  // Passing the constructor is what separates that pairing from a second grain
  // fused under the same name, which the registry rejects rather than merging.
  const iface = defineGrainInterface<T, GrainKeyKind>(
    interfaceName ?? name,
    {
      ...(interfaceOptions !== undefined
        ? { options: interfaceOptions as Partial<Record<keyof T & string, InvokeMethodOptions>> }
        : {}),
      ...(version !== undefined ? { version } : {}),
      ...(extension !== undefined ? { extension } : {}),
      ...(key !== undefined ? { key } : {}),
    },
    FunctionalGrain,
  );
  return Object.assign(iface, { grain: FunctionalGrain });
}

/**
 * The functional counterpart of exposing a `DURABLE_JOB_HANDLER` member: register
 * the handler the runtime runs when a durable job targeting this grain fires (ADR
 * 0018). The job runs as a turn on this activation; resolving the handler means
 * the job is `Completed` (removed), throwing means `Failed` (the retry policy
 * decides), and returning `pollAfter(delay)` re-polls under supervision. Handlers
 * must be idempotent — delivery is at-least-once. Mirrors the symbol-observer
 * idiom (`BROADCAST_CHANNEL_OBSERVER`).
 */
export function useDurableJobHandler(handler: DurableJobHandler): void {
  const { instance } = activeSetup("useDurableJobHandler");
  Object.defineProperty(instance, DURABLE_JOB_HANDLER, {
    value: handler,
    writable: true,
    enumerable: false,
    configurable: true,
  });
}

export interface UseReducerStateOptions<TState, TEvent> {
  initial: () => TState;
  reduce: Reducer<TState, TEvent>;
  /** Storage provider name; defaults to the silo's default provider. */
  provider?: string;
}

/**
 * The functional counterpart of `@reducerState`. Registers a reducer facet on
 * the activation and returns a handle for it; the runtime binds and reads the
 * snapshot before `onActivate`, so the handle is live by the time any method
 * runs (it throws if read before then).
 */
export function useReducerState<TState, TEvent>(
  stateName: string,
  options: UseReducerStateOptions<TState, TEvent>,
): ReducerState<TState, TEvent> {
  const { instance } = activeSetup("useReducerState");
  const fieldName = `__thresh_reducer$${stateName}`;
  registerReducerField(instance, {
    fieldName,
    stateName,
    initial: options.initial as () => unknown,
    reduce: options.reduce as (state: unknown, event: unknown) => unknown,
    ...(options.provider !== undefined ? { provider: options.provider } : {}),
  });
  const bound = (): ReducerState<TState, TEvent> => {
    const facet = (instance as Record<string, unknown>)[fieldName] as
      | ReducerState<TState, TEvent>
      | undefined;
    if (facet === undefined) throw new Error(`reducer state "${stateName}" not yet bound`);
    return facet;
  };
  return {
    get value() {
      return bound().value;
    },
    get etag() {
      return bound().etag;
    },
    get exists() {
      return bound().exists;
    },
    raise: (event) => bound().raise(event),
    read: () => bound().read(),
    write: () => bound().write(),
  };
}

export interface UsePersistentStateOptions<TState> {
  /** Storage provider name; defaults to the silo's default provider. */
  provider?: string;
  /** Factory for the initial value before any record exists. */
  defaultValue?: () => TState;
}

/**
 * The functional counterpart of `@persistentState`. Registers a persistence
 * facet on the activation and returns a handle for it; the runtime binds and
 * reads it before `onActivate` (the handle throws if read before then).
 */
export function usePersistentState<TState>(
  stateName: string,
  options: UsePersistentStateOptions<TState> = {},
): PersistentState<TState> {
  const { instance } = activeSetup("usePersistentState");
  const fieldName = `__thresh_state$${stateName}`;
  registerPersistentField(instance, {
    fieldName,
    stateName,
    ...(options.provider !== undefined ? { provider: options.provider } : {}),
    ...(options.defaultValue !== undefined ? { defaultValue: options.defaultValue } : {}),
  });
  const bound = (): PersistentState<TState> => {
    const facet = (instance as Record<string, unknown>)[fieldName] as
      | PersistentState<TState>
      | undefined;
    if (facet === undefined) throw new Error(`persistent state "${stateName}" not yet bound`);
    return facet;
  };
  return {
    get value() {
      return bound().value;
    },
    set value(next: TState) {
      bound().value = next;
    },
    get etag() {
      return bound().etag;
    },
    get exists() {
      return bound().exists;
    },
    read: () => bound().read(),
    write: () => bound().write(),
    clear: () => bound().clear(),
  };
}

export interface UseTransactionalStateOptions<TState> {
  /** The state before any committed write. */
  initial: () => TState;
}

/**
 * The functional counterpart of `@transactionalState`. Registers a transactional
 * facet on the activation and returns a handle; the runtime binds it before
 * `onActivate`. State is reached only through `performRead` / `performUpdate`
 * inside a transaction (the handle throws if used before binding).
 */
export function useTransactionalState<TState>(
  stateName: string,
  options: UseTransactionalStateOptions<TState>,
): TransactionalState<TState> {
  const { instance } = activeSetup("useTransactionalState");
  const fieldName = `__thresh_tx$${stateName}`;
  registerTransactionalField(instance, {
    fieldName,
    stateName,
    initial: options.initial as () => unknown,
  });
  const bound = (): TransactionalState<TState> => {
    const facet = (instance as Record<string, unknown>)[fieldName] as
      | TransactionalState<TState>
      | undefined;
    if (facet === undefined) throw new Error(`transactional state "${stateName}" not yet bound`);
    return facet;
  };
  return {
    performRead: (read, readOptions) => bound().performRead(read, readOptions),
    performUpdate: (update) => bound().performUpdate(update),
  };
}

export interface UseDurableStateOptions {
  /** Journal storage provider name; defaults to the silo's default provider. */
  provider?: string;
}

/**
 * The functional counterpart of `@durableState`. Registers a durable-journalling
 * value facet on the activation and returns a handle; the runtime binds the
 * grain's single state-machine manager and replays the log before `onActivate`.
 * The handle throws
 * if used before then.
 */
/**
 * Shared plumbing for the `useDurable*` hooks: registers the field under the
 * per-kind storage-key prefix and returns a `bound()` accessor that resolves the
 * facet (throwing the per-kind "not yet bound" error if the runtime has not bound
 * it yet). Each hook wraps the result in its own facade.
 */
function createDurableFacetHook<F>(
  hookName: string,
  stateName: string,
  kind: DurableKind,
  fieldPrefix: string,
  label: string,
  options: UseDurableStateOptions,
): () => F {
  const { instance } = activeSetup(hookName);
  const fieldName = `${fieldPrefix}${stateName}`;
  registerDurableField(instance, {
    fieldName,
    stateName,
    kind,
    ...(options.provider !== undefined ? { provider: options.provider } : {}),
  });
  return (): F => {
    const facet = (instance as Record<string, unknown>)[fieldName] as F | undefined;
    if (facet === undefined) throw new Error(`${label} "${stateName}" not yet bound`);
    return facet;
  };
}

export function useDurableState<T>(
  stateName: string,
  options: UseDurableStateOptions = {},
): DurableValue<T> {
  const bound = createDurableFacetHook<DurableValue<T>>(
    "useDurableState",
    stateName,
    "value",
    "__thresh_durable$",
    "durable value",
    options,
  );
  return {
    get value() {
      return bound().value;
    },
    get: () => bound().get(),
    set: (value) => bound().set(value),
    clear: () => bound().clear(),
  };
}

/**
 * The functional counterpart of `@durableDictionary` — a journalled scalar-keyed
 * map bound and replayed before `onActivate`.
 */
export function useDurableDictionary<K, V>(
  stateName: string,
  options: UseDurableStateOptions = {},
): DurableDictionary<K, V> {
  const bound = createDurableFacetHook<DurableDictionary<K, V>>(
    "useDurableDictionary",
    stateName,
    "dictionary",
    "__thresh_durabledict$",
    "durable dictionary",
    options,
  );
  return {
    get size() {
      return bound().size;
    },
    has: (key) => bound().has(key),
    get: (key) => bound().get(key),
    entries: () => bound().entries(),
    keys: () => bound().keys(),
    values: () => bound().values(),
    set: (key, value) => bound().set(key, value),
    delete: (key) => bound().delete(key),
    clear: () => bound().clear(),
  };
}

/**
 * The functional counterpart of `@durableList` — a journalled ordered list bound
 * and replayed before `onActivate`.
 */
export function useDurableList<T>(
  stateName: string,
  options: UseDurableStateOptions = {},
): DurableList<T> {
  const bound = createDurableFacetHook<DurableList<T>>(
    "useDurableList",
    stateName,
    "list",
    "__thresh_durablelist$",
    "durable list",
    options,
  );
  return {
    get length() {
      return bound().length;
    },
    get: (index) => bound().get(index),
    toArray: () => bound().toArray(),
    contains: (value) => bound().contains(value),
    [Symbol.iterator]: () => bound()[Symbol.iterator](),
    add: (value) => bound().add(value),
    set: (index, value) => bound().set(index, value),
    insert: (index, value) => bound().insert(index, value),
    removeAt: (index) => bound().removeAt(index),
    remove: (value) => bound().remove(value),
    clear: () => bound().clear(),
  };
}

/**
 * The functional counterpart of `@durableQueue` — a journalled FIFO queue bound
 * and replayed before `onActivate`.
 */
export function useDurableQueue<T>(
  stateName: string,
  options: UseDurableStateOptions = {},
): DurableQueue<T> {
  const bound = createDurableFacetHook<DurableQueue<T>>(
    "useDurableQueue",
    stateName,
    "queue",
    "__thresh_durablequeue$",
    "durable queue",
    options,
  );
  return {
    get size() {
      return bound().size;
    },
    peek: () => bound().peek(),
    peekOrThrow: () => bound().peekOrThrow(),
    toArray: () => bound().toArray(),
    [Symbol.iterator]: () => bound()[Symbol.iterator](),
    enqueue: (value) => bound().enqueue(value),
    dequeue: () => bound().dequeue(),
    dequeueOrThrow: () => bound().dequeueOrThrow(),
    clear: () => bound().clear(),
  };
}

/**
 * The functional counterpart of `@durableSet` — a journalled set of scalar
 * values bound and replayed before `onActivate`.
 */
export function useDurableSet<T>(
  stateName: string,
  options: UseDurableStateOptions = {},
): DurableSet<T> {
  const bound = createDurableFacetHook<DurableSet<T>>(
    "useDurableSet",
    stateName,
    "set",
    "__thresh_durableset$",
    "durable set",
    options,
  );
  return {
    get size() {
      return bound().size;
    },
    has: (value) => bound().has(value),
    values: () => bound().values(),
    [Symbol.iterator]: () => bound()[Symbol.iterator](),
    add: (value) => bound().add(value),
    delete: (value) => bound().delete(value),
    clear: () => bound().clear(),
  };
}
