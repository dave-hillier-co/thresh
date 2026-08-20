import { Grain } from "./grain";
import type { GrainContext } from "./grain-context";
import type { SelfFilteringGrain } from "./grain-call-filter";
import type { GrainInterface } from "./grain-interface";
import {
  markBroadcastSubscription,
  markImplicitSubscription,
  markReentrant,
  setGrainOptions,
  type GrainConstructor,
  type GrainOptions,
} from "./grain-metadata";
import { DURABLE_JOB_HANDLER, type DurableJobHandler } from "./durable-job";
import type { GrainKeyFor } from "./key-kinds";
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
import { createFieldRegistry } from "./field-registry";
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
  getGrain<T>(def: GrainInterface<T>, key: GrainKeyFor<T>): T;
}

/**
 * A hook run when the activation comes up, before the first message. Registered
 * with {@link useOnActivate}; the runtime awaits it.
 */
export type ActivateHandler = (reason: ActivationReason) => void | Promise<void>;

/**
 * A hook run when the activation is torn down. Registered with
 * {@link useOnDeactivate}; the runtime awaits it and may pass an advisory
 * `signal` (see `Grain.onDeactivate`).
 */
export type DeactivateHandler = (
  reason: DeactivationReason,
  signal?: AbortSignal,
) => void | Promise<void>;

/**
 * What a grain factory returns: the message surface — the interface
 * implementation, plus an optional self incoming-call filter (under
 * `INCOMING_CALL_FILTER`), which is itself part of how messages are handled.
 * Lifecycle is *not* returned: register it with {@link useOnActivate} /
 * {@link useOnDeactivate}, like any other facet hook.
 */
export type GrainSurface<T> = T & Partial<SelfFilteringGrain>;

export interface DefineGrainOptions extends Omit<GrainOptions, "name"> {
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

// The grain instance is carried on the setup behind a private symbol so the
// facet hooks can register their field metadata against it without widening the
// public `GrainSetup` surface.
const INSTANCE = Symbol("thresh.functional.instance");

interface InternalSetup extends GrainSetup {
  [INSTANCE]: object;
}

function createSetup(instance: object, context: GrainContext): GrainSetup {
  const setup: InternalSetup = {
    id: context.id,
    runtime: context.runtime,
    getGrain: (def, key) => context.runtime.getGrain(def, key),
    [INSTANCE]: instance,
  };
  return setup;
}

function instanceOf(ctx: GrainSetup): object {
  const instance = (ctx as InternalSetup)[INSTANCE];
  if (instance === undefined) throw new Error("useX hooks must be called with the grain setup ctx");
  return instance;
}

// Lifecycle handlers are registered per activation instance, the same way the
// state facets register their fields — the factory may call the hooks any
// number of times, including from helpers it composes.
const activateHandlers = createFieldRegistry<ActivateHandler>();
const deactivateHandlers = createFieldRegistry<DeactivateHandler>();

/**
 * Register a hook to run when the activation comes up, before the first
 * message — the functional counterpart of overriding `Grain.onActivate`.
 *
 * Hooks compose: each call adds one, and they run in registration order, so a
 * helper that sets something up runs after the setup it depends on. Every hook
 * is awaited in turn, and a hook that throws fails the activation (the
 * remaining ones do not run), exactly as a throwing `onActivate` does.
 */
export function useOnActivate(ctx: GrainSetup, handler: ActivateHandler): void {
  activateHandlers.register(instanceOf(ctx), handler);
}

/**
 * Register a hook to run when the activation is torn down — the functional
 * counterpart of overriding `Grain.onDeactivate`.
 *
 * Hooks compose LIFO: the last registered runs first, unwinding the activation
 * in the reverse of the order it was built up, so a hook always tears down
 * before whatever it was set up on top of. Every hook is awaited in turn, and
 * one that throws does not strand the hooks under it — the unwind runs to the
 * bottom and the first failure is then surfaced.
 */
export function useOnDeactivate(ctx: GrainSetup, handler: DeactivateHandler): void {
  deactivateHandlers.register(instanceOf(ctx), handler);
}

/**
 * Register a grain implementation written as a factory closure rather than a
 * class — the functional counterpart of the `@grain()` decorator on a `Grain`
 * subclass. The factory runs once per activation, after the context is bound and
 * before any facet read or `onActivate`; it captures per-activation state in
 * closure variables (safe without locks under the single-turn model, exactly as
 * class fields are) and returns the grain's message surface — nothing else.
 * Lifecycle belongs to the hooks (`useOnActivate` / `useOnDeactivate`), not to
 * the returned object.
 *
 * The returned constructor registers and activates through the same catalog,
 * scheduler and facet-binding machinery as a class grain, so the two styles
 * coexist; pass it to `registerGrain` like any other.
 */
export function defineGrain<T extends object>(
  name: string,
  factory: (ctx: GrainSetup) => GrainSurface<T>,
  options: DefineGrainOptions = {},
): new () => Grain {
  class FunctionalGrain extends Grain {
    // Run the factory once the runtime binds the context (before `preActivate`
    // reads facets), then install the returned methods as own members so the
    // catalog's method dispatch finds them.
    override setContext(context: GrainContext): void {
      super.setContext(context);
      const surface = factory(createSetup(this, context));
      // String keys are the interface methods; symbol keys carry system hooks
      // (e.g. a self incoming-call filter under `INCOMING_CALL_FILTER`).
      const keys = [...Object.keys(surface), ...Object.getOwnPropertySymbols(surface)];
      for (const key of keys) {
        const value = (surface as Record<PropertyKey, unknown>)[key];
        if (typeof value !== "function") continue;
        // A returned lifecycle hook would install as an own property and
        // silently shadow the composed runners below, dropping every hook the
        // factory registered. The surface is messages only — say so loudly.
        if (key === "onActivate" || key === "onDeactivate") {
          throw new Error(
            `grain "${name}" returned ${key} from its factory; lifecycle is registered with ` +
              "useOnActivate / useOnDeactivate, not returned in the message surface",
          );
        }
        Object.defineProperty(this, key, {
          value,
          writable: true,
          enumerable: false,
          configurable: true,
        });
      }
    }

    // Registration order coming up: a hook that throws fails the activation
    // and the ones after it never run, exactly as a throwing `onActivate` does.
    override async onActivate(reason: ActivationReason): Promise<void> {
      for (const handler of activateHandlers.getFields(this)) await handler(reason);
    }

    // The reverse of registration order going down, and — unlike activation —
    // a hook that throws does not strand the ones registered under it: the
    // stack unwinds to the bottom, then the first failure is surfaced to the
    // runtime's usual `onDeactivate` error handling.
    override async onDeactivate(reason: DeactivationReason, signal?: AbortSignal): Promise<void> {
      const handlers = deactivateHandlers.getFields(this);
      let failure: unknown;
      let failed = false;
      for (let i = handlers.length - 1; i >= 0; i--) {
        try {
          await handlers[i]!(reason, signal);
        } catch (err) {
          if (!failed) {
            failure = err;
            failed = true;
          }
        }
      }
      if (failed) throw failure;
    }
  }

  const { reentrant, implicitSubscriptions, implicitChannelSubscriptions, ...grainOptions } =
    options;
  setGrainOptions(FunctionalGrain as GrainConstructor, name, { ...grainOptions, name });
  if (reentrant === true) markReentrant(FunctionalGrain as GrainConstructor);
  for (const namespace of implicitSubscriptions ?? []) {
    markImplicitSubscription(FunctionalGrain as GrainConstructor, namespace);
  }
  for (const namespace of implicitChannelSubscriptions ?? []) {
    markBroadcastSubscription(FunctionalGrain as GrainConstructor, namespace);
  }
  return FunctionalGrain;
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
export function useDurableJobHandler(ctx: GrainSetup, handler: DurableJobHandler): void {
  const instance = instanceOf(ctx);
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
  ctx: GrainSetup,
  stateName: string,
  options: UseReducerStateOptions<TState, TEvent>,
): ReducerState<TState, TEvent> {
  const instance = instanceOf(ctx);
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
  ctx: GrainSetup,
  stateName: string,
  options: UsePersistentStateOptions<TState> = {},
): PersistentState<TState> {
  const instance = instanceOf(ctx);
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
  ctx: GrainSetup,
  stateName: string,
  options: UseTransactionalStateOptions<TState>,
): TransactionalState<TState> {
  const instance = instanceOf(ctx);
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
  ctx: GrainSetup,
  stateName: string,
  kind: DurableKind,
  fieldPrefix: string,
  label: string,
  options: UseDurableStateOptions,
): () => F {
  const instance = instanceOf(ctx);
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
  ctx: GrainSetup,
  stateName: string,
  options: UseDurableStateOptions = {},
): DurableValue<T> {
  const bound = createDurableFacetHook<DurableValue<T>>(
    ctx,
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
  ctx: GrainSetup,
  stateName: string,
  options: UseDurableStateOptions = {},
): DurableDictionary<K, V> {
  const bound = createDurableFacetHook<DurableDictionary<K, V>>(
    ctx,
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
  ctx: GrainSetup,
  stateName: string,
  options: UseDurableStateOptions = {},
): DurableList<T> {
  const bound = createDurableFacetHook<DurableList<T>>(
    ctx,
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
  ctx: GrainSetup,
  stateName: string,
  options: UseDurableStateOptions = {},
): DurableQueue<T> {
  const bound = createDurableFacetHook<DurableQueue<T>>(
    ctx,
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
  ctx: GrainSetup,
  stateName: string,
  options: UseDurableStateOptions = {},
): DurableSet<T> {
  const bound = createDurableFacetHook<DurableSet<T>>(
    ctx,
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
