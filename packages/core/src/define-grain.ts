import { Grain } from "./grain";
import type { GrainContext } from "./grain-context";
import type { GrainInterface } from "./grain-interface";
import {
  markReentrant,
  setGrainOptions,
  type GrainConstructor,
  type GrainOptions,
} from "./grain-metadata";
import type { GrainKeyFor } from "./key-kinds";
import type { PersistentState } from "./persistent-state";
import { registerPersistentField } from "./persistent-state-metadata";
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

/** Lifecycle hooks a behaviour may return; both are optional and awaited by the runtime. */
export interface GrainLifecycle {
  onActivate(reason: ActivationReason): void | Promise<void>;
  onDeactivate(reason: DeactivationReason): void | Promise<void>;
}

/** What a grain factory returns: the interface implementation plus optional lifecycle hooks. */
export type GrainBehaviour<T> = T & Partial<GrainLifecycle>;

export interface DefineGrainOptions extends Omit<GrainOptions, "name"> {
  /** Mark every method reentrant (the functional equivalent of `@reentrant()`). */
  reentrant?: boolean;
}

// The grain instance is carried on the setup behind a private symbol so the
// facet hooks can register their field metadata against it without widening the
// public `GrainSetup` surface.
const INSTANCE = Symbol("tsva.functional.instance");

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

/**
 * Register a grain implementation written as a factory closure rather than a
 * class — the functional counterpart of the `@grain()` decorator on a `Grain`
 * subclass. The factory runs once per activation, after the context is bound and
 * before any facet read or `onActivate`; it captures per-activation state in
 * closure variables (safe without locks under the single-turn model, exactly as
 * class fields are) and returns the interface methods plus optional lifecycle
 * hooks.
 *
 * The returned constructor registers and activates through the same catalog,
 * scheduler and facet-binding machinery as a class grain, so the two styles
 * coexist; pass it to `registerGrain` like any other.
 */
export function defineGrain<T extends object>(
  name: string,
  factory: (ctx: GrainSetup) => GrainBehaviour<T>,
  options: DefineGrainOptions = {},
): new () => Grain {
  class FunctionalGrain extends Grain {
    // Run the factory once the runtime binds the context (before `preActivate`
    // reads facets), then install the returned methods/hooks as own members so
    // the catalog's method dispatch and lifecycle calls find them.
    override setContext(context: GrainContext): void {
      super.setContext(context);
      const behaviour = factory(createSetup(this, context));
      for (const key of Object.keys(behaviour)) {
        const value = (behaviour as Record<string, unknown>)[key];
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

  const { reentrant, ...grainOptions } = options;
  setGrainOptions(FunctionalGrain as GrainConstructor, name, { ...grainOptions, name });
  if (reentrant === true) markReentrant(FunctionalGrain as GrainConstructor);
  return FunctionalGrain;
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
  const fieldName = `__tsva_reducer$${stateName}`;
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
  const fieldName = `__tsva_state$${stateName}`;
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
  const fieldName = `__tsva_tx$${stateName}`;
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
    performRead: (read) => bound().performRead(read),
    performUpdate: (update) => bound().performUpdate(update),
  };
}
