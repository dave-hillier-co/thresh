import {
  defaultGrainType,
  markBroadcastSubscription,
  markImplicitSubscription,
  markMayInterleave,
  markReentrant,
  setGrainOptions,
  type GrainConstructor,
  type GrainOptions,
  type MayInterleavePredicate,
} from "./grain-metadata";
import { registerGrainFacetField } from "./grain-facet-metadata";
import { registerPersistentField } from "./persistent-state-metadata";
import { registerReducerField } from "./reducer-state-metadata";
import type { Reducer } from "./reducer-state";
import { registerTransactionalField } from "./transactional-state-metadata";
import { registerDurableField, type DurableKind } from "./durable-state-metadata";

export interface PersistentStateOptions {
  /** Storage provider name; defaults to the silo's default provider. */
  provider?: string;
  /** Factory for the initial value before any record exists. Defaults to `{}`. */
  defaultValue?: () => unknown;
}

/** Registers a class as a grain implementation. */
export function grain(options: GrainOptions = {}) {
  return function <T extends GrainConstructor>(value: T, context: ClassDecoratorContext): T {
    const grainType = options.name ?? defaultGrainType(context.name ?? value.name);
    setGrainOptions(value, grainType, options);
    return value;
  };
}

/** Marks a grain class as fully reentrant: all its methods may interleave. */
export function reentrant() {
  return function <T extends GrainConstructor>(value: T, _context: ClassDecoratorContext): T {
    markReentrant(value);
    return value;
  };
}

/**
 * Marks a grain class with an admission predicate for calls that would
 * otherwise queue behind the currently-running turn (Orleans' `[MayInterleave]`).
 * Given the incoming call's method name and arguments, `predicate` returns
 * whether that call may interleave with the running turn — a finer-grained
 * alternative to `@reentrant()` (which admits every call unconditionally).
 */
export function mayInterleave(predicate: MayInterleavePredicate) {
  return function <T extends GrainConstructor>(value: T, _context: ClassDecoratorContext): T {
    markMayInterleave(value, predicate);
    return value;
  };
}

/**
 * Implicitly subscribes this grain type to a stream namespace (Orleans'
 * `[ImplicitStreamSubscription]`). A grain of this type with key `K` is
 * auto-subscribed to the stream `(namespace, K)` — events delivered to that
 * stream reactivate the grain and reach the handler it exposes under
 * `STREAM_SUBSCRIPTION_OBSERVER`, with no explicit `subscribe` call. Repeatable
 * to subscribe to several namespaces.
 */
export function implicitStreamSubscription(namespace: string) {
  return function <T extends GrainConstructor>(value: T, _context: ClassDecoratorContext): T {
    markImplicitSubscription(value, namespace);
    return value;
  };
}

/**
 * Implicitly subscribes this grain type to a broadcast-channel namespace
 * (Orleans' `[ImplicitChannelSubscription]`). A grain of this type with key `K`
 * receives every item published to the channel `(namespace, K)` — the publish
 * reactivates the grain and reaches the handler it exposes under
 * `BROADCAST_CHANNEL_OBSERVER`, with no explicit subscribe. Repeatable to
 * subscribe to several namespaces.
 */
export function implicitChannelSubscription(namespace: string) {
  return function <T extends GrainConstructor>(value: T, _context: ClassDecoratorContext): T {
    markBroadcastSubscription(value, namespace);
    return value;
  };
}

/**
 * Injects a `PersistentState<T>` facet into a grain field. The runtime binds and
 * reads it before `onActivate`; the field is named by `stateName` in the store.
 */
export function persistentState(stateName: string, options: PersistentStateOptions = {}) {
  return function (_value: undefined, context: ClassFieldDecoratorContext): void {
    if (context.kind !== "field") throw new Error("@persistentState must decorate a field");
    const fieldName = String(context.name);
    context.addInitializer(function (this: unknown) {
      registerPersistentField(this as object, {
        fieldName,
        stateName,
        ...(options.provider !== undefined ? { provider: options.provider } : {}),
        ...(options.defaultValue !== undefined ? { defaultValue: options.defaultValue } : {}),
      });
    });
  };
}

/**
 * Injects a third-party facet into a grain field, resolved by a named factory
 * a silo builder registered via `addGrainFacetFactory(kind, name, factory)`
 * (the general form of what `@persistentState` does for storage: Orleans'
 * constructor-parameter `[Attribute]` facets, e.g. `[ExampleStorage("Blob")]
 * IExampleStorage<T> state`, become a decorated field here because this
 * framework has no constructor DI). `kind` names the facet family; `name`
 * selects which registered factory binds this field, defaulting to whichever
 * factory was registered under `"default"` for that kind. `config` is passed
 * through to the factory unchanged — typically a plugin defines its own
 * thin decorator wrapping this one to shape `config` (see the `ExampleStorage`
 * facet in `packages/parity` for a worked third-party example).
 */
export function grainFacet<TConfig = unknown>(kind: string, name?: string, config?: TConfig) {
  return function (_value: undefined, context: ClassFieldDecoratorContext): void {
    if (context.kind !== "field") throw new Error("@grainFacet must decorate a field");
    const fieldName = String(context.name);
    context.addInitializer(function (this: unknown) {
      registerGrainFacetField(this as object, {
        fieldName,
        kind,
        ...(name !== undefined ? { providerName: name } : {}),
        ...(config !== undefined ? { config } : {}),
      });
    });
  };
}

export interface ReducerStateOptions<TState, TEvent> {
  /** The state before any event has been folded in. */
  initial: () => TState;
  /** Pure fold of an event onto state (no I/O, no grain calls, no clock). */
  reduce: Reducer<TState, TEvent>;
  /** Storage provider name; defaults to the silo's default provider. */
  provider?: string;
}

/**
 * Injects a `ReducerState<TState, TEvent>` facet into a grain field. The runtime binds and reads
 * its snapshot before `onActivate`; command handlers `raise` events that fold
 * through `reduce` into immutable state, and `write` persists the snapshot.
 */
export function reducerState<TState, TEvent>(
  stateName: string,
  options: ReducerStateOptions<TState, TEvent>,
) {
  return function (_value: undefined, context: ClassFieldDecoratorContext): void {
    if (context.kind !== "field") throw new Error("@reducerState must decorate a field");
    const fieldName = String(context.name);
    context.addInitializer(function (this: unknown) {
      registerReducerField(this as object, {
        fieldName,
        stateName,
        initial: options.initial as () => unknown,
        reduce: options.reduce as (state: unknown, event: unknown) => unknown,
        ...(options.provider !== undefined ? { provider: options.provider } : {}),
      });
    });
  };
}

export interface TransactionalStateOptions<TState> {
  /** The state before any committed write. */
  initial: () => TState;
}

/**
 * Injects a `TransactionalState<T>` facet into a grain field. The runtime binds
 * it before `onActivate`; the grain reaches state only through `performRead` /
 * `performUpdate` inside a transaction, and the facet participates in the
 * cross-grain commit. The class-substrate counterpart of `useTransactionalState`.
 */
export function transactionalState<TState>(
  stateName: string,
  options: TransactionalStateOptions<TState>,
) {
  return function (_value: undefined, context: ClassFieldDecoratorContext): void {
    if (context.kind !== "field") throw new Error("@transactionalState must decorate a field");
    const fieldName = String(context.name);
    context.addInitializer(function (this: unknown) {
      registerTransactionalField(this as object, {
        fieldName,
        stateName,
        initial: options.initial as () => unknown,
      });
    });
  };
}

export interface DurableStateOptions {
  /** Journal storage provider name; defaults to the silo's default provider. */
  provider?: string;
}

/**
 * Shared field decorator for the durable-journalling facets. The runtime binds the
 * grain's single state-machine manager and replays its log before `onActivate`;
 * each mutation appends to the log.
 */
function durableField(kind: DurableKind, stateName: string, options: DurableStateOptions) {
  return function (_value: undefined, context: ClassFieldDecoratorContext): void {
    if (context.kind !== "field") throw new Error("durable facets must decorate a field");
    const fieldName = String(context.name);
    context.addInitializer(function (this: unknown) {
      registerDurableField(this as object, {
        fieldName,
        stateName,
        kind,
        ...(options.provider !== undefined ? { provider: options.provider } : {}),
      });
    });
  };
}

/** Injects a `DurableValue<T>` journalled facet into a grain field. */
export function durableState(stateName: string, options: DurableStateOptions = {}) {
  return durableField("value", stateName, options);
}

/** Injects a `DurableDictionary<K,V>` journalled facet into a grain field. */
export function durableDictionary(stateName: string, options: DurableStateOptions = {}) {
  return durableField("dictionary", stateName, options);
}

/** Injects a `DurableList<T>` journalled facet into a grain field. */
export function durableList(stateName: string, options: DurableStateOptions = {}) {
  return durableField("list", stateName, options);
}

/** Injects a `DurableQueue<T>` journalled facet into a grain field. */
export function durableQueue(stateName: string, options: DurableStateOptions = {}) {
  return durableField("queue", stateName, options);
}

/** Injects a `DurableSet<T>` journalled facet into a grain field. */
export function durableSet(stateName: string, options: DurableStateOptions = {}) {
  return durableField("set", stateName, options);
}
