import {
  defaultGrainType,
  markReentrant,
  setGrainOptions,
  type GrainConstructor,
  type GrainOptions,
} from "./grain-metadata";
import { registerPersistentField } from "./persistent-state-metadata";
import { registerReducerField } from "./reducer-state-metadata";
import type { Reducer } from "./reducer-state";

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

export interface ReducerStateOptions<TState, TEvent> {
  /** The state before any event has been folded in. */
  initial: () => TState;
  /** Pure fold of an event onto state (no I/O, no grain calls, no clock). */
  reduce: Reducer<TState, TEvent>;
  /** Storage provider name; defaults to the silo's default provider. */
  provider?: string;
}

/**
 * Injects a `ReducerState<TState, TEvent>` facet into a grain field (see
 * [ADR 0006](../../docs/adr/0006-reducer-grains.md)). The runtime binds and reads
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
