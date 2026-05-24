import {
  defaultGrainType,
  markReentrant,
  setGrainOptions,
  type GrainConstructor,
  type GrainOptions,
} from "./grain-metadata";
import { registerPersistentField } from "./persistent-state-metadata";

export interface PersistentStateOptions {
  /** Storage provider name; defaults to the silo's default provider. */
  provider?: string;
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
      });
    });
  };
}
