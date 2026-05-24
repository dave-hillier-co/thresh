import {
  defaultGrainType,
  markReentrant,
  setGrainOptions,
  type GrainConstructor,
  type GrainOptions,
} from "./grain-metadata";

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
