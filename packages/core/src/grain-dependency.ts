/**
 * A typed key for a per-silo dependency: a value a silo's builder registers
 * (`SiloBuilder.addGrainDependency`) and a functional grain's factory asks for
 * (`useDependency`). It is the functional path's counterpart of what a class
 * grain gets from a `GrainActivator` passing arguments to its constructor.
 *
 * The token is compared by identity, so two tokens made with the same `name`
 * are different keys; `name` is for diagnostics only. The `T` brand is
 * phantom: it exists so `useDependency(token)` returns the type the builder
 * was given.
 */
export interface DependencyToken<T> {
  readonly name: string;
  /** Phantom: never set at runtime. */
  readonly __type?: T;
}

/** Make a token for a dependency of type `T`; share the returned value between builder and grain. */
export function createDependencyToken<T>(name: string): DependencyToken<T> {
  return Object.freeze({ name });
}
