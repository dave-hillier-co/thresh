import type { Grain } from "./grain";

/**
 * A grain implementation class, as a *registration* accepts it.
 *
 * Deliberately wider than `new () => Grain`: `never[]` parameters mean a class
 * whose constructor takes an options bag — the normal shape once there is no
 * constructor DI container — is registrable without a cast, and `abstract`
 * means a class the registrar never constructs itself is too. The registry
 * only reads the class's `@grain()` metadata; it is the `GrainActivator`
 * (`SiloBuilder.useGrainActivator`) that decides how an instance is made.
 *
 * The corollary is that TypeScript cannot let the *runtime* call the
 * constructor through this type. `constructGrain` in `@thresh/runtime` is the
 * one place that asserts the default `new ctor()` path, for the registered
 * classes that really do take no arguments.
 */
export type GrainClass = abstract new (...args: never[]) => Grain;
