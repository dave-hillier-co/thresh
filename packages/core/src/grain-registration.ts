import type { GrainDefinition } from "./define-grain";
import type { GrainClass } from "./grain-class";
import type { AnyGrainInterface } from "./grain-interface";
import type { GrainKeyKind } from "./grain-key";

/**
 * The interfaces a grain type answers to. Stays a named field rather than a
 * rest argument: one constructor under several interfaces is the multi-interface
 * form, and one constructor under per-silo interface *versions* is the
 * rolling-upgrade form — both need the list to be an explicit, orderable value.
 */
export interface GrainRegistration {
  readonly interfaces: readonly AnyGrainInterface[];
}

/**
 * Anything that carries both its implementation constructor and its own
 * interface: a `defineGrain` definition or a `defineReducerGrain` one (a
 * `ReducerGrain<S, A>` *is* a `GrainDefinition`). `unknown` erases the message
 * surface — a `GrainDefinition<T>` is assignable here for any `T`, which
 * `never` would not allow.
 */
export type Registrable = GrainDefinition<unknown, GrainKeyKind>;

/**
 * Resolve the two `registerGrain` shapes to one `{ ctor, interfaces }` pair, so
 * every host (`Silo`, `ClusterNode`, `SiloBuilder`, `ClientNode`, the test
 * cluster) agrees on what a registration means.
 *
 * An explicit `interfaces` list wins outright — it is *not* unioned with the
 * definition's own interface. A definition paired with a separately declared
 * contract (`registerGrain(Counter, { interfaces: [ICounter] })`) registers the
 * one interface the caller named, exactly as `registerGrain(Counter.grain, …)`
 * does today; unioning would silently register such grains under two
 * interfaces. The definition contributes its own interface only when no list is
 * given.
 */
export function normalizeRegistration(
  definition: Registrable | GrainClass,
  registration?: GrainRegistration,
): { ctor: GrainClass; interfaces: AnyGrainInterface[] } {
  const ctor = typeof definition === "function" ? definition : definition.grain;
  if (registration === undefined) {
    if (typeof definition === "function") {
      throw new Error(
        `registerGrain(${ctor.name}) needs { interfaces: [...] }: a plain constructor ` +
          `carries no interface. Pass the interfaces it answers to, or register a ` +
          `defineGrain/defineReducerGrain definition, which carries its own.`,
      );
    }
    return { ctor, interfaces: [definition as AnyGrainInterface] };
  }
  if (registration.interfaces.length === 0) {
    throw new Error(
      `registerGrain(${ctor.name}) was given an empty interfaces list; a grain type must ` +
        `answer to at least one interface to be addressable.`,
    );
  }
  return { ctor, interfaces: [...registration.interfaces] };
}
