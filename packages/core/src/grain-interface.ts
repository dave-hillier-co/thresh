import { stableHash32 } from "./hash";
import type { InvokeMethodOptions } from "./invoke-options";

/**
 * A registered grain interface: a stable, ordered method table. Method order
 * gives each method a small integer id used on the wire, decoupling the
 * protocol from method names.
 */
export interface GrainInterface<T> {
  readonly id: number;
  readonly name: string;
  readonly methods: readonly string[];
  readonly methodId: ReadonlyMap<string, number>;
  readonly options: Readonly<Record<string, InvokeMethodOptions>>;
  /** Phantom marker so a `GrainInterface<T>` carries its interface type. */
  readonly __t?: T;
}

export interface GrainInterfaceDefinition<T> {
  methods: (keyof T & string)[];
  options?: Partial<Record<keyof T & string, InvokeMethodOptions>>;
}

// Process-wide registry so a receiving silo can resolve interfaceId -> method
// table without the caller's static type. Populated as interface modules load.
const registry = new Map<number, GrainInterface<unknown>>();

export function defineGrainInterface<T>(
  name: string,
  def: GrainInterfaceDefinition<T>,
): GrainInterface<T> {
  const methodId = new Map<string, number>();
  def.methods.forEach((m, i) => methodId.set(m, i));
  const result: GrainInterface<T> = {
    id: stableHash32(name),
    name,
    methods: [...def.methods],
    methodId,
    options: { ...(def.options ?? {}) } as Record<string, InvokeMethodOptions>,
  };
  registry.set(result.id, result as GrainInterface<unknown>);
  return result;
}

export function getGrainInterface(id: number): GrainInterface<unknown> | undefined {
  return registry.get(id);
}
