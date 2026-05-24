import type { GrainType } from "./grain-type";

export interface GrainOptions {
  /** Override the grain type name (defaults to the class name without "Grain"). */
  name?: string;
  placement?: "random" | "preferLocal" | "activationCount";
  stateless?: boolean;
  collectionAgeSeconds?: number;
}

export interface GrainMetadata {
  grainType: GrainType;
  options: GrainOptions;
  reentrant: boolean;
}

export type GrainConstructor = abstract new (...args: never[]) => object;

// Decorator order is irrelevant: options and the reentrant flag are tracked
// separately and composed on read.
const optionsRegistry = new WeakMap<
  GrainConstructor,
  { grainType: GrainType; options: GrainOptions }
>();
const reentrantRegistry = new WeakSet<GrainConstructor>();

export function setGrainOptions(
  ctor: GrainConstructor,
  grainType: GrainType,
  options: GrainOptions,
): void {
  optionsRegistry.set(ctor, { grainType, options });
}

export function markReentrant(ctor: GrainConstructor): void {
  reentrantRegistry.add(ctor);
}

export function getGrainMetadata(ctor: GrainConstructor): GrainMetadata | undefined {
  const entry = optionsRegistry.get(ctor);
  if (entry === undefined) return undefined;
  return {
    grainType: entry.grainType,
    options: entry.options,
    reentrant: reentrantRegistry.has(ctor),
  };
}

export function defaultGrainType(className: string): GrainType {
  const suffix = "Grain";
  return className.endsWith(suffix) && className.length > suffix.length
    ? className.slice(0, -suffix.length)
    : className;
}
