/**
 * The remotely callable method surface of a grain.
 *
 * A call arrives as a bare method name. Dispatching "whatever function that name
 * resolves to" would expose the runtime's own hooks (`onDeactivate`, `setContext`,
 * ...), framework base-class members (`JournaledGrain.raiseEvent`) and everything
 * on `Object.prototype` to any caller, including an external gateway client. So
 * only a fixed, per-type set of names is dispatched:
 *
 * - methods on the grain class's own prototype chain, stopping before a framework
 *   base class (those registered with `markFrameworkGrainBase`); methods from a
 *   user-authored base class are included;
 * - function-valued own properties of the instance (class-field arrow functions,
 *   and the surface a `defineGrain` factory installs);
 * - minus `constructor` and the names the runtime itself invokes.
 *
 * TypeScript `private` and `protected` are erased at runtime, so they do not
 * remove a method from this set.
 */

const frameworkBases = new WeakSet<object>();

/** Marks a class as part of the framework: its prototype members are never remotely callable. */
export function markFrameworkGrainBase<T extends abstract new (...args: never[]) => unknown>(
  base: T,
  hooks: readonly string[] = [],
): T {
  frameworkBases.add(base);
  if (hooks.length > 0) frameworkHooks.set(base, new Set(hooks));
  return base;
}

/**
 * Framework hooks a subclass must define itself (`JournaledGrain.initialState`), by framework base.
 * They live on the subclass's prototype, so the stop-at-the-framework-base rule does not remove
 * them; they are excluded by name for any grain that descends from that base.
 */
const frameworkHooks = new WeakMap<object, ReadonlySet<string>>();

function isFrameworkHook(proto: object, name: string): boolean {
  for (let p: object | null = proto; p !== null; p = Object.getPrototypeOf(p) as object | null) {
    const owner = Object.getOwnPropertyDescriptor(p, "constructor")?.value as object | undefined;
    if (owner !== undefined && frameworkHooks.get(owner)?.has(name) === true) return true;
  }
  return false;
}

/** Names the runtime invokes itself, by exact name, on a grain instance. */
const RUNTIME_INVOKED = new Set<string>([
  "constructor",
  "onActivate",
  "onDeactivate",
  "onDehydrate",
  "onRehydrate",
  "setContext",
  "readStateFromStorage",
  "applyUpdatesToStorage",
  "clearStoredState",
]);

const prototypeSurfaces = new WeakMap<object, ReadonlySet<string>>();

function prototypeSurface(proto: object): ReadonlySet<string> {
  const cached = prototypeSurfaces.get(proto);
  if (cached !== undefined) return cached;
  const names = new Set<string>();
  for (
    let p: object | null = proto;
    p !== null && p !== Object.prototype;
    p = Object.getPrototypeOf(p) as object | null
  ) {
    const owner = Object.getOwnPropertyDescriptor(p, "constructor")?.value as object | undefined;
    if (owner !== undefined && frameworkBases.has(owner)) break;
    for (const name of Object.getOwnPropertyNames(p)) {
      if (RUNTIME_INVOKED.has(name) || isFrameworkHook(proto, name)) continue;
      if (typeof Object.getOwnPropertyDescriptor(p, name)?.value === "function") names.add(name);
    }
  }
  prototypeSurfaces.set(proto, names);
  return names;
}

function hasOwnFunction(instance: object, name: string): boolean {
  return typeof Object.getOwnPropertyDescriptor(instance, name)?.value === "function";
}

/** Whether `name` may be invoked remotely on this grain instance. */
export function isRemotelyCallable(instance: object, name: string): boolean {
  if (RUNTIME_INVOKED.has(name)) return false;
  const proto = Object.getPrototypeOf(instance) as object | null;
  if (proto !== null && isFrameworkHook(proto, name)) return false;
  if (hasOwnFunction(instance, name)) return true;
  if (proto === null) return false;
  return prototypeSurface(proto).has(name);
}

/**
 * Whether `name` may be invoked on a client-hosted observer: the object's own and
 * user-prototype methods, never `Object.prototype`'s.
 */
export function isObserverMethod(observer: object, name: string): boolean {
  if (name === "constructor") return false;
  for (
    let p: object | null = observer;
    p !== null && p !== Object.prototype;
    p = Object.getPrototypeOf(p) as object | null
  ) {
    const d = Object.getOwnPropertyDescriptor(p, name);
    if (d !== undefined) return typeof d.value === "function";
  }
  return false;
}
