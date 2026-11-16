/**
 * The values their authors have promised not to mutate, by identity. A `WeakSet` rather than a
 * wrapper or a hidden property, so marking changes nothing about the value's shape — its keys,
 * its prototype, what the codec writes for it — and holds nothing alive.
 */
const marked = new WeakSet<object>();

/**
 * Mark `value` as safe to share: the analogue of Orleans' `Immutable<T>` and `[Immutable]`. A
 * same-silo call made with the deep copy (`localCallSemantics: "copy"`) hands a marked value to
 * the callee, or back to the caller, as itself rather than copying it, at any depth of an
 * argument or a result. Returns `value`, so it reads inline: `grain.load(immutable(rows))`.
 *
 * It is a PROMISE the author makes, and nothing enforces it: neither side may mutate the value
 * (or anything it reaches) after the call, because both now hold the same object. Break it and a
 * same-silo callee sees the change where a callee on another silo would not.
 *
 * The mark covers this one object, by identity: not the objects it contains (they are shared
 * because their holder is), not a copy of it, and not another value deep-equal to it. It does
 * nothing on the wire — the codec ignores it — so a cross-silo call still serializes the value,
 * flattening a class instance there as always. A primitive needs no mark (it is never copied) and
 * is returned unchanged; a function cannot be marked, since the codec refuses it on every path.
 * One place the copier does not look for the mark is inside an `Error` carried as a value: an
 * error is rebuilt through the codec, properties and all, so a marked value it holds is copied.
 */
export function immutable<T>(value: T): T {
  if (typeof value === "object" && value !== null) marked.add(value);
  return value;
}

/** Whether `value` has been marked by {@link immutable}. */
export function isImmutable(value: unknown): boolean {
  return typeof value === "object" && value !== null && marked.has(value);
}
