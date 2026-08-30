import { CancellationTokenPlaceholder, GrainCancellationToken } from "./grain-cancellation-token";
import { grainReferenceIdentity } from "./grain-reference";

/**
 * The three shapes cancellation takes in a grain-call argument: what the caller
 * passes (`AbortSignal`, or an explicit `GrainCancellationToken`), and what a
 * cross-silo callee's `decodeValue` produces before it is bound
 * (`CancellationTokenPlaceholder`).
 */
export type CancellationValue = AbortSignal | GrainCancellationToken | CancellationTokenPlaceholder;

/**
 * Rewrite every cancellation value reachable in `value`, at ANY depth, returning
 * the rewritten graph.
 *
 * Both ends of a grain call need this. The caller side (`GrainFactory`) converts
 * an `AbortSignal` into the `GrainCancellationToken` the wire carries and records
 * the call's target on it, so a later abort cascades; the callee side
 * (`ActivationData.bindCancellationTokens`, `ClientNode`'s mirror) converts what
 * arrived back into the shape the method's signature declares. Doing this in the
 * value codec instead is not an option: the codec cannot record the targets a
 * source was sent to, so the cancel cascade would be lost (see issue #54).
 *
 * Decisions this walk makes deliberately.
 *
 * - **No copy unless something changed.** A subtree containing no cancellation
 *   value is returned by identity, so the common call allocates nothing and a
 *   same-silo callee keeps receiving the caller's own objects by reference.
 *   Where a replacement DOES happen, the containers on the path to it are
 *   rebuilt rather than mutated — a caller's own request record must not come
 *   back from a call with its `signal` field replaced by a token.
 * - **Cycle-safe, with no depth bound.** A path set (as `encodeValue` keeps)
 *   stops a cyclic graph recursing; a node already on the current path is
 *   returned unchanged rather than throwing, because a cyclic argument is legal
 *   on a same-silo call, which never serializes. Depth is otherwise unbounded,
 *   matching `encodeValue`: a bound here would only reintroduce, deeper down,
 *   the silent degradation this walk exists to remove.
 * - **Containers only.** Arrays, `Map` values, `Set` members and plain objects
 *   are walked. Class instances (including `Date`, `Uint8Array`, `GrainId`,
 *   errors and anything with a registered codec surrogate) and grain-reference
 *   proxies are NOT: rebuilding one would hand a same-silo callee a plain object
 *   where it expects its class, which is a worse fault than the one being fixed.
 *   A cancellation value buried inside a class instance therefore still does not
 *   cross a silo boundary — see `docs/deviations.md`.
 * - **`Map` keys are not walked.** A cancellation token used as a dictionary KEY
 *   is not a shape any port produces, and walking keys would double the cost of
 *   every map on the hot path.
 */
export function mapCancellationValues(
  value: unknown,
  replace: (found: CancellationValue) => unknown,
): unknown {
  return walk(value, replace, { active: new Set<object>(), memo: new Map<object, unknown>() });
}

/**
 * The state one top-level walk carries.
 *
 * `active` is the current PATH, for cycle detection; `memo` is every container already finished,
 * so a node reachable by several paths is walked once. A path set alone is not enough: released on
 * the way out, a shared subtree is re-walked per path, and a diamond-shaped argument graph costs
 * 2^depth on a code path that runs for every argument of every grain call.
 *
 * Memoising also preserves SHARING in the output - two references to one subtree stay two
 * references to one rebuilt subtree, as they were on the way in.
 */
interface WalkContext {
  readonly active: Set<object>;
  readonly memo: Map<object, unknown>;
}

function walk(
  value: unknown,
  replace: (found: CancellationValue) => unknown,
  ctx: WalkContext,
): unknown {
  if (value === null || typeof value !== "object") return value;
  if (
    value instanceof AbortSignal ||
    value instanceof GrainCancellationToken ||
    value instanceof CancellationTokenPlaceholder
  ) {
    return replace(value);
  }
  if (!isWalkableContainer(value)) return value;
  const finished = ctx.memo.get(value);
  if (finished !== undefined || ctx.memo.has(value)) return finished;
  // Already on this path: a cycle. Return it unchanged rather than throwing - a cyclic argument is
  // legal on a same-silo call, which never serializes. Not memoised: the result is only correct
  // for this path, not for a later visit that reaches the node from outside the cycle.
  if (ctx.active.has(value)) return value;
  ctx.active.add(value);
  try {
    let result: unknown;
    if (Array.isArray(value)) result = walkArray(value, replace, ctx);
    else if (value instanceof Map) result = walkMap(value, replace, ctx);
    else if (value instanceof Set) result = walkSet(value, replace, ctx);
    else result = walkObject(value as Record<string, unknown>, replace, ctx);
    ctx.memo.set(value, result);
    return result;
  } finally {
    ctx.active.delete(value);
  }
}

/** Arrays, `Map`s, `Set`s and plain objects — see `mapCancellationValues`. */
function isWalkableContainer(value: object): boolean {
  if (Array.isArray(value)) return true;
  if (value instanceof Map || value instanceof Set) return true;
  // A grain reference is a `Proxy` over an empty object, so its prototype IS
  // `Object.prototype`: without this it would read as a plain object and be
  // rebuilt into one, destroying the reference.
  if (grainReferenceIdentity(value) !== undefined) return false;
  const proto = Object.getPrototypeOf(value) as object | null;
  return proto === Object.prototype || proto === null;
}

function walkArray(
  value: readonly unknown[],
  replace: (found: CancellationValue) => unknown,
  ctx: WalkContext,
): unknown {
  let out: unknown[] | undefined;
  for (let i = 0; i < value.length; i += 1) {
    const before = value[i];
    const after = walk(before, replace, ctx);
    if (after !== before && out === undefined) out = value.slice(0, i);
    if (out !== undefined) out.push(after);
  }
  return out ?? value;
}

function walkMap(
  value: ReadonlyMap<unknown, unknown>,
  replace: (found: CancellationValue) => unknown,
  ctx: WalkContext,
): unknown {
  let out: Map<unknown, unknown> | undefined;
  let index = 0;
  for (const [k, v] of value) {
    const after = walk(v, replace, ctx);
    if (after !== v && out === undefined) out = new Map(take(value, index));
    if (out !== undefined) out.set(k, after);
    index += 1;
  }
  return out ?? value;
}

function walkSet(
  value: ReadonlySet<unknown>,
  replace: (found: CancellationValue) => unknown,
  ctx: WalkContext,
): unknown {
  let out: Set<unknown> | undefined;
  let index = 0;
  for (const v of value) {
    const after = walk(v, replace, ctx);
    if (after !== v && out === undefined) out = new Set(take(value, index));
    if (out !== undefined) out.add(after);
    index += 1;
  }
  return out ?? value;
}

/** The first `count` entries of an iterable, in iteration order. */
function take<T>(source: Iterable<T>, count: number): T[] {
  const out: T[] = [];
  for (const item of source) {
    if (out.length >= count) break;
    out.push(item);
  }
  return out;
}

function walkObject(
  value: Record<string, unknown>,
  replace: (found: CancellationValue) => unknown,
  ctx: WalkContext,
): unknown {
  let out: Record<string, unknown> | undefined;
  const keys = Object.keys(value);
  for (let i = 0; i < keys.length; i += 1) {
    const key = keys[i]!;
    const before = value[key];
    const after = walk(before, replace, ctx);
    if (after !== before && out === undefined) {
      out = {};
      for (let j = 0; j < i; j += 1) out[keys[j]!] = value[keys[j]!];
    }
    if (out !== undefined) out[key] = after;
  }
  return out ?? value;
}
