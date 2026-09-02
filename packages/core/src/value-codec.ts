import {
  GatewayTooBusyException,
  GrainCallAbortedError,
  GrainCallError,
  GrainCallTimeoutError,
  GrainExtensionNotInstalledException,
  GrainTaskCanceledError,
  InconsistentStateError,
  LimitExceededException,
  ReadOnlyStateViolationError,
  RejectionError,
  ThreshCancellationError,
  ThreshRuntimeError,
  TransactionAbortedError,
  TransactionCascadingAbortError,
  TransactionInDoubtError,
  TransactionLockUpgradeError,
  TransactionOrphanCallError,
  TransactionReadOnlyViolatedError,
  TransactionsDisabledError,
  UnavailableExceptionFallbackException,
  type RejectionKind,
} from "./errors";
import { CancellationTokenPlaceholder, GrainCancellationToken } from "./grain-cancellation-token";
import { GrainId } from "./grain-id";
import { keyToString, type GrainKeyKind } from "./grain-key";
import { grainReferenceIdentity, type GrainReferenceIdentity } from "./grain-reference";
import { Guid } from "./guid";
import { SiloAddress } from "./silo-address";

// Tag key for the plain, transport-safe form of a runtime value type. The JSON
// and MessagePack serializers and the durable providers share this
// transformation so what they can represent stays identical.
const T = "$thresh";
// Schema-version key, carried alongside `T` on every tagged envelope. Bumping
// it (per tag, if a tag's wire shape ever needs to change) lets a decoder
// distinguish old and new shapes during a rolling upgrade. Payloads produced
// before this field existed carry no `V` at all — `versionOf` below treats
// that absence as version 1, so old payloads keep decoding unchanged.
const V = "$tsvv";
const CURRENT_VERSION = 1;

export interface CodecContext {
  /** Rehydrate a grain reference identity into a working proxy on receive. */
  resolveGrainReference?: (identity: GrainReferenceIdentity) => unknown;
}

/**
 * Thrown by `encodeValue` when a value graph contains a cycle (an object that
 * transitively contains itself). The codec walks plain objects, arrays,
 * `Map`s and `Set`s eagerly with no visited-node output cache, so a cycle
 * would otherwise recurse until the stack overflows.
 *
 * Full reference-preservation (encoding a DAG/cyclic graph and reconstructing
 * shared identity on decode, as e.g. a `$id`/`$ref` scheme would) is out of
 * scope here — this only turns an unbounded recursion into a clear error.
 */
export class CircularReferenceError extends Error {
  constructor(public readonly path: string) {
    super(
      `encodeValue: circular reference at "${path}" (cycles are not supported; break the cycle before serializing)`,
    );
    this.name = "CircularReferenceError";
  }
}

/**
 * Rebuild the runtime's OWN error classes from the generic `error` envelope, keyed by the `name`
 * they carry. This is the counterpart of Orleans' `ExceptionCodec` resolving the wire's type name
 * through its `TypeConverter`: a type the receiving process knows is reconstructed as itself, and
 * only an unresolvable one degrades to the fallback. Without it a `catch (OrleansException)`
 * ported as `isThreshRuntimeError` would stop firing the moment the call crossed a silo.
 *
 * Application types belong in `registerSurrogate`, not here — this map is closed over the classes
 * `@thresh/core/errors` declares, so a rebuild can never run application code the sender chose.
 */
const knownErrors = new Map<string, (message: string, props: Record<string, unknown>) => Error>();

/**
 * The constructor each name in {@link knownErrors} rebuilds, used at ENCODE time to prove the
 * sender's error really is that Thresh class before the wire claims the name.
 *
 * Orleans keys its rebuild on a namespace- and assembly-qualified type name (`ExceptionCodec`
 * writes `_typeConverter.Format(value.GetType())`), so an application exception can never be
 * mistaken for a framework one. `name` alone is not that: an application is free to declare
 * `class LimitExceededException extends Error` — a name Orleans itself uses — and a decoder
 * matching on the bare string would rebuild Thresh's `LimitExceededException`, a
 * `ThreshRuntimeError`, with every field `undefined`. The caller's `isThreshRuntimeError` (the
 * transliteration of `catch (OrleansException)`) would then answer TRUE for a permanent domain
 * failure and retry it — the exact hazard the error-fidelity work exists to remove.
 *
 * So the `thresh` marker is set only when `value instanceof` the registered class holds, and
 * decode consults this table only when the marker is present. An unmarked name, whatever it says,
 * takes the fallback.
 */
const knownErrorClasses = new Map<string, abstract new (...args: never[]) => Error>();

/** Force a rebuilt error's message to the one that crossed the wire, where the constructor derives it. */
function withMessage<T extends Error>(error: T, message: string): T {
  error.message = message;
  return error;
}

function registerKnownError(
  name: string,
  ctor: abstract new (...args: never[]) => Error,
  build: (message: string, props: Record<string, unknown>) => Error,
): void {
  knownErrors.set(name, build);
  knownErrorClasses.set(name, ctor);
}

/** True when `value` genuinely IS the Thresh error class registered under `name`. */
function isKnownThreshError(value: Error, name: string): boolean {
  const ctor = knownErrorClasses.get(name);
  return ctor !== undefined && value instanceof ctor;
}

registerKnownError("ThreshRuntimeError", ThreshRuntimeError, (m) => new ThreshRuntimeError(m));
registerKnownError("GrainCallError", GrainCallError, (m) => new GrainCallError(m));
registerKnownError(
  "GrainCallTimeoutError",
  GrainCallTimeoutError,
  (m) => new GrainCallTimeoutError(m),
);
registerKnownError(
  "GrainExtensionNotInstalledException",
  GrainExtensionNotInstalledException,
  (m) => new GrainExtensionNotInstalledException(m),
);
registerKnownError(
  "GatewayTooBusyException",
  GatewayTooBusyException,
  (m) => new GatewayTooBusyException(m),
);
registerKnownError(
  "RejectionError",
  RejectionError,
  (m, p) => new RejectionError(m, p.kind as RejectionKind),
);
registerKnownError("LimitExceededException", LimitExceededException, (m, p) =>
  withMessage(
    new LimitExceededException(
      p.limitName as string,
      p.currentValue as number,
      p.maxValue as number,
    ),
    m,
  ),
);
registerKnownError(
  "ThreshCancellationError",
  ThreshCancellationError,
  (m) => new ThreshCancellationError(m),
);
registerKnownError(
  "ReadOnlyStateViolationError",
  ReadOnlyStateViolationError,
  (m) => new ReadOnlyStateViolationError(m),
);
registerKnownError("TransactionsDisabledError", TransactionsDisabledError, (m) =>
  withMessage(new TransactionsDisabledError(), m),
);
registerKnownError(
  "InconsistentStateError",
  InconsistentStateError,
  (m, p) =>
    new InconsistentStateError(
      m,
      p.expectedEtag as string | undefined,
      p.storedEtag as string | undefined,
    ),
);
registerKnownError("TransactionAbortedError", TransactionAbortedError, (m, p) =>
  withMessage(new TransactionAbortedError(p.transactionId as string, ""), m),
);
registerKnownError("TransactionReadOnlyViolatedError", TransactionReadOnlyViolatedError, (m, p) =>
  withMessage(new TransactionReadOnlyViolatedError(p.transactionId as string), m),
);
registerKnownError("TransactionLockUpgradeError", TransactionLockUpgradeError, (m, p) =>
  withMessage(new TransactionLockUpgradeError(p.transactionId as string), m),
);
registerKnownError("TransactionOrphanCallError", TransactionOrphanCallError, (m, p) =>
  withMessage(
    new TransactionOrphanCallError(p.transactionId as string, p.pendingCalls as number),
    m,
  ),
);
registerKnownError("TransactionCascadingAbortError", TransactionCascadingAbortError, (m, p) =>
  withMessage(new TransactionCascadingAbortError(p.transactionId as string), m),
);
registerKnownError("TransactionInDoubtError", TransactionInDoubtError, (m, p) =>
  withMessage(new TransactionInDoubtError(p.transactionId as string), m),
);
registerKnownError("CircularReferenceError", CircularReferenceError, (m, p) =>
  withMessage(new CircularReferenceError(p.path as string), m),
);

// JavaScript's built-in error classes, the analogue of Orleans rebuilding the `System.*` namespace
// with no registration (`ExceptionSerializationOptions.SupportedNamespacePrefixes` defaults to
// `{ "Microsoft", "System", "Azure" }`). Upstream depends on it: `GrainCallFilterTests` catches
// `ArgumentOutOfRangeException` BY TYPE inside an outgoing filter on a cross-silo call. Without
// these a grain throwing `new RangeError(...)` reaches its caller as the fallback and
// `instanceof RangeError` is false, so the ported filter tests can only assert `.rejects.toThrow()`
// where upstream pins the type. Safe for the same reason as the Thresh classes above: the table is
// closed, and the `thresh` marker still requires the sender to have passed `instanceof`.
// `Error` itself is deliberately NOT here. It would swallow the diagnostic: a subclass that never
// sets `this.name` inherits "Error", so it would rebuild as a plain `Error` and the caller would
// lose the `instanceof UnavailableExceptionFallbackException` signal that says "your type was not
// reconstructed". The fallback is an `Error` anyway, so nothing is gained by claiming the base.
registerKnownError("TypeError", TypeError, (m) => new TypeError(m));
registerKnownError("RangeError", RangeError, (m) => new RangeError(m));
registerKnownError("ReferenceError", ReferenceError, (m) => new ReferenceError(m));
registerKnownError("SyntaxError", SyntaxError, (m) => new SyntaxError(m));
registerKnownError("URIError", URIError, (m) => new URIError(m));
registerKnownError("EvalError", EvalError, (m) => new EvalError(m));
registerKnownError(
  "AggregateError",
  AggregateError,
  (m, p) => new AggregateError((p.errors as unknown[]) ?? [], m),
);

/**
 * Cap on the plain-string `stack` an error carries across the wire — proportionate to the
 * diagnostic value of a stack trace without letting every error response (and every persisted
 * state that happens to nest one) grow unbounded. A truncated stack still names the outermost
 * frames, which is where a sender-side fault almost always shows.
 */
const STACK_TRACE_CAP = 8192;
const STACK_TRUNCATION_MARKER = "... (truncated)";
/** Mirrors .NET's "End of stack trace from previous location", the `ExceptionDispatchInfo.SetRemoteStackTrace` marker. */
const REMOTE_STACK_MARKER = "--- End of remote stack trace from grain call ---";

function capStack(stack: string): string {
  return stack.length > STACK_TRACE_CAP
    ? stack.slice(0, STACK_TRACE_CAP) + STACK_TRUNCATION_MARKER
    : stack;
}

/**
 * Install a decoded remote stack as the rebuilt error's own `stack`, the analogue of Orleans'
 * `ExceptionDispatchInfo.SetRemoteStackTrace`. The sender's frames are kept verbatim — appending
 * this process's own frames on top would only add rehydration noise — and an explicit marker line
 * makes clear the trace is not local.
 */
function setRemoteStackTrace(error: Error, remoteStack: string): void {
  error.stack = `${remoteStack}\n${REMOTE_STACK_MARKER}`;
}

/**
 * A registered encode/decode pair for a user or library type, keyed by a
 * stable wire tag. Distinct subclasses of a base type register under
 * distinct tags with their own `test`, so `decodeValue` reconstructs the
 * concrete subtype the tag names rather than the common base — this is the
 * codec's polymorphism resolution: which constructor runs is chosen by the
 * tag on the wire, not by a discriminator-field convention callers would
 * otherwise have to agree on separately.
 */
export interface SurrogateDescriptor<T = unknown> {
  /** Stable wire tag; must be unique across the registry and never reused for a different shape. */
  readonly tag: string;
  /** True for exactly the runtime values this surrogate should encode. */
  readonly test: (value: unknown) => boolean;
  /** Produce the plain, transport-safe fields for a matched value. */
  readonly encode: (value: T) => Record<string, unknown>;
  /** Reconstruct the runtime value from its decoded fields. */
  readonly decode: (fields: Record<string, unknown>, ctx: CodecContext) => T;
}

const surrogates = new Map<string, SurrogateDescriptor>();
// Registration order matters for `test`: later registrations are checked
// first, so a subclass registered after its base type is preferred over it.
const surrogateOrder: SurrogateDescriptor[] = [];

/** Register a surrogate encode/decode pair for a user or library type. Throws if the tag is already registered. */
export function registerSurrogate<T>(descriptor: SurrogateDescriptor<T>): void {
  if (surrogates.has(descriptor.tag)) {
    throw new Error(`registerSurrogate: tag "${descriptor.tag}" is already registered`);
  }
  surrogates.set(descriptor.tag, descriptor as SurrogateDescriptor);
  surrogateOrder.unshift(descriptor as SurrogateDescriptor);
}

/** Remove a previously registered surrogate. No-op if the tag is not registered. */
export function unregisterSurrogate(tag: string): void {
  if (surrogates.delete(tag)) {
    const i = surrogateOrder.findIndex((d) => d.tag === tag);
    if (i >= 0) surrogateOrder.splice(i, 1);
  }
}

/** Remove every registered surrogate. Mainly for test isolation. */
export function clearSurrogates(): void {
  surrogates.clear();
  surrogateOrder.length = 0;
}

function findSurrogate(value: unknown): SurrogateDescriptor | undefined {
  return surrogateOrder.find((d) => d.test(value));
}

function grainIdFields(id: GrainId): Record<string, unknown> {
  return { grainType: id.type, keyKind: id.keyKind, key: keyToString(id.key) };
}

function grainIdFrom(obj: Record<string, unknown>): GrainId {
  return GrainId.parse(
    `${obj.grainType as string}/${obj.key as string}`,
    obj.keyKind as GrainKeyKind,
  );
}

function tagged(tag: string, fields: Record<string, unknown>): Record<string, unknown> {
  return { [T]: tag, [V]: CURRENT_VERSION, ...fields };
}

// A wire payload's object keys are attacker-controlled data, not trusted JS source. `JSON.parse`
// (and msgpack's map decode) hand "__proto__" back as an ordinary OWN string key — they never
// touch the prototype LINK — but copying that key across with plain bracket assignment
// (`out[key] = value`) invokes the inherited `Object.prototype.__proto__` setter and replaces the
// target's prototype instead of adding a property. `constructor`/`prototype` are the same hazard
// one property hop away (`out.constructor.prototype.x = y` reaches every object of that
// constructor). Every decode branch that copies wire keys onto a plain object literal must skip
// these three rather than assign them.
const DANGEROUS_KEYS = new Set(["__proto__", "constructor", "prototype"]);

function isDangerousKey(key: string): boolean {
  return DANGEROUS_KEYS.has(key);
}

/**
 * How a `Uint8Array` is carried.
 *
 * The default passes it through untouched, which is what the MessagePack serializer and the
 * in-memory clone want: msgpack has a native binary type, so tagging binary would cost a third of
 * every message body. A JSON transport has no such type — `JSON.stringify` turns a typed array into
 * `{"0":1,...}` and `JSON.parse` hands back a plain object — so the JSON paths
 * (`serializeValue` and the JSON serializer) ask for the tagged base64 form instead.
 */
export interface EncodeValueOptions {
  /** Encode `Uint8Array` as a tagged base64 envelope rather than passing it through. */
  readonly binaryAsBase64?: boolean;
}

/** Replace runtime value types with tagged, transport-safe plain forms. */
export function encodeValue(value: unknown, options: EncodeValueOptions = {}): unknown {
  // A TOP-LEVEL `undefined` is a VALUE, not an absent field: it is what a grain method declared
  // `Promise<T | undefined>` (the shape every ported .NET `Task<T?>` takes) returns when it has
  // nothing, and what a `void` method returns always. Neither transport can carry it as itself —
  // MessagePack has no `undefined`, so `encode(undefined)` writes nil and the caller reads `null`;
  // `JSON.stringify(undefined)` is not a string at all — so the caller's `result === undefined`
  // guard fails and a `null` is used as if it were a value. Tag it, so the absent case survives
  // the round trip exactly as it was returned.
  //
  // Deliberately only at the TOP: `encodeInner` leaves a nested `undefined` member alone, so an
  // object's optional field still travels as an OMITTED key (MessagePack's `ignoreUndefined`,
  // `JSON.stringify`'s own omission) rather than as a tagged envelope. Tagging those too would
  // grow every message and turn "key absent" into "key present, value undefined".
  if (value === undefined) return tagged("undefined", {});
  return encodeInner(value, new Set<unknown>(), "$", options);
}

/**
 * Encode a value in a POSITIONAL slot - an array element, a `Map` key or value, a `Set` member -
 * where `undefined` is a value rather than an omittable field. An array is how a grain call's
 * arguments travel, so an optional parameter passed explicitly as `undefined` lands here: without
 * the tag it is written as nil and the callee reads `null`, and every `x === undefined` guard in
 * its body is then wrong. Object MEMBERS deliberately do not come through here (see `encodeValue`).
 */
function encodeElement(
  value: unknown,
  seen: Set<unknown>,
  path: string,
  options: EncodeValueOptions,
): unknown {
  return value === undefined ? tagged("undefined", {}) : encodeInner(value, seen, path, options);
}

function encodeInner(
  value: unknown,
  seen: Set<unknown>,
  path: string,
  options: EncodeValueOptions,
): unknown {
  if (value instanceof Uint8Array) {
    // Binary (e.g. a Message body) passes through unless the caller's transport cannot carry it.
    return options.binaryAsBase64 ? tagged("bytes", { value: bytesToBase64(value) }) : value;
  }
  if (value instanceof Date) return tagged("date", { value: value.getTime() });
  if (typeof value === "bigint") return tagged("bigint", { value: value.toString() });
  if (value instanceof Guid) return tagged("guid", { value: value.toString() });
  if (value instanceof GrainId) return tagged("grainId", grainIdFields(value));
  if (value instanceof GrainCancellationToken) {
    return tagged("cancellationToken", {
      tokenId: value.tokenId,
      cancelled: value.isCancellationRequested,
      asSignal: value.asSignal,
    });
  }
  // A placeholder re-entering `encodeValue` (e.g. a call re-forwarded to
  // another silo, over a stale directory cache, before this silo ever bound
  // it to a live `GrainCancellationToken`) must re-tag with its wire shape —
  // without this, the generic plain-object branch below strips the `$thresh`
  // tag (only `tokenId`/`cancelled` survive as bare own properties), and the
  // next hop's `decodeValue` can no longer recognise it as a cancellation
  // token, leaving the eventual callee with a signal-less plain object.
  if (value instanceof CancellationTokenPlaceholder) {
    return tagged("cancellationToken", {
      tokenId: value.tokenId,
      cancelled: value.cancelled,
      asSignal: value.asSignal,
    });
  }
  // A `DOMException` (an `AbortSignal`'s `AbortError` above all) is a built-in
  // error type with no constructor the generic object branch can rebuild, so it
  // is carried explicitly: a callee that stopped because its signal fired must
  // reach the caller AS a cancellation, not as a generic call failure whose only
  // remaining evidence is the message text.
  if (value instanceof DOMException) {
    return tagged("domException", { name: value.name, message: value.message });
  }
  // The cancellation family, carried for exactly the reason `DOMException` is: a callee that
  // stopped because its signal fired must reach the caller AS a cancellation. These have no
  // enumerable own properties, so the generic object branch below would flatten them to `{}` and
  // the caller would see a bare `GrainCallError` - making `isCancellationError` false and a
  // deliberate abort indistinguishable from a retriable call failure.
  if (value instanceof GrainCallAbortedError) {
    return tagged("callAborted", { message: value.message });
  }
  if (value instanceof GrainTaskCanceledError) {
    return tagged("taskCanceled", { message: value.message });
  }
  if (value instanceof SiloAddress) {
    return tagged("silo", {
      podName: value.podName,
      podUid: value.podUid,
      endpoint: value.endpoint,
    });
  }
  const ref = grainReferenceIdentity(value);
  if (ref !== undefined) {
    return tagged("grainRef", { interfaceId: ref.interfaceId, ...grainIdFields(ref.grainId) });
  }

  const surrogate = findSurrogate(value);
  if (surrogate !== undefined) {
    if (seen.has(value)) throw new CircularReferenceError(path);
    seen.add(value);
    try {
      const fields = surrogate.encode(value);
      const out: Record<string, unknown> = {};
      for (const [k, v] of Object.entries(fields))
        out[k] = encodeInner(v, seen, `${path}.${k}`, options);
      return tagged(surrogate.tag, out);
    } finally {
      seen.delete(value);
    }
  }

  if (value instanceof Map) {
    if (seen.has(value)) throw new CircularReferenceError(path);
    seen.add(value);
    try {
      const entries = [...value.entries()].map(([k, v], i) => [
        encodeElement(k, seen, `${path}[${i}].key`, options),
        encodeElement(v, seen, `${path}[${i}].value`, options),
      ]);
      return tagged("map", { entries });
    } finally {
      seen.delete(value);
    }
  }
  if (value instanceof Set) {
    if (seen.has(value)) throw new CircularReferenceError(path);
    seen.add(value);
    try {
      const values = [...value.values()].map((v, i) =>
        encodeElement(v, seen, `${path}[${i}]`, options),
      );
      return tagged("set", { values });
    } finally {
      seen.delete(value);
    }
  }
  if (Array.isArray(value)) {
    if (seen.has(value)) throw new CircularReferenceError(path);
    seen.add(value);
    try {
      return value.map((v, i) => encodeElement(v, seen, `${path}[${i}]`, options));
    } finally {
      seen.delete(value);
    }
  }
  // Any remaining `Error` — an application's domain error with no surrogate above all. An `Error`
  // subclass has no enumerable own properties of its own, so the generic object branch below
  // flattened it to `{}` and the caller was left with a `GrainCallError` carrying only the message
  // text: the type it discriminates on was gone, silently. Orleans never had that hazard because
  // its `ExceptionCodec` writes the type name, the message and the properties for EVERY exception
  // and only falls back to `UnavailableExceptionFallbackException` when the name does not resolve.
  // This is the same contract. It sits deliberately LAST among the error branches, so a registered
  // surrogate — and the cancellation family's own tags — still win.
  if (value instanceof Error) {
    if (seen.has(value)) throw new CircularReferenceError(path);
    seen.add(value);
    try {
      const properties: Record<string, unknown> = {};
      for (const [k, v] of Object.entries(value)) {
        // `name` and `message` travel as dedicated fields; `stack` travels as its own dedicated
        // field below (not as a generic property); `cause` is a spec-defined NON-enumerable own
        // property normally, so this loop never sees it — but a caller can still assign one
        // enumerably, and it has its own dedicated handling below, so an enumerable one is skipped
        // here rather than double-applied on decode. `errors` gets the SAME treatment, but only for
        // a genuine `AggregateError` — that is the only case with a dedicated `fields.errors` below
        // to fall back on; a plain `Error` with its own enumerable `errors` (ajv-style validation
        // errors, e.g.) has no such fallback, so skipping it here unconditionally would drop it
        // silently. Let it travel as an ordinary property instead.
        if (k === "name" || k === "message" || k === "stack" || k === "cause") continue;
        if (k === "errors" && value instanceof AggregateError) continue;
        if (isDangerousKey(k)) continue;
        properties[k] = encodeInner(v, seen, `${path}.${k}`, options);
      }
      const name = typeof value.name === "string" ? value.name : "Error";
      const fields: Record<string, unknown> = { name, message: value.message };
      // Only a value that genuinely IS the Thresh class of that name may claim it; see
      // `knownErrorClasses`. An application error that merely shares the name goes unmarked and
      // rebuilds as the fallback, never as a `ThreshRuntimeError`.
      if (isKnownThreshError(value, name)) fields.thresh = true;
      if (Object.keys(properties).length > 0) fields.properties = properties;
      // `cause` is a NON-enumerable spec property (`CreateNonEnumerableDataPropertyOrThrow`), so
      // `Object.entries` above never reaches it — checked here with `in` rather than a truthiness
      // test, so an explicit `cause: undefined` still crosses the wire as a PRESENT own property.
      // `encodeElement`, not `encodeInner`: the tagged-undefined path keeps that distinction after
      // decode too. A nested `Error` cause recurses through this same branch, so a cause chain
      // rebuilds link by link, and the shared `seen` set turns a cause cycle into the same
      // `CircularReferenceError` a cycle anywhere else in the graph gets.
      if ("cause" in value)
        fields.cause = encodeElement(value.cause, seen, `${path}.cause`, options);
      if (typeof value.stack === "string") fields.stack = capStack(value.stack);
      if (value instanceof AggregateError) {
        fields.errors = value.errors.map((e, i) =>
          encodeElement(e, seen, `${path}.errors[${i}]`, options),
        );
      }
      return tagged("error", fields);
    } finally {
      seen.delete(value);
    }
  }
  if (value !== null && typeof value === "object") {
    if (seen.has(value)) throw new CircularReferenceError(path);
    seen.add(value);
    try {
      const out: Record<string, unknown> = {};
      for (const [k, v] of Object.entries(value)) {
        if (isDangerousKey(k)) continue;
        out[k] = encodeInner(v, seen, `${path}.${k}`, options);
      }
      return out;
    } finally {
      seen.delete(value);
    }
  }
  return value;
}

/** Reverse `encodeValue`, rehydrating value types (and, optionally, grain refs). */
export function decodeValue(value: unknown, ctx: CodecContext = {}): unknown {
  if (value instanceof Uint8Array) return value;
  if (Array.isArray(value)) return value.map((v) => decodeValue(v, ctx));
  if (value === null || typeof value !== "object") return value;

  const obj = value as Record<string, unknown>;
  const tag = obj[T];
  if (typeof tag === "string") {
    switch (tag) {
      case "bigint":
        return BigInt(obj.value as string);
      case "date":
        return new Date(obj.value as number);
      case "bytes":
        return base64ToBytes(obj.value as string);
      case "guid":
        return Guid.parse(obj.value as string);
      case "grainId":
        return grainIdFrom(obj);
      case "cancellationToken":
        return new CancellationTokenPlaceholder(
          obj.tokenId as string,
          obj.cancelled as boolean,
          obj.asSignal === true,
        );
      case "undefined":
        // The top-level absent value `encodeValue` tags; see the note there.
        return undefined;
      case "domException":
        return new DOMException(obj.message as string, obj.name as string);
      case "callAborted":
        return new GrainCallAbortedError(obj.message as string);
      case "taskCanceled":
        return new GrainTaskCanceledError(obj.message as string);
      case "error": {
        const name = typeof obj.name === "string" ? obj.name : "Error";
        const message = typeof obj.message === "string" ? obj.message : "";
        const raw = obj.properties;
        const properties: Record<string, unknown> = {};
        if (raw !== null && typeof raw === "object") {
          for (const [k, v] of Object.entries(raw as Record<string, unknown>)) {
            if (isDangerousKey(k)) continue;
            properties[k] = decodeValue(v, ctx);
          }
        }
        const errors = Array.isArray(obj.errors)
          ? obj.errors.map((e) => decodeValue(e, ctx))
          : undefined;
        if (errors !== undefined) properties.errors = errors;
        // The name alone never selects a constructor: the sender must also have marked the value
        // as one of Thresh's own errors, which it can only do by passing `instanceof`.
        const known = obj.thresh === true ? knownErrors.get(name) : undefined;
        const error =
          known !== undefined
            ? known(message, properties)
            : new UnavailableExceptionFallbackException(name, message, properties, errors);
        // `cause` is installed post-construction, uniformly for every known builder and the
        // fallback alike — none of the ~20 registered builders need to see it, matching the spec's
        // non-enumerable `CreateNonEnumerableDataPropertyOrThrow` install, so a rebuilt error is
        // indistinguishable from `new Error(m, { cause })`. A nested `error` envelope recurses
        // through this same case, so a cause chain rebuilds link by link.
        if ("cause" in obj) {
          Object.defineProperty(error, "cause", {
            value: decodeValue(obj.cause, ctx),
            writable: true,
            enumerable: false,
            configurable: true,
          });
        }
        // The remote stack trace, the analogue of `ExceptionDispatchInfo.SetRemoteStackTrace`: the
        // sender's frames verbatim, plus an explicit marker so it reads as a rehydrated trace
        // rather than this process's own.
        if (typeof obj.stack === "string") setRemoteStackTrace(error, obj.stack);
        return error;
      }
      case "silo":
        return new SiloAddress(obj.podName as string, obj.podUid as string, obj.endpoint as string);
      case "map": {
        const entries = obj.entries as [unknown, unknown][];
        return new Map(entries.map(([k, v]) => [decodeValue(k, ctx), decodeValue(v, ctx)]));
      }
      case "set": {
        const values = obj.values as unknown[];
        return new Set(values.map((v) => decodeValue(v, ctx)));
      }
      case "grainRef": {
        const identity: GrainReferenceIdentity = {
          grainId: grainIdFrom(obj),
          interfaceId: obj.interfaceId as number,
        };
        return ctx.resolveGrainReference ? ctx.resolveGrainReference(identity) : identity;
      }
      default: {
        const surrogate = surrogates.get(tag);
        if (surrogate !== undefined) {
          const fields: Record<string, unknown> = {};
          for (const [k, v] of Object.entries(obj)) {
            if (k === T || k === V || isDangerousKey(k)) continue;
            fields[k] = decodeValue(v, ctx);
          }
          return surrogate.decode(fields, ctx);
        }
        // Unknown tag (e.g. produced by a newer build's surrogate this
        // process has no registration for): fall through and decode as a
        // plain object rather than throwing, so an old reader on a
        // mixed-version cluster degrades to a structural value instead of
        // failing outright.
      }
    }
  }

  const out: Record<string, unknown> = {};
  for (const [k, v] of Object.entries(obj)) {
    if (isDangerousKey(k)) continue;
    out[k] = decodeValue(v, ctx);
  }
  return out;
}

/** JSON string of a value with runtime types tagged; pair with `deserializeValue`. */
export function serializeValue(value: unknown): string {
  return JSON.stringify(encodeValue(value, { binaryAsBase64: true }));
}

/** Reverse `serializeValue`, rehydrating runtime value types. */
export function deserializeValue<T>(json: string, ctx?: CodecContext): T {
  return decodeValue(JSON.parse(json), ctx) as T;
}

const BASE64_ALPHABET = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

// Hand-rolled rather than `btoa`/`Buffer`: the core package targets ES2022 with no DOM and no
// ambient Node types, and the encoding is part of a wire shape, so it is spelled out here and
// pinned by tests rather than inherited from whichever global happens to exist.
function bytesToBase64(bytes: Uint8Array): string {
  let out = "";
  for (let i = 0; i < bytes.length; i += 3) {
    const b0 = bytes[i] as number;
    const b1 = bytes[i + 1];
    const b2 = bytes[i + 2];
    out += BASE64_ALPHABET[b0 >> 2];
    out += BASE64_ALPHABET[((b0 & 0b11) << 4) | ((b1 ?? 0) >> 4)];
    out += b1 === undefined ? "=" : BASE64_ALPHABET[((b1 & 0b1111) << 2) | ((b2 ?? 0) >> 6)];
    out += b2 === undefined ? "=" : BASE64_ALPHABET[b2 & 0b111111];
  }
  return out;
}

function base64ToBytes(text: string): Uint8Array {
  const body = text.endsWith("==")
    ? text.slice(0, -2)
    : text.endsWith("=")
      ? text.slice(0, -1)
      : text;
  const bytes = new Uint8Array((body.length * 3) >> 2);
  let acc = 0;
  let bits = 0;
  let at = 0;
  for (const ch of body) {
    const digit = BASE64_ALPHABET.indexOf(ch);
    if (digit < 0) continue;
    acc = (acc << 6) | digit;
    bits += 6;
    if (bits >= 8) {
      bits -= 8;
      bytes[at++] = (acc >> bits) & 0xff;
    }
  }
  return bytes;
}
