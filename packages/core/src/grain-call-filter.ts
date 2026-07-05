import type { GrainId } from "./grain-id";

/**
 * A method invocation passing through the call-filter pipeline, mirroring
 * Orleans' `IGrainCallContext` (grain call filters — see
 * `Orleans.Core.Abstractions/Core/IGrainCallContext.cs`). A filter may inspect
 * or rewrite `args` before calling `invoke()` to proceed, and read or replace
 * `result` after. A filter may short-circuit by setting `result` and returning
 * without calling `invoke()`; but returning without either calling `invoke()`
 * or setting a `result` is an error (see {@link runCallFilters}), matching
 * Orleans' behaviour.
 */
export interface GrainCallContext {
  /** The grain being invoked. */
  readonly target: GrainId;
  /** The caller, if known. */
  readonly source: GrainId | undefined;
  readonly interfaceId: number;
  readonly interfaceName: string;
  readonly methodName: string;
  /** Mutable: a filter may rewrite the arguments before proceeding. */
  args: unknown[];
  /** Mutable: the call's result, available after `invoke()` and replaceable. */
  result: unknown;
  /**
   * Request-context headers for this call (Orleans `RequestContext`). On an
   * outgoing call a filter may inject into it (e.g. W3C trace context) and it is
   * sent with the request; on an incoming call it holds the headers that arrived.
   */
  headers: Record<string, string>;
  /**
   * Proceed to the next filter, or — at the end of the chain — the method
   * itself. A filter that returns without calling this and without setting
   * `result` is treated as a broken chain and {@link runCallFilters} throws.
   */
  invoke(): Promise<void>;
}

/** An incoming-call context also exposes the activation's grain instance. */
export interface IncomingGrainCallContext extends GrainCallContext {
  readonly grain: object;
}

/** Intercepts an incoming call on the grain side (Orleans `IIncomingGrainCallFilter`). */
export type IncomingGrainCallFilter = (context: IncomingGrainCallContext) => Promise<void>;

/** Intercepts an outgoing call on the caller side (Orleans `IOutgoingGrainCallFilter`). */
export type OutgoingGrainCallFilter = (context: GrainCallContext) => Promise<void>;

/**
 * A grain may filter its own incoming calls (Orleans' `IIncomingGrainCallFilter`
 * implemented by the grain) by exposing a method under this symbol; it runs as
 * the innermost filter — after the silo-wide ones, just before the method. The
 * symbol key keeps it from colliding with the grain's own (string-named) methods.
 */
export const INCOMING_CALL_FILTER = Symbol.for("tsva.incomingCallFilter");

/** A grain that filters its own incoming calls. */
export interface SelfFilteringGrain {
  [INCOMING_CALL_FILTER](context: IncomingGrainCallContext): Promise<void>;
}

/** The grain's own incoming filter, bound to it, or `undefined` if it declares none. */
export function grainIncomingFilter(instance: object): IncomingGrainCallFilter | undefined {
  const fn = (instance as Record<symbol, unknown>)[INCOMING_CALL_FILTER];
  return typeof fn === "function"
    ? (context) => (fn as IncomingGrainCallFilter).call(instance, context)
    : undefined;
}

/**
 * Run a call through the filter chain, then the terminal step (the method, or the
 * dispatch for an outgoing call). Each filter receives the context and calls
 * `context.invoke()` to continue; the terminal sets `context.result`. Returns the
 * final result. A filter may short-circuit the chain by setting `context.result`
 * and returning without calling `invoke()`. But a filter that returns having
 * neither called `invoke()` nor set a result is an error: the chain never
 * advanced and no result was produced, so this throws (mirroring Orleans'
 * `InvalidOperationException` for a broken filter chain) rather than resolving
 * with an unset result.
 */
export async function runCallFilters<C extends GrainCallContext>(
  filters: readonly ((context: C) => Promise<void>)[],
  context: C,
  terminal: () => Promise<unknown>,
): Promise<unknown> {
  let index = 0;
  let reachedTerminal = false;
  const invoke = async (): Promise<void> => {
    if (index < filters.length) {
      const filter = filters[index++]!;
      await filter(context);
    } else {
      reachedTerminal = true;
      context.result = await terminal();
    }
  };
  context.invoke = invoke;
  await invoke();
  if (!reachedTerminal && context.result === undefined) {
    throw new Error(
      `Filter for '${context.interfaceName}.${context.methodName}' did not call context.invoke()`,
    );
  }
  return context.result;
}
