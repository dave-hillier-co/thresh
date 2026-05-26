import type { GrainId } from "./grain-id";

/**
 * A method invocation passing through the call-filter pipeline, mirroring
 * Orleans' `IGrainCallContext` (grain call filters — see
 * `Orleans.Core.Abstractions/Core/IGrainCallContext.cs`). A filter may inspect
 * or rewrite `args` before calling `invoke()` to proceed, and read or replace
 * `result` after; not calling `invoke()` short-circuits the call.
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
  /** Proceed to the next filter, or — at the end of the chain — the method itself. */
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
 * final result. The chain advances by a shared cursor, so a filter that omits
 * `invoke()` short-circuits the rest of the chain and the terminal.
 */
export async function runCallFilters<C extends GrainCallContext>(
  filters: readonly ((context: C) => Promise<void>)[],
  context: C,
  terminal: () => Promise<unknown>,
): Promise<unknown> {
  let index = 0;
  const invoke = async (): Promise<void> => {
    if (index < filters.length) {
      const filter = filters[index++]!;
      await filter(context);
    } else {
      context.result = await terminal();
    }
  };
  context.invoke = invoke;
  await invoke();
  return context.result;
}
