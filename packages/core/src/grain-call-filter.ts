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
