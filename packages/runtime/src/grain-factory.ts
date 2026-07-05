import {
  runCallFilters,
  type GrainCallContext,
  type OutgoingGrainCallFilter,
} from "@tsva/core/grain-call-filter";
import { GrainId } from "@tsva/core/grain-id";
import type { GrainInterface } from "@tsva/core/grain-interface";
import type { GrainKey } from "@tsva/core/grain-key";
import { GRAIN_REF } from "@tsva/core/grain-reference";
import type { GrainType } from "@tsva/core/grain-type";
import { Guid } from "@tsva/core/guid";
import type { InvokeMethodOptions } from "@tsva/core/invoke-options";
import type { InvocationRequest } from "@tsva/core/request";
import type { TransactionInfo } from "@tsva/core/transaction-info";
import type { Dispatcher } from "@tsva/runtime/dispatcher";
import { invocationContext } from "@tsva/runtime/invocation-context";
import type { TransactionAgent } from "@tsva/runtime/transaction-agent";

const newChainId = () => Guid.newGuid().toString();

/**
 * Builds grain references as ES `Proxy` objects. Each intercepted method call
 * becomes an `InvocationRequest` dispatched through the runtime; caller identity
 * and the call-chain reentrancy id are read from the ambient invocation context.
 */
export class GrainFactory {
  private dispatcher: Dispatcher | undefined;
  private transactionAgent: TransactionAgent | undefined;
  private outgoingCallFilters: readonly OutgoingGrainCallFilter[] = [];

  constructor(private readonly resolveGrainType: (interfaceId: number) => GrainType) {}

  setDispatcher(dispatcher: Dispatcher): void {
    this.dispatcher = dispatcher;
  }

  setTransactionAgent(agent: TransactionAgent): void {
    this.transactionAgent = agent;
  }

  setOutgoingCallFilters(filters: readonly OutgoingGrainCallFilter[]): void {
    this.outgoingCallFilters = filters;
  }

  getGrain<T>(def: GrainInterface<T>, key: GrainKey): T {
    const target = new GrainId(this.resolveGrainType(def.id), key);
    return this.buildProxy(def, target);
  }

  private buildProxy<T>(def: GrainInterface<T>, target: GrainId): T {
    return new Proxy(
      {},
      {
        // The proxy target is an empty object, so without this trap
        // `GRAIN_REF in ref` would always be false (identity lookups and the
        // wire codec's "is this a grain ref?" check rely on `in`).
        has: (_t, prop): boolean => {
          if (prop === GRAIN_REF) return true;
          // Keep `"then" in ref` false too, for the same reason `get` hides it.
          if (prop === "then") return false;
          return typeof prop === "string";
        },
        get: (_t, prop): unknown => {
          if (prop === GRAIN_REF) return { grainId: target, interfaceId: def.id };
          if (typeof prop !== "string") return undefined;
          // Never appear thenable: a grain ref must not resolve `.then` to a
          // dispatcher, or awaiting/Promise.resolve-ing one would invoke it.
          if (prop === "then") return undefined;
          const options = def.options[prop] ?? {};
          // `async` so a synchronous boundary-validation throw (e.g. a `join`
          // method called with no ambient transaction) surfaces as a rejected
          // promise, like every other grain-call error.
          return async (...args: unknown[]): Promise<unknown> => {
            if (this.dispatcher === undefined) throw new Error("grain factory has no dispatcher");
            const ambient = invocationContext.getStore();
            const { transaction, beginsHere } = this.resolveTransaction(
              prop,
              options,
              ambient?.transaction,
            );
            // The dispatch (with the transaction boundary) is the terminal step;
            // it reads `callArgs`/`callHeaders` so an outgoing filter's rewrites
            // (arguments, injected trace context) take effect on the request.
            const dispatch = (
              callArgs: unknown[],
              callHeaders: Record<string, string>,
            ): Promise<unknown> => {
              const req: InvocationRequest = {
                target,
                interfaceId: def.id,
                interfaceVersion: def.version,
                method: prop,
                args: callArgs,
                options,
                reentrancyId: ambient?.reentrancyId ?? newChainId(),
                ...(ambient?.senderId !== undefined ? { sender: ambient.senderId } : {}),
                ...(transaction !== undefined ? { transaction } : {}),
                ...(Object.keys(callHeaders).length > 0 ? { headers: callHeaders } : {}),
              };
              return beginsHere
                ? this.runRootTransaction(transaction!, req)
                : this.dispatcher!.invoke(req);
            };
            const baseHeaders = ambient?.headers !== undefined ? { ...ambient.headers } : {};
            if (this.outgoingCallFilters.length === 0) return await dispatch(args, baseHeaders);
            // Run the outgoing call-filter pipeline (caller side, Orleans parity).
            const context: GrainCallContext = {
              target,
              source: ambient?.senderId,
              interfaceId: def.id,
              interfaceName: def.name,
              methodName: prop,
              args: [...args],
              result: undefined,
              headers: baseHeaders,
              invoke: () => Promise.resolve(),
            };
            return await runCallFilters(this.outgoingCallFilters, context, () =>
              dispatch(context.args, context.headers),
            );
          };
        },
      },
    ) as T;
  }

  /**
   * Apply the method's `TransactionOption` against the ambient transaction,
   * mirroring Orleans' boundaries. Returns the transaction to propagate and
   * whether this call is the root that begins (and must resolve) it.
   */
  private resolveTransaction(
    method: string,
    options: InvokeMethodOptions,
    ambient: TransactionInfo | undefined,
  ): { transaction: TransactionInfo | undefined; beginsHere: boolean } {
    switch (options.transaction) {
      case "create":
        return { transaction: this.requireAgent().startTransaction(), beginsHere: true };
      case "createOrJoin":
        return ambient !== undefined
          ? { transaction: ambient, beginsHere: false }
          : { transaction: this.requireAgent().startTransaction(), beginsHere: true };
      case "join":
        if (ambient === undefined) {
          throw new Error(`method ${method} requires an ambient transaction (join)`);
        }
        return { transaction: ambient, beginsHere: false };
      case "notAllowed":
        if (ambient !== undefined) {
          throw new Error(`method ${method} must not run in a transaction (notAllowed)`);
        }
        return { transaction: undefined, beginsHere: false };
      case "suppress":
        return { transaction: undefined, beginsHere: false };
      case "supported":
      case undefined:
      default:
        return { transaction: ambient, beginsHere: false };
    }
  }

  /** Dispatch the boundary call, then commit on success or abort on failure. */
  private async runRootTransaction(
    transaction: TransactionInfo,
    req: InvocationRequest,
  ): Promise<unknown> {
    const agent = this.requireAgent();
    try {
      const result = await this.dispatcher!.invoke(req);
      await agent.resolve(transaction);
      return result;
    } catch (error) {
      await agent.abort(transaction);
      throw error;
    }
  }

  private requireAgent(): TransactionAgent {
    if (this.transactionAgent === undefined) {
      throw new Error("grain factory has no transaction agent");
    }
    return this.transactionAgent;
  }
}
