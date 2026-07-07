import { durationToMs } from "@tsva/core/duration";
import {
  runCallFilters,
  type GrainCallContext,
  type OutgoingGrainCallFilter,
} from "@tsva/core/grain-call-filter";
import {
  cancellationTokenSourceOf,
  GrainCancellationToken,
} from "@tsva/core/grain-cancellation-token";
import { GrainId } from "@tsva/core/grain-id";
import { getGrainInterface, type GrainInterface } from "@tsva/core/grain-interface";
import type { GrainKey } from "@tsva/core/grain-key";
import { GRAIN_REF, GRAIN_REF_CAST } from "@tsva/core/grain-reference";
import type { GrainType } from "@tsva/core/grain-type";
import { Guid } from "@tsva/core/guid";
import type { InvokeMethodOptions } from "@tsva/core/invoke-options";
import type { InvocationRequest } from "@tsva/core/request";
import type { TransactionInfo } from "@tsva/core/transaction-info";
import { GrainCallTimeoutError, TransactionsDisabledError } from "@tsva/core/errors";
import type { Dispatcher } from "@tsva/runtime/dispatcher";
import { invocationContext } from "@tsva/runtime/invocation-context";
import { systemTimeProvider, type TimeProvider } from "@tsva/runtime/time-provider";
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

  constructor(
    private readonly resolveGrainType: (interfaceId: number) => GrainType,
    private readonly time: TimeProvider = systemTimeProvider,
    /**
     * Silo-wide default response timeout (ms), applied when a method has no
     * `options.responseTimeout` of its own. Off by default (`undefined`): no
     * silo behaves differently unless this is explicitly configured.
     */
    private readonly defaultResponseTimeoutMs?: number,
  ) {}

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

  /**
   * Build a reference to an explicit `GrainId`, bypassing type resolution
   * from the interface. Used to rehydrate a wire identity as-is (a normal
   * grain reference's `grainId.type` already matches what `getGrain` would
   * resolve; an observer reference's reserved `$client` type and `+scope`
   * key must survive unchanged, which `getGrain` cannot do since it derives
   * the type from the interface).
   */
  getReference<T>(def: GrainInterface<T>, grainId: GrainId): T {
    return this.buildProxy(def, grainId);
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
        get: (_t, prop, receiver): unknown => {
          if (prop === GRAIN_REF) return { grainId: target, interfaceId: def.id };
          if (prop === GRAIN_REF_CAST) return this.castTo(def, target, receiver);
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
            // A caller-side `GrainCancellationToken` argument means this call is
            // sending that token to `target` — record it on the token's source so
            // a later `source.cancel()` knows to notify this activation too (a
            // callee-side token, bound from the wire, has no source and is a
            // cheap no-op here).
            for (const arg of args) {
              if (arg instanceof GrainCancellationToken) {
                cancellationTokenSourceOf(arg)?.recordTarget(target);
              }
            }
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
            // The terminal call, filters included — a plain function so the
            // deadline race below can wrap it without duplicating either branch.
            const invokeCall = (): Promise<unknown> => {
              if (this.outgoingCallFilters.length === 0) return dispatch(args, baseHeaders);
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
              return runCallFilters(this.outgoingCallFilters, context, () =>
                dispatch(context.args, context.headers),
              );
            };
            const timeoutMs = this.resolveResponseTimeout(options);
            if (timeoutMs === undefined) return await invokeCall();
            return await this.raceResponseDeadline(invokeCall(), timeoutMs, prop);
          };
        },
      },
    ) as T;
  }

  /**
   * Build the `GRAIN_REF_CAST` handler for a reference currently typed as
   * `sourceDef` over `target`: re-type it to `newDef`, same grain id (Orleans'
   * `AsReference<T>`/`Cast`).
   *
   * A same-interface cast is a no-op that returns the identical proxy
   * (`Assert.Same` in Orleans' `CastInternalCastFromMyType`/
   * `CastInternalCastUpFromChild`-style tests). Casting to anything that
   * isn't even a registered `GrainInterface` fails eagerly here (Orleans'
   * `Cast(grain, typeof(bool))` throws `ArgumentException` synchronously);
   * casting to an interface the concrete grain type doesn't actually
   * implement still succeeds — that mismatch only surfaces once a call for a
   * method the grain lacks reaches the activation (Orleans parity: casts are
   * validated optimistically, calls are validated for real).
   */
  private castTo(
    sourceDef: GrainInterface<unknown>,
    target: GrainId,
    sourceRef: unknown,
  ): (newDef: GrainInterface<unknown>) => unknown {
    return (newDef) => {
      if (
        typeof newDef !== "object" ||
        newDef === null ||
        typeof newDef.id !== "number" ||
        getGrainInterface(newDef.id) === undefined
      ) {
        throw new TypeError("cast target is not a registered grain interface");
      }
      if (newDef.id === sourceDef.id) return sourceRef;
      return this.buildProxy(newDef, target);
    };
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

  /**
   * The effective response timeout (ms) for this call: the per-method
   * `options.responseTimeout` if set, else the silo-wide default, else no
   * deadline at all. A `oneWay` call never races a deadline — the caller
   * doesn't wait for a response in the first place.
   */
  private resolveResponseTimeout(options: InvokeMethodOptions): number | undefined {
    if (options.oneWay) return undefined;
    if (options.responseTimeout !== undefined) return durationToMs(options.responseTimeout);
    return this.defaultResponseTimeoutMs;
  }

  /**
   * Race `call` against a `timeoutMs` timer (Orleans `ResponseTimeout`): if
   * the timer wins, reject with {@link GrainCallTimeoutError}. The callee is
   * never interrupted — JS has no cooperative cancellation for an in-flight
   * turn — so `call` may still be running when the deadline fires; its
   * eventual settlement is swallowed (not surfaced as an unhandled
   * rejection) once the deadline has already decided the outcome. When
   * `call` settles first, the timer is cleared so it never fires.
   */
  private raceResponseDeadline(
    call: Promise<unknown>,
    timeoutMs: number,
    method: string,
  ): Promise<unknown> {
    return new Promise((resolve, reject) => {
      let settled = false;
      const timer = this.time.setTimer(() => {
        if (settled) return;
        settled = true;
        reject(
          new GrainCallTimeoutError(
            `grain call ${method} exceeded its ${timeoutMs}ms response deadline`,
          ),
        );
        // The call is still running; consume its eventual settlement so a
        // later rejection doesn't surface as an unhandled promise rejection.
        call.catch(() => {});
      }, timeoutMs);
      call.then(
        (value) => {
          if (settled) return;
          settled = true;
          this.time.clearTimer(timer);
          resolve(value);
        },
        (error: unknown) => {
          if (settled) return;
          settled = true;
          this.time.clearTimer(timer);
          reject(error);
        },
      );
    });
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

  /**
   * Resolve the transaction agent, or raise the Orleans-parity
   * "transactions disabled" error when this silo was built without one — a
   * silo built via `SiloBuilder` only wires an agent when transactional
   * storage is configured (transactions are opt-in).
   */
  private requireAgent(): TransactionAgent {
    if (this.transactionAgent === undefined) {
      throw new TransactionsDisabledError();
    }
    return this.transactionAgent;
  }
}
