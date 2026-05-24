import { GrainId } from "@tsva/core/grain-id";
import type { GrainInterface } from "@tsva/core/grain-interface";
import type { GrainKey } from "@tsva/core/grain-key";
import { GRAIN_REF } from "@tsva/core/grain-reference";
import type { GrainType } from "@tsva/core/grain-type";
import { Guid } from "@tsva/core/guid";
import type { InvocationRequest } from "@tsva/core/request";
import type { Dispatcher } from "@tsva/runtime/dispatcher";
import { invocationContext } from "@tsva/runtime/invocation-context";

const newChainId = () => Guid.newGuid().toString();

/**
 * Builds grain references as ES `Proxy` objects. Each intercepted method call
 * becomes an `InvocationRequest` dispatched through the runtime; caller identity
 * and the call-chain reentrancy id are read from the ambient invocation context.
 */
export class GrainFactory {
  private dispatcher: Dispatcher | undefined;

  constructor(private readonly resolveGrainType: (interfaceId: number) => GrainType) {}

  setDispatcher(dispatcher: Dispatcher): void {
    this.dispatcher = dispatcher;
  }

  getGrain<T>(def: GrainInterface<T>, key: GrainKey): T {
    const target = new GrainId(this.resolveGrainType(def.id), key);
    return this.buildProxy(def, target);
  }

  private buildProxy<T>(def: GrainInterface<T>, target: GrainId): T {
    return new Proxy(
      {},
      {
        get: (_t, prop): unknown => {
          if (prop === GRAIN_REF) return { grainId: target, interfaceId: def.id };
          if (typeof prop !== "string") return undefined;
          const methodId = def.methodId.get(prop);
          if (methodId === undefined) return undefined;
          const options = def.options[prop] ?? {};
          return (...args: unknown[]): Promise<unknown> => {
            if (this.dispatcher === undefined) throw new Error("grain factory has no dispatcher");
            const ambient = invocationContext.getStore();
            const req: InvocationRequest = {
              target,
              interfaceId: def.id,
              methodId,
              args,
              options,
              reentrancyId: ambient?.reentrancyId ?? newChainId(),
              ...(ambient?.senderId !== undefined ? { sender: ambient.senderId } : {}),
            };
            return this.dispatcher.invoke(req);
          };
        },
      },
    ) as T;
  }
}
