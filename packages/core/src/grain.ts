import type { GrainContext } from "./grain-context";
import type { GrainId } from "./grain-id";
import type { GrainInterface } from "./grain-interface";
import type { GrainKeyFor } from "./key-kinds";
import type { ActivationReason, DeactivationReason } from "./reasons";
import type { GrainRuntime } from "./grain-runtime";

/**
 * Base class for grain implementations. The runtime binds the context
 * immediately after construction, before `onActivate` runs.
 */
export abstract class Grain {
  private _context: GrainContext | undefined;

  /** @internal Wired by the runtime; not part of the public surface. */
  setContext(context: GrainContext): void {
    this._context = context;
  }

  protected get context(): GrainContext {
    if (this._context === undefined) throw new Error("grain context not yet bound");
    return this._context;
  }

  protected get id(): GrainId {
    return this.context.id;
  }

  protected get runtime(): GrainRuntime {
    return this.context.runtime;
  }

  protected getGrain<T>(def: GrainInterface<T>, key: GrainKeyFor<T>): T {
    return this.context.runtime.getGrain(def, key);
  }

  onActivate(_reason: ActivationReason): Promise<void> {
    return Promise.resolve();
  }

  onDeactivate(_reason: DeactivationReason): Promise<void> {
    return Promise.resolve();
  }
}
