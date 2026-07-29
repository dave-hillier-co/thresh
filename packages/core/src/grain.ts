import type { GrainContext } from "./grain-context";
import type { GrainId } from "./grain-id";
import type { GrainInterface } from "./grain-interface";
import type { GrainKeyKind } from "./grain-key";
import type { KeyTypeOf } from "./key-kinds";
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

  protected getGrain<T, K extends GrainKeyKind>(def: GrainInterface<T, K>, key: KeyTypeOf<K>): T {
    return this.context.runtime.getGrain(def, key);
  }

  onActivate(_reason: ActivationReason): Promise<void> {
    return Promise.resolve();
  }

  /**
   * `signal`, when given, fires if the deactivation is abandoned before this
   * hook finishes (e.g. a future deactivate-with-timeout caller) — purely
   * advisory, since JS has no thread interruption; nothing in this base
   * implementation observes it, and today no caller supplies one.
   */
  onDeactivate(_reason: DeactivationReason, _signal?: AbortSignal): Promise<void> {
    return Promise.resolve();
  }
}
