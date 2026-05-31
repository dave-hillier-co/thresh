/** A write-only bag a participant populates during dehydration (Orleans `IDehydrationContext`). */
export interface DehydrationContext {
  set(key: string, value: unknown): void;
}

/** A read-only bag a participant consults during rehydration (Orleans `IRehydrationContext`). */
export interface RehydrationContext {
  has(key: string): boolean;
  get<T>(key: string): T | undefined;
  readonly keys: readonly string[];
}

/**
 * A grain — or a state facet it owns — that carries its in-memory state across a
 * live migration: `onDehydrate` writes it into the bag on the source silo and
 * `onRehydrate` restores it on the target, so the activation moves without losing
 * (possibly unflushed) state. Mirrors Orleans `IGrainMigrationParticipant`.
 */
export interface IGrainMigrationParticipant {
  onDehydrate(context: DehydrationContext): void;
  onRehydrate(context: RehydrationContext): void;
}

/** Duck-type guard: an object is a participant if it exposes both migration hooks. */
export function isMigrationParticipant(value: unknown): value is IGrainMigrationParticipant {
  const candidate = value as Partial<IGrainMigrationParticipant> | null | undefined;
  return (
    typeof candidate?.onDehydrate === "function" && typeof candidate?.onRehydrate === "function"
  );
}

/**
 * A string-keyed bag backing both migration contexts; its `data` is the
 * serializable payload carried to the target silo.
 */
export class MigrationBag implements DehydrationContext, RehydrationContext {
  constructor(private readonly bag: Record<string, unknown> = {}) {}

  set(key: string, value: unknown): void {
    this.bag[key] = value;
  }

  has(key: string): boolean {
    return Object.prototype.hasOwnProperty.call(this.bag, key);
  }

  get<T>(key: string): T | undefined {
    return this.bag[key] as T | undefined;
  }

  get keys(): readonly string[] {
    return Object.keys(this.bag);
  }

  get data(): Record<string, unknown> {
    return this.bag;
  }
}
