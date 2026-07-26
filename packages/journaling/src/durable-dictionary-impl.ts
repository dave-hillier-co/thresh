import type { DurableStateMachine, StateMachineManager } from "@thresh/core/durable-state-machine";
import type { DurableDictionary } from "@thresh/core/durable-state";

type DictOp<K, V> =
  | { t: "set"; k: K; v: V }
  | { t: "del"; k: K }
  | { t: "clear" }
  | { t: "load"; entries: [K, V][] };

/**
 * A journalled scalar-keyed map, implemented as a `DurableStateMachine`. Keys
 * must be JSON-stable scalars (string/number) so they round-trip through the
 * value-codec on replay. Each mutation is appended to the grain's log and then
 * applied to memory.
 */
export class DurableDictionaryImpl<K, V> implements DurableDictionary<K, V>, DurableStateMachine {
  private readonly map = new Map<K, V>();

  constructor(
    readonly name: string,
    private readonly manager: StateMachineManager,
  ) {}

  get size(): number {
    return this.map.size;
  }

  has(key: K): boolean {
    return this.map.has(key);
  }

  get(key: K): V | undefined {
    return this.map.get(key);
  }

  entries(): IterableIterator<[K, V]> {
    return this.map.entries();
  }

  keys(): IterableIterator<K> {
    return this.map.keys();
  }

  values(): IterableIterator<V> {
    return this.map.values();
  }

  async set(key: K, value: V): Promise<void> {
    // The manager applies the op to memory after a successful append.
    await this.manager.append(this.name, { t: "set", k: key, v: value } satisfies DictOp<K, V>);
  }

  async delete(key: K): Promise<boolean> {
    if (!this.map.has(key)) return false;
    await this.manager.append(this.name, { t: "del", k: key } satisfies DictOp<K, V>);
    return true;
  }

  async clear(): Promise<void> {
    await this.manager.append(this.name, { t: "clear" } satisfies DictOp<K, V>);
  }

  reset(): void {
    this.map.clear();
  }

  apply(payload: unknown): void {
    const op = payload as DictOp<K, V>;
    switch (op.t) {
      case "set":
        this.map.set(op.k, op.v);
        break;
      case "del":
        this.map.delete(op.k);
        break;
      case "clear":
        this.map.clear();
        break;
      case "load":
        this.map.clear();
        for (const [k, v] of op.entries) this.map.set(k, v);
        break;
    }
  }

  snapshot(): unknown {
    return { t: "load", entries: [...this.map.entries()] } satisfies DictOp<K, V>;
  }
}
