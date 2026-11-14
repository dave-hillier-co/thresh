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
 * value-codec on replay. Each mutation is applied to memory at once and
 * buffered; `writeState` appends the buffer.
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

  set(key: K, value: V): void {
    this.mutate({ t: "set", k: key, v: value });
  }

  delete(key: K): boolean {
    if (!this.map.has(key)) return false;
    this.mutate({ t: "del", k: key });
    return true;
  }

  clear(): void {
    this.mutate({ t: "clear" });
  }

  /** Buffers the op's log entry for the next `writeState` and applies it to memory now. */
  private mutate(op: DictOp<K, V>): void {
    // Serialised first: a payload the codec rejects leaves memory untouched, as in Orleans.
    this.manager.buffer(this.name, op);
    this.apply(op);
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
