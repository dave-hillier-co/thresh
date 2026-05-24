import type { GrainId } from "@tsva/core/grain-id";
import type { GrainStorage, StateHolder } from "@tsva/core/grain-storage";
import type { Reducer, ReducerState } from "@tsva/core/reducer-state";

/**
 * Snapshot-mode reducer state: keeps the folded state in memory and persists it
 * (the snapshot) through a `GrainStorage` with etag optimistic concurrency. The
 * raised events are transient — only the reduced `value` is durable. An event log
 * (true event sourcing) is a separate provider, future work per ADR 0006.
 */
export class ReducerStateImpl<TState, TEvent> implements ReducerState<TState, TEvent> {
  private readonly holder: StateHolder<TState>;

  constructor(
    private readonly stateName: string,
    private readonly grainId: GrainId,
    private readonly storage: GrainStorage,
    initial: () => TState,
    private readonly reduce: Reducer<TState, TEvent>,
  ) {
    this.holder = { value: initial(), exists: false };
  }

  get value(): TState {
    return this.holder.value;
  }

  get etag(): string | undefined {
    return this.holder.etag;
  }

  get exists(): boolean {
    return this.holder.exists;
  }

  raise(event: TEvent): TState {
    this.holder.value = this.reduce(this.holder.value, event);
    return this.holder.value;
  }

  async read(): Promise<void> {
    await this.storage.read(this.stateName, this.grainId, this.holder);
  }

  async write(): Promise<void> {
    await this.storage.write(this.stateName, this.grainId, this.holder);
  }
}
