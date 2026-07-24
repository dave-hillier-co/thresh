/**
 * The grain-facing persistence facet, mirroring Orleans `IPersistentState<T>`.
 * `value` is the in-memory state, served without touching the store; `write`
 * persists it (bumping the etag), `read` reloads, `clear` deletes the record.
 */
export interface PersistentState<TState> {
  value: TState;
  readonly etag: string | undefined;
  readonly exists: boolean;

  /** `signal`, when given, is threaded to the backing `GrainStorage` call (see its doc). */
  read(signal?: AbortSignal): Promise<void>;
  write(signal?: AbortSignal): Promise<void>;
  clear(signal?: AbortSignal): Promise<void>;
}
