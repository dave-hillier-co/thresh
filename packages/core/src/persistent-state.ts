/**
 * The grain-facing persistence facet, mirroring Orleans `IPersistentState<T>`.
 * `value` is the in-memory state, served without touching the store; `write`
 * persists it (bumping the etag), `read` reloads, `clear` deletes the record.
 */
export interface PersistentState<TState> {
  value: TState;
  readonly etag: string | undefined;
  readonly exists: boolean;

  read(): Promise<void>;
  write(): Promise<void>;
  clear(): Promise<void>;
}
