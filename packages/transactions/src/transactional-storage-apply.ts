import type {
  PendingTransactionState,
  TransactionalStateMetadata,
} from "@tsva/core/transactional-storage";

/** The durable record a transactional-storage provider keeps per (grain, state). */
export interface StoredRecord<T> {
  committedState: T;
  committedSequenceId: number;
  metadata: TransactionalStateMetadata;
  pendingStates: PendingTransactionState<T>[];
}

const clone = <T>(value: T): T =>
  typeof structuredClone === "function"
    ? structuredClone(value)
    : (JSON.parse(JSON.stringify(value)) as T);

export function emptyRecord<T>(): StoredRecord<T> {
  return {
    committedState: undefined as T,
    committedSequenceId: 0,
    metadata: { timeStamp: 0, commitRecords: {} },
    pendingStates: [],
  };
}

/**
 * Apply the prepare/commit/abort deltas of one `store` call to a record (in
 * place), returning it. Shared by the in-memory and Redis providers so their
 * semantics are identical: prepare replaces-or-appends a pending state by
 * sequence id; `commitUpTo` promotes pending up to that id into the committed
 * version; `abortAfter` drops pending strictly past that id.
 */
export function applyStore<T>(
  record: StoredRecord<T>,
  metadata: TransactionalStateMetadata,
  statesToPrepare: ReadonlyArray<PendingTransactionState<T>>,
  commitUpTo: number | undefined,
  abortAfter: number | undefined,
): StoredRecord<T> {
  for (const pending of statesToPrepare) {
    const at = record.pendingStates.findIndex((p) => p.sequenceId === pending.sequenceId);
    if (at >= 0) record.pendingStates[at] = clone(pending);
    else record.pendingStates.push(clone(pending));
  }
  record.pendingStates.sort((a, b) => a.sequenceId - b.sequenceId);

  if (commitUpTo !== undefined) {
    for (const pending of record.pendingStates) {
      if (pending.sequenceId <= commitUpTo) {
        record.committedState = pending.state;
        record.committedSequenceId = pending.sequenceId;
      }
    }
    record.pendingStates = record.pendingStates.filter((p) => p.sequenceId > commitUpTo);
  }

  if (abortAfter !== undefined) {
    record.pendingStates = record.pendingStates.filter((p) => p.sequenceId <= abortAfter);
  }

  record.metadata = clone(metadata);
  return record;
}
