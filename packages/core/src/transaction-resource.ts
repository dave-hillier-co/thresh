import { defineGrainInterface } from "./grain-interface";
import type { AccessCounter, ParticipantId, TransactionalStatus } from "./transaction-info";

/**
 * The system extension through which the commit protocol reaches a
 * transactional resource on any silo (Orleans `ITransactionalResourceExtension`
 * and `ITransactionManagerExtension`, merged into one interface). The first
 * argument names the state on the target grain; the activation routes the call
 * to that resource. Mirrors the `StreamConsumer` system-extension precedent.
 *
 * The participant messages (`commitReadOnly`, `prepare`, `abort`, `cancel`,
 * `confirm`) are `TransactionParticipant`'s; the manager messages
 * (`prepareAndCommit`, `prepared`, `ping`) are `TransactionManager`'s
 * (`@thresh/core/transaction-info` documents each).
 */
export interface TransactionResource {
  commitReadOnly(
    stateName: string,
    transactionId: string,
    accessCount: AccessCounter,
    timeStamp: number,
  ): Promise<TransactionalStatus>;
  prepare(
    stateName: string,
    transactionId: string,
    accessCount: AccessCounter,
    timeStamp: number,
    transactionManager: ParticipantId,
  ): Promise<void>;
  abort(stateName: string, transactionId: string): Promise<void>;
  cancel(
    stateName: string,
    transactionId: string,
    timeStamp: number,
    status: TransactionalStatus,
  ): Promise<void>;
  confirm(stateName: string, transactionId: string, timeStamp: number): Promise<void>;
  prepareAndCommit(
    stateName: string,
    transactionId: string,
    accessCount: AccessCounter,
    timeStamp: number,
    writeParticipants: ParticipantId[],
    totalParticipants: number,
  ): Promise<TransactionalStatus>;
  prepared(
    stateName: string,
    transactionId: string,
    timeStamp: number,
    participant: ParticipantId,
    status: TransactionalStatus,
  ): Promise<void>;
  ping(
    stateName: string,
    transactionId: string,
    timeStamp: number,
    participant: ParticipantId,
  ): Promise<void>;
}

/** A protocol message name. */
export type TransactionResourceMethod = keyof TransactionResource;

/**
 * Every protocol call is always-interleave, as Orleans marks
 * `ITransactionalResourceExtension` and `ITransactionManagerExtension`: they
 * must run while the grain's own turn is suspended — above all while that turn
 * waits for the very lock an abort or commit here releases. `prepare`,
 * `prepared` and `ping` are one-way, as upstream marks them.
 */
export const TransactionResourceInterface = defineGrainInterface<TransactionResource>(
  "system.TransactionResource",
  {
    options: {
      commitReadOnly: { alwaysInterleave: true },
      prepare: { alwaysInterleave: true, oneWay: true },
      abort: { alwaysInterleave: true },
      cancel: { alwaysInterleave: true },
      confirm: { alwaysInterleave: true },
      prepareAndCommit: { alwaysInterleave: true },
      prepared: { alwaysInterleave: true, oneWay: true },
      ping: { alwaysInterleave: true, oneWay: true },
    },
  },
);

/**
 * How a transactional resource sends a protocol message to another one, named
 * by its {@link ParticipantId} (in Orleans, a grain reference cast to the
 * resource or manager extension). `args` omit the state name; the transport
 * supplies it from `target`. The host wires this to its dispatcher; tests wire
 * it to an in-memory network.
 */
export interface TransactionProtocol {
  invoke(
    target: ParticipantId,
    method: TransactionResourceMethod,
    args: readonly unknown[],
  ): Promise<unknown>;
}
