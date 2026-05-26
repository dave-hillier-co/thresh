import { defineGrainInterface } from "./grain-interface";

/**
 * The system extension the transaction agent calls to drive a transactional
 * resource that lives on another silo (Orleans `ITransactionalResourceExtension`).
 * The first argument names the state on the target grain; the activation routes
 * the call to that resource. Mirrors the `StreamConsumer` system-extension
 * precedent. Local participants are driven directly (no dispatch); this is the
 * path for participants merged back from a remote silo.
 */
export interface TransactionResource {
  prepare(stateName: string, transactionId: string, timeStamp: number): Promise<boolean>;
  commit(stateName: string, transactionId: string): Promise<void>;
  abort(stateName: string, transactionId: string): Promise<void>;
}

export const TransactionResourceInterface = defineGrainInterface<TransactionResource>(
  "system.TransactionResource",
);
