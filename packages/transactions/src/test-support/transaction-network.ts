import { GrainId } from "@thresh/core/grain-id";
import type { GrainType } from "@thresh/core/grain-type";
import type { ParticipantId, TransactionInfo } from "@thresh/core/transaction-info";
import { participantKey } from "@thresh/core/transaction-info";
import type {
  TransactionProtocol,
  TransactionResourceMethod,
} from "@thresh/core/transaction-resource";
import type { TransactionalStateStorage } from "@thresh/core/transactional-storage";
import type { TimeProvider } from "@thresh/core/time-provider";
import type { InvocationRequest } from "@thresh/core/request";
import type { Dispatcher } from "@thresh/runtime/dispatcher";
import { invocationContext } from "@thresh/runtime/invocation-context";
import { TransactionalStateImpl } from "@thresh/transactions/transactional-state-impl";
import type { TransactionsOptions } from "@thresh/transactions/transactions-options";

/** One protocol message as the network carried it. */
export interface SentMessage {
  readonly target: string;
  readonly method: TransactionResourceMethod;
  readonly args: readonly unknown[];
}

/**
 * The network between transactional resources in a unit test: the boundary the
 * host's dispatcher is in production. Each message is delivered on a later
 * tick to the resource registered under the target's participant id. A test
 * can take a resource down (its activation is gone: messages to it fail, as a
 * call to a crashed silo does) and register a replacement over the same
 * storage, as a re-activation.
 */
export class TransactionNetwork implements TransactionProtocol {
  private readonly resources = new Map<string, object>();
  private readonly down = new Set<string>();
  readonly sent: SentMessage[] = [];
  /** Drop (fail) a message when this returns true. */
  dropWhen: (message: SentMessage) => boolean = () => false;

  register(id: ParticipantId, resource: object): void {
    const key = participantKey(id);
    this.resources.set(key, resource);
    this.down.delete(key);
  }

  /** The resource's activation is gone; messages to it fail until one is registered again. */
  takeDown(id: ParticipantId): void {
    this.down.add(participantKey(id));
  }

  async invoke(
    target: ParticipantId,
    method: TransactionResourceMethod,
    args: readonly unknown[],
  ): Promise<unknown> {
    await new Promise<void>((resolve) => setImmediate(resolve));
    const key = participantKey(target);
    const message: SentMessage = { target: key, method, args };
    this.sent.push(message);
    const resource = this.resources.get(key);
    if (resource === undefined || this.down.has(key) || this.dropWhen(message)) {
      throw new Error(`no route to ${key} for ${method}`);
    }
    const fn = (resource as Record<string, unknown>)[method] as (...a: unknown[]) => unknown;
    return fn.apply(resource, args as unknown[]);
  }

  /** The messages of one kind sent to one resource. */
  messages(method: TransactionResourceMethod, target?: ParticipantId): SentMessage[] {
    const key = target === undefined ? undefined : participantKey(target);
    return this.sent.filter((m) => m.method === method && (key === undefined || m.target === key));
  }
}

/**
 * A dispatcher that carries the agent's protocol calls over `network`, for a
 * participant handed to the agent as a remote silo's reply would (id and
 * access only).
 */
export function dispatcherOver(network: TransactionNetwork): Dispatcher {
  return {
    invoke: (req: InvocationRequest) => {
      const [stateName, ...args] = req.args as [string, ...unknown[]];
      return network.invoke(
        { grainId: req.target, stateName },
        req.method as TransactionResourceMethod,
        args,
      );
    },
  };
}

/** Hand a participant to the root as a remote silo's reply would: id and access only. */
export function asRemote(tx: TransactionInfo, id: ParticipantId): void {
  const key = participantKey(id);
  const entry = tx.participants.get(key)!;
  tx.participants.set(key, { id: entry.id, access: entry.access });
}

export interface Balance {
  cents: number;
}

export const accountId = (key: string): GrainId => new GrainId("Account" as GrainType, key);
export const account = (key: string): ParticipantId => ({
  grainId: accountId(key),
  stateName: "balance",
});

/** A `balance` resource for `key` on the network, loaded from `storage`. */
export async function openAccount(
  network: TransactionNetwork,
  storage: TransactionalStateStorage,
  key: string,
  time: TimeProvider,
  options?: Partial<TransactionsOptions>,
  initial = 100,
): Promise<TransactionalStateImpl<Balance>> {
  const resource = new TransactionalStateImpl<Balance>(
    "balance",
    accountId(key),
    () => ({ cents: initial }),
    storage,
    network,
    options,
    time,
  );
  network.register(account(key), resource);
  await resource.load();
  return resource;
}

/** Run `fn` as a turn inside `tx`. */
export function inTransaction<R>(tx: TransactionInfo, fn: () => Promise<R>): Promise<R> {
  return invocationContext.run({ senderId: undefined, ownerId: undefined, transaction: tx }, fn);
}

/** Let every chain of ticks the protocol started run to completion. */
export async function settle(rounds = 20): Promise<void> {
  for (let i = 0; i < rounds; i++) await new Promise<void>((resolve) => setImmediate(resolve));
}
