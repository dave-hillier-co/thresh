import { TransactionAbortedError } from "@tsva/core/errors";
import { Guid } from "@tsva/core/guid";
import type { EnlistedParticipant, TransactionInfo } from "@tsva/core/transaction-info";
import { CausalClock } from "@tsva/runtime/causal-clock";
import type { TimeProvider } from "@tsva/runtime/time-provider";

/**
 * The per-silo transaction agent (Orleans `TransactionAgent`). It begins a
 * transaction on behalf of the originating call — assigning a unique id and a
 * causal-clock timestamp — and resolves it at the boundary with an optimistic,
 * serializable commit ([ADR 0008](../../docs/adr/0008-cross-grain-transactions.md)).
 *
 * Resolve runs the faithful two-phase protocol: it collates the enlisted
 * participants, elects the transaction manager from the writers, **prepares**
 * every participant (each validates its lock and stages its tentative state),
 * and then **commits** them all — or, if any prepare vetoes, **cancels** them all
 * (cascading abort) and raises {@link TransactionAbortedError} so the caller may
 * retry. For now the agent drives the rounds in-process; routing the TM/resource
 * calls over the dispatcher for remote participants is later-slice work.
 */
export class TransactionAgent {
  private readonly clock: CausalClock;

  constructor(time: TimeProvider) {
    this.clock = new CausalClock(time);
  }

  startTransaction(readOnly = false): TransactionInfo {
    return {
      id: Guid.newGuid().toString(),
      timeStamp: this.clock.utcNow(),
      readOnly,
      participants: new Map(),
    };
  }

  /** Commit the transaction across its participants, or abort all and throw. */
  async resolve(info: TransactionInfo): Promise<void> {
    const enlisted = [...info.participants.values()];
    if (enlisted.length === 0) return;

    // Elect the transaction manager from the writers (Orleans: the first
    // write participant, or a priority manager). Recorded for prepared records
    // and recovery; in-process the agent coordinates the rounds directly.
    this.electManager(enlisted);

    const prepared = await Promise.all(
      enlisted.map(async (e) => {
        try {
          return await e.participant.prepare(info.id, info.timeStamp);
        } catch {
          return false;
        }
      }),
    );

    if (prepared.every((ok) => ok)) {
      await Promise.all(enlisted.map((e) => e.participant.commit(info.id)));
      return;
    }
    await this.abort(info);
    throw new TransactionAbortedError(info.id, "a participant failed to prepare");
  }

  /** Abort: discard every enlisted participant's tentative writes and release locks. */
  async abort(info: TransactionInfo): Promise<void> {
    await Promise.all([...info.participants.values()].map((e) => e.participant.abort(info.id)));
  }

  /** The elected manager (first writer), or undefined for a read-only transaction. */
  private electManager(enlisted: readonly EnlistedParticipant[]): EnlistedParticipant | undefined {
    return enlisted.find((e) => e.access.writes > 0);
  }
}
