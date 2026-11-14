import type { TimeProvider, TimerHandle } from "@thresh/core/time-provider";
import { sameParticipant, type ParticipantId } from "@thresh/core/transaction-info";
import type { TransactionProtocol } from "@thresh/core/transaction-resource";
import { deferred } from "@thresh/transactions/transaction-record";
import type { StorageBatch } from "@thresh/transactions/storage-batch";

/** One participant still to confirm; once it has acknowledged, it is not asked again. */
interface Confirmation {
  readonly participant: ParticipantId;
  pending: Promise<unknown> | undefined;
  complete: boolean;
}

/**
 * The TM's confirmation worker (Orleans `ConfirmationWorker<TState>`). For a
 * commit it has made durable it sends `confirm` to every other write
 * participant, resending every `confirmationRetryDelayMs` until each has
 * acknowledged, and then removes the commit record from storage, retrying on
 * the same delay until that write lands.
 */
export class ConfirmationWorker<T> {
  private readonly pending = new Set<string>();
  private readonly delays = new Map<TimerHandle, () => void>();
  private stopped = false;

  constructor(
    private readonly me: ParticipantId,
    private readonly retryDelayMs: number,
    private readonly protocol: TransactionProtocol | undefined,
    private readonly getStorageBatch: () => StorageBatch<T>,
    private readonly notifyStorageWorker: () => void,
    private readonly time: TimeProvider,
  ) {}

  /** Confirm and then collect `transactionId`, unless that is already under way. */
  add(transactionId: string, timestamp: number, participants: readonly ParticipantId[]): void {
    if (this.stopped || this.isConfirmed(transactionId)) return;
    this.pending.add(transactionId);
    void this.sendConfirmation(transactionId, timestamp, participants);
  }

  /**
   * Whether `transactionId` is still being confirmed or collected. Named after
   * Orleans' `IsConfirmed`, which answers exactly this: a ping for such a
   * transaction needs no answer, because the confirmation will reach the
   * participant.
   */
  isConfirmed(transactionId: string): boolean {
    return this.pending.has(transactionId);
  }

  get size(): number {
    return this.pending.size;
  }

  /** The activation is going away: no further attempts, and no timer left behind. */
  stop(): void {
    this.stopped = true;
    for (const [handle, wake] of this.delays) {
      this.time.clearTimer(handle);
      wake();
    }
    this.delays.clear();
  }

  private async sendConfirmation(
    transactionId: string,
    timestamp: number,
    participants: readonly ParticipantId[],
  ): Promise<void> {
    await this.notifyAll(transactionId, timestamp, participants);
    await this.collect(transactionId);
  }

  private async notifyAll(
    transactionId: string,
    timestamp: number,
    participants: readonly ParticipantId[],
  ): Promise<void> {
    const confirmations: Confirmation[] = participants
      .filter((p) => !sameParticipant(p, this.me))
      .map((participant) => ({ participant, pending: undefined, complete: false }));
    if (confirmations.length === 0) return;
    let outstanding = true;
    while (!this.stopped && outstanding) {
      const results = await Promise.all(
        confirmations.map((c) => this.confirmed(c, transactionId, timestamp)),
      );
      outstanding = results.some((ok) => !ok);
      if (outstanding) await this.delay();
    }
  }

  private async confirmed(
    confirmation: Confirmation,
    transactionId: string,
    timestamp: number,
  ): Promise<boolean> {
    if (confirmation.complete) return true;
    confirmation.pending ??= this.send(confirmation.participant, transactionId, timestamp);
    try {
      await confirmation.pending;
      confirmation.complete = true;
    } catch {
      confirmation.pending = undefined;
    }
    return confirmation.complete;
  }

  private send(participant: ParticipantId, transactionId: string, timestamp: number) {
    if (this.protocol === undefined) {
      return Promise.reject(new Error("no transaction protocol to reach the participant"));
    }
    return this.protocol.invoke(participant, "confirm", [transactionId, timestamp]);
  }

  private async collect(transactionId: string): Promise<void> {
    while (!this.stopped) {
      if (await this.tryCollect(transactionId)) return;
      await this.delay();
    }
  }

  /** Remove the commit record in the next storage batch, and wait for that batch. */
  private async tryCollect(transactionId: string): Promise<boolean> {
    const stored = deferred<boolean>();
    const batch = this.getStorageBatch();
    batch.collect(transactionId);
    batch.followUpAction(() => {
      this.pending.delete(transactionId);
      stored.resolve(true);
    });
    batch.onFailure(() => stored.resolve(false));
    this.notifyStorageWorker();
    return stored.promise;
  }

  private delay(): Promise<void> {
    if (this.stopped) return Promise.resolve();
    return new Promise<void>((resolve) => {
      const handle = this.time.setTimer(() => {
        this.delays.delete(handle);
        resolve();
      }, this.retryDelayMs);
      this.delays.set(handle, resolve);
    });
  }
}
