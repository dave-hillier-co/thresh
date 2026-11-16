import { describe, expect, it } from "vitest";
import { FakeTimeProvider } from "@thresh/core/test-support/fake-time-provider";
import { TransactionAgent } from "@thresh/runtime/transaction-agent";
import { MemoryTransactionalStorage } from "@thresh/transactions/memory-transactional-storage";
import type { TransactionsOptions } from "@thresh/transactions/transactions-options";
import {
  account,
  accountId,
  inTransaction,
  openAccount,
  settle,
  TransactionNetwork,
  type Balance,
} from "@thresh/transactions/test-support/transaction-network";

/**
 * A transfer `a` → `b` (and `c`, when asked) whose writes are done; the test
 * then plays the agent's messages itself, so it can stop wherever the agent
 * would have died. `a` wrote first, so it is the TM.
 */
async function transfer(options: Partial<TransactionsOptions>, withC = false) {
  const clock = new FakeTimeProvider();
  const network = new TransactionNetwork();
  const storage = new MemoryTransactionalStorage();
  const a = await openAccount(network, storage, "a", clock, options);
  const b = await openAccount(network, storage, "b", clock, options);
  const c = withC ? await openAccount(network, storage, "c", clock, options) : undefined;
  const agent = new TransactionAgent(clock);
  const tx = agent.startTransaction();
  await inTransaction(tx, () => a.performUpdate((s) => (s.cents -= 30)));
  await inTransaction(tx, () => b.performUpdate((s) => (s.cents += 30)));
  if (c !== undefined) await inTransaction(tx, () => c.performUpdate((s) => (s.cents += 1)));
  const writers = withC ? [account("a"), account("b"), account("c")] : [account("a"), account("b")];
  const one = { reads: 0, writes: 1 };
  return {
    clock,
    network,
    storage,
    a,
    b,
    c,
    agent,
    tx,
    /** The agent's one-way prepare to `b`. */
    prepareB: () => b.prepare(tx.id, one, tx.timeStamp, account("a")),
    /** The agent's request to the TM. */
    prepareAndCommit: () => a.prepareAndCommit(tx.id, one, tx.timeStamp, writers, writers.length),
  };
}

async function committed(storage: MemoryTransactionalStorage, key: string) {
  const loaded = await storage.load("balance", accountId(key));
  return {
    cents: loaded.committedSequenceId === 0 ? 100 : (loaded.committedState as Balance).cents,
    pending: loaded.pendingStates.length,
    commitRecords: Object.keys(loaded.metadata.commitRecords).length,
  };
}

async function advance(clock: FakeTimeProvider, ms: number, step = 1_000): Promise<void> {
  for (let elapsed = 0; elapsed < ms; elapsed += step) {
    clock.advance(Math.min(step, ms - elapsed));
    await settle();
  }
}

describe("a prepared transaction whose coordinator dies is still resolved (T4)", () => {
  it(
    "the agent dies before prepareAndCommit reaches the TM: the participant pings, the TM " +
      "presumes abort once its own lock has timed out, and the participant rolls back",
    async () => {
      const options = { remoteTransactionPingFrequencyMs: 1_000, lockTimeoutMs: 5_000 };
      const { clock, network, storage, a, b, agent, prepareB } = await transfer(options);
      await prepareB();
      await settle();
      expect(await committed(storage, "b")).toMatchObject({ cents: 100, pending: 1 });

      // Pinged while the TM still holds the transaction in its lock: no answer,
      // the participant keeps its record.
      await advance(clock, 1_000);
      expect(network.messages("ping", account("a")).length).toBe(1);
      expect(network.messages("cancel", account("b"))).toEqual([]);
      expect(b.blocksDeactivation).toBe(true);

      // The TM's lock times out; the next ping is answered with a presumed abort.
      await advance(clock, 6_000);
      expect(network.messages("cancel", account("b")).map((m) => m.args[2])).toContain(
        "presumedAbort",
      );
      expect(await committed(storage, "b")).toMatchObject({ cents: 100, pending: 0 });
      expect(await committed(storage, "a")).toMatchObject({ cents: 100, pending: 0 });
      expect(a.blocksDeactivation).toBe(false);
      expect(b.blocksDeactivation).toBe(false);

      // No lock stays held.
      const later = agent.startTransaction();
      await inTransaction(later, () => a.performUpdate((s) => (s.cents += 1)));
      await inTransaction(later, () => b.performUpdate((s) => (s.cents += 1)));
      await agent.resolve(later);
      await settle();
      expect(await committed(storage, "a")).toMatchObject({ cents: 101 });
      expect(await committed(storage, "b")).toMatchObject({ cents: 101 });
    },
  );

  it("the agent dies after the TM accepted prepareAndCommit: the TM still commits and confirms", async () => {
    const { storage, a, b, prepareB, prepareAndCommit } = await transfer({});
    void prepareB();
    // The agent never hears the answer.
    void prepareAndCommit();
    await settle();

    expect(await committed(storage, "a")).toEqual({ cents: 70, pending: 0, commitRecords: 0 });
    expect(await committed(storage, "b")).toEqual({ cents: 130, pending: 0, commitRecords: 0 });
    expect(a.blocksDeactivation).toBe(false);
    expect(b.blocksDeactivation).toBe(false);
  });

  it("the TM aborts after PrepareTimeout when a vote never arrives, and cancels the participants", async () => {
    const options = { prepareTimeoutMs: 20_000 };
    const { clock, storage, a, b, prepareB, prepareAndCommit } = await transfer(options, true);
    // The agent dies before it sent `c` its prepare.
    void prepareB();
    const answer = prepareAndCommit();
    await settle();
    expect(await committed(storage, "b")).toMatchObject({ pending: 1 });

    await advance(clock, 20_000, 5_000);

    await expect(answer).resolves.toBe("prepareTimeout");
    expect(await committed(storage, "a")).toEqual({ cents: 100, pending: 0, commitRecords: 0 });
    expect(await committed(storage, "b")).toEqual({ cents: 100, pending: 0, commitRecords: 0 });
    expect(a.blocksDeactivation).toBe(false);
    expect(b.blocksDeactivation).toBe(false);
  });

  it("a participant keeps its record while the TM has not decided, and pings again", async () => {
    const options = { prepareTimeoutMs: 20_000, remoteTransactionPingFrequencyMs: 5_000 };
    const { clock, network, storage, b, prepareB, prepareAndCommit } = await transfer(
      options,
      true,
    );
    void prepareB();
    void prepareAndCommit();
    await settle();

    // In the TM's commit queue, so undetermined: the ping goes unanswered.
    await advance(clock, 5_000, 5_000);
    expect(network.messages("ping", account("a")).length).toBe(1);
    expect(b.blocksDeactivation).toBe(true);
    expect(await committed(storage, "b")).toMatchObject({ cents: 100, pending: 1 });

    // The TM decides abort at 20 s and cancels it.
    await advance(clock, 15_000, 5_000);
    expect(network.messages("ping", account("a")).length).toBeGreaterThanOrEqual(3);
    expect(b.blocksDeactivation).toBe(false);
    expect(await committed(storage, "b")).toMatchObject({ cents: 100, pending: 0 });
  });
});
