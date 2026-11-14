import { describe, expect, it } from "vitest";
import { TransactionCascadingAbortError } from "@thresh/core/errors";
import { FakeTimeProvider } from "@thresh/core/test-support/fake-time-provider";
import { TransactionAgent } from "@thresh/runtime/transaction-agent";
import { MemoryTransactionalStorage } from "@thresh/transactions/memory-transactional-storage";
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
 * Pending states form a chain (T5, the root of GitHub issue 78): each
 * transaction's tentative state is built on the newest pending one, records
 * commit strictly in order, and an abort cascades to every record built on the
 * aborted one. That is the invariant under which the storage contract's
 * `commitUpTo` and `abortAfter` are correct (Orleans `TransactionQueue`).
 */

async function durable(storage: MemoryTransactionalStorage, key: string) {
  const loaded = await storage.load("balance", accountId(key));
  return {
    cents: loaded.committedSequenceId === 0 ? 100 : (loaded.committedState as Balance).cents,
    committedSequenceId: loaded.committedSequenceId,
    pending: loaded.pendingStates.map((p) => [
      p.sequenceId,
      p.transactionId,
      (p.state as Balance).cents,
    ]),
  };
}

/**
 * `b` holds T1's prepared record in storage (250 at sequence 1) whose TM is
 * unreachable, as a crash leaves it; the test plays that TM's eventual message.
 * `a` is a live account that will be the TM of the transaction that follows.
 */
async function inDoubtAtB() {
  const clock = new FakeTimeProvider();
  const network = new TransactionNetwork();
  const storage = new MemoryTransactionalStorage();
  await storage.store(
    "balance",
    accountId("b"),
    undefined,
    { timeStamp: 1, commitRecords: {} },
    [
      {
        sequenceId: 1,
        transactionId: "T1",
        timeStamp: 1,
        transactionManager: account("unreachable-tm"),
        state: { cents: 250 },
      },
    ],
    undefined,
    undefined,
  );
  const a = await openAccount(network, storage, "a", clock);
  const b = await openAccount(network, storage, "b", clock);
  const agent = new TransactionAgent(clock);
  return { clock, network, storage, a, b, agent };
}

describe("pending states form a chain (T5)", () => {
  it("queues a loaded prepared record again, ahead of new work, and builds new work on it", async () => {
    const { storage, a, b, agent } = await inDoubtAtB();

    // T2 moves 10 from a to b while T1 is still in doubt at b.
    const t2 = agent.startTransaction();
    const seen = await inTransaction(t2, async () => {
      await a.performUpdate((s) => (s.cents -= 10));
      return b.performUpdate((s) => (s.cents += 10));
    });
    // Built on T1's pending state, not on the committed 100.
    expect(seen).toBe(260);
    // Left undecided: b will not vote for T2 until T1 is resolved.
    void agent.resolve(t2).catch(() => undefined);
    await settle();
    expect((await durable(storage, "b")).pending).toEqual([
      [1, "T1", 250],
      [2, t2.id, 260],
    ]);
    a.unload();
    b.unload();
  });

  it(
    "issue 78: a live commit does not sweep in an unresolved record below it; when that " +
      "record aborts, the live transaction built on it aborts too",
    async () => {
      const { storage, a, b, agent } = await inDoubtAtB();
      const t2 = agent.startTransaction();
      await inTransaction(t2, async () => {
        await a.performUpdate((s) => (s.cents -= 10));
        await b.performUpdate((s) => (s.cents += 10));
      });
      let outcome: unknown = "pending";
      const resolving = agent.resolve(t2).then(
        () => (outcome = "committed"),
        (error: unknown) => (outcome = error),
      );
      await settle();

      // T2 cannot commit at b ahead of T1: b votes only for the record at the
      // bottom of its queue, so the TM waits, and nothing is promoted.
      expect(outcome).toBe("pending");
      expect(await durable(storage, "b")).toMatchObject({ committedSequenceId: 0 });

      // T1's TM decides abort.
      await b.cancel("T1", 1, "presumedAbort");
      await resolving;
      await settle();

      expect(outcome).toBeInstanceOf(TransactionCascadingAbortError);
      expect(await durable(storage, "b")).toEqual({
        cents: 100,
        committedSequenceId: 0,
        pending: [],
      });
      expect(await durable(storage, "a")).toMatchObject({ cents: 100, pending: [] });
    },
  );

  it(
    "issue 78: an in-doubt record that later commits is not lost because a newer " +
      "transaction was computed without it",
    async () => {
      const { storage, a, b, agent } = await inDoubtAtB();
      const t2 = agent.startTransaction();
      await inTransaction(t2, async () => {
        await a.performUpdate((s) => (s.cents -= 10));
        await b.performUpdate((s) => (s.cents += 10));
      });
      const resolving = agent.resolve(t2);
      await settle();

      // T1's TM confirms it: T1 commits, then T2 votes, commits and is confirmed.
      await b.confirm("T1", 1);
      await resolving;
      await settle();

      expect(await durable(storage, "b")).toEqual({
        cents: 260,
        committedSequenceId: 2,
        pending: [],
      });
      expect(await durable(storage, "a")).toMatchObject({ cents: 90, pending: [] });
    },
  );

  it("cascades an abort to every later pending record built on the aborted one", async () => {
    const clock = new FakeTimeProvider();
    const network = new TransactionNetwork();
    const storage = new MemoryTransactionalStorage();
    const tm1 = await openAccount(network, storage, "tm1", clock);
    const tm2 = await openAccount(network, storage, "tm2", clock);
    const shared = await openAccount(network, storage, "shared", clock);
    const agent = new TransactionAgent(clock);
    const one = { reads: 0, writes: 1 };

    // T1 writes tm1 and shared; shared prepares (votes), tm1 is never asked.
    const t1 = agent.startTransaction();
    await inTransaction(t1, async () => {
      await tm1.performUpdate((s) => (s.cents += 1));
      await shared.performUpdate((s) => (s.cents += 1));
    });
    await shared.prepare(t1.id, one, t1.timeStamp, account("tm1"));

    // T2, built on T1's pending state at shared, prepares behind it.
    const t2 = agent.startTransaction();
    await inTransaction(t2, async () => {
      await tm2.performUpdate((s) => (s.cents += 2));
      await shared.performUpdate((s) => (s.cents += 2));
    });
    await shared.prepare(t2.id, one, t2.timeStamp, account("tm2"));
    const t2Answer = tm2.prepareAndCommit(
      t2.id,
      one,
      t2.timeStamp,
      [account("tm2"), account("shared")],
      2,
    );
    await settle();
    expect((await durable(storage, "shared")).pending.map((p) => p[2])).toEqual([101, 103]);

    // T1 aborts: T2, built on it, aborts as well and its TM hears so.
    await shared.cancel(t1.id, t1.timeStamp, "presumedAbort");
    await expect(t2Answer).resolves.toBe("cascadingAbort");
    await settle();
    expect(await durable(storage, "shared")).toEqual({
      cents: 100,
      committedSequenceId: 0,
      pending: [],
    });
    expect(await durable(storage, "tm2")).toMatchObject({ cents: 100, pending: [] });
  });

  it("orders a read behind the pending write it read, and commits it only after that write", async () => {
    const clock = new FakeTimeProvider();
    const network = new TransactionNetwork();
    const storage = new MemoryTransactionalStorage();
    const tm = await openAccount(network, storage, "tm", clock);
    const b = await openAccount(network, storage, "b", clock);
    const agent = new TransactionAgent(clock);
    const one = { reads: 0, writes: 1 };

    const writer = agent.startTransaction();
    await inTransaction(writer, async () => {
      await tm.performUpdate((s) => (s.cents -= 5));
      await b.performUpdate((s) => (s.cents += 5));
    });
    await b.prepare(writer.id, one, writer.timeStamp, account("tm"));

    const reader = agent.startTransaction(true);
    expect(await inTransaction(reader, () => b.performRead((s) => s.cents))).toBe(105);
    let readCommitted = false;
    const reading = agent.resolve(reader).then(() => (readCommitted = true));
    await settle();
    expect(readCommitted).toBe(false);

    await tm.prepareAndCommit(writer.id, one, writer.timeStamp, [account("tm"), account("b")], 2);
    await reading;
    expect(readCommitted).toBe(true);
    expect(await durable(storage, "b")).toMatchObject({ cents: 105, pending: [] });
  });
});
