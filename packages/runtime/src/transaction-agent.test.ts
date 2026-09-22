import { describe, expect, it } from "vitest";
import {
  TransactionAbortedError,
  TransactionAlreadyResolvedError,
  TransactionInDoubtError,
} from "@thresh/core/errors";
import type {
  EnlistedParticipant,
  TransactionInfo,
  TransactionParticipant,
} from "@thresh/core/transaction-info";
import { participantKey } from "@thresh/core/transaction-info";
import { GrainId } from "@thresh/core/grain-id";
import { FakeTimeProvider } from "@thresh/runtime/test-support/fake-time-provider";
import { invocationContext, requireTransaction } from "@thresh/runtime/invocation-context";
import { TransactionAgent } from "@thresh/runtime/transaction-agent";

/** A trivial in-memory participant recording prepare/commit/abort calls. */
class FakeParticipant implements TransactionParticipant {
  committed = false;
  aborted = false;

  constructor(
    private readonly onCommit?: () => void | Promise<void>,
    private readonly onRecordCommit?: () => void | Promise<void>,
  ) {}

  prepare(): boolean {
    return true;
  }

  async commit(): Promise<void> {
    if (this.onCommit !== undefined) await this.onCommit();
    this.committed = true;
  }

  abort(): void {
    this.aborted = true;
  }

  async recordCommit(): Promise<void> {
    if (this.onRecordCommit !== undefined) await this.onRecordCommit();
  }
}

function enlist(
  grainId: string,
  stateName: string,
  participant: TransactionParticipant,
): EnlistedParticipant {
  return {
    id: { grainId: new GrainId("test", grainId), stateName },
    participant,
    access: { reads: 0, writes: 1 },
  };
}

describe("TransactionAgent.resolve", () => {
  it("commits every enlisted participant on the golden path", async () => {
    const agent = new TransactionAgent(new FakeTimeProvider());
    const info = agent.startTransaction();
    const a = new FakeParticipant();
    const b = new FakeParticipant();
    info.participants.set(participantKey(enlist("a", "s", a).id), enlist("a", "s", a));
    info.participants.set(participantKey(enlist("b", "s", b).id), enlist("b", "s", b));

    await agent.resolve(info);

    expect(a.committed).toBe(true);
    expect(b.committed).toBe(true);
  });

  it(
    "raises TransactionInDoubtError — not TransactionAbortedError — when a participant's " +
      "commit step itself fails after the manager already recorded the commit, and other " +
      "participants still get their commit applied",
    async () => {
      const agent = new TransactionAgent(new FakeTimeProvider());
      const info = agent.startTransaction();
      const ok = new FakeParticipant();
      const faulty = new FakeParticipant(() => {
        throw new Error("external resource threw during commit");
      });
      info.participants.set(participantKey(enlist("ok", "s", ok).id), enlist("ok", "s", ok));
      info.participants.set(
        participantKey(enlist("faulty", "s", faulty).id),
        enlist("faulty", "s", faulty),
      );

      const failure = agent.resolve(info);

      await expect(failure).rejects.toBeInstanceOf(TransactionInDoubtError);
      await expect(failure).rejects.not.toBeInstanceOf(TransactionAbortedError);
      // The other participant's write still lands: the commit decision was
      // already durable, so an in-doubt failure elsewhere does not roll it back.
      expect(ok.committed).toBe(true);
    },
  );

  it(
    "raises TransactionInDoubtError — never aborting a prepared participant — when the elected " +
      "manager's own recordCommit fails: whether the durable record landed before the failure " +
      "is unknowable, so recovery (querying the TM) is the only safe way to resolve it",
    async () => {
      const agent = new TransactionAgent(new FakeTimeProvider());
      const info = agent.startTransaction();
      const manager = new FakeParticipant(undefined, () => {
        throw new Error("TM write timed out — ack lost");
      });
      const other = new FakeParticipant();
      info.participants.set(
        participantKey(enlist("manager", "s", manager).id),
        enlist("manager", "s", manager),
      );
      info.participants.set(
        participantKey(enlist("other", "s", other).id),
        enlist("other", "s", other),
      );

      const failure = agent.resolve(info);

      await expect(failure).rejects.toBeInstanceOf(TransactionInDoubtError);
      await expect(failure).rejects.not.toBeInstanceOf(TransactionAbortedError);
      // Every participant was already prepared and the commit may or may not
      // have been durably recorded — neither may be rolled back.
      expect(manager.aborted).toBe(false);
      expect(other.aborted).toBe(false);
      expect(manager.committed).toBe(false);
      expect(other.committed).toBe(false);
    },
  );
});

/** Run `fn` as if it were a turn executing inside `tx`. */
function inTurn<R>(tx: TransactionInfo, fn: () => R): R {
  return invocationContext.run(
    { senderId: undefined, ownerId: undefined, reentrancyId: tx.id, transaction: tx },
    fn,
  );
}

/** A promise plus its resolve, so a test can hold a participant mid-round. */
function deferred<T>(): { promise: Promise<T>; resolve: (value: T) => void } {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>((r) => {
    resolve = r;
  });
  return { promise, resolve };
}

describe("TransactionAgent resolution status", () => {
  // The participant set is snapshotted once, when the boundary resolves the
  // transaction, so a resource enlisting afterwards is never prepared, never
  // committed and never aborted — and the only release points for the state
  // locks are commit/abort. The agent therefore records where the transaction
  // stands, and `requireTransaction` refuses a late enlistment outright rather
  // than letting it take a lock nothing will release.
  it("marks the transaction committed once its boundary has committed it", async () => {
    const agent = new TransactionAgent(new FakeTimeProvider());
    const info = agent.startTransaction();
    const a = new FakeParticipant();
    info.participants.set(participantKey(enlist("a", "s", a).id), enlist("a", "s", a));

    await agent.resolve(info);

    expect(info.status).toBe("committed");
    expect(a.committed).toBe(true);
  });

  it("marks the transaction aborted when its boundary aborts it", async () => {
    const agent = new TransactionAgent(new FakeTimeProvider());
    const info = agent.startTransaction();
    const a = new FakeParticipant();
    info.participants.set(participantKey(enlist("a", "s", a).id), enlist("a", "s", a));

    await agent.abort(info);

    expect(info.status).toBe("aborted");
    expect(a.aborted).toBe(true);
  });

  it("refuses an enlistment once the transaction has resolved, before any lock is taken", async () => {
    const agent = new TransactionAgent(new FakeTimeProvider());
    const info = agent.startTransaction();
    await agent.resolve(info);

    expect(() => inTurn(info, () => requireTransaction())).toThrow(TransactionAlreadyResolvedError);
  });

  it("seals the participant set as soon as resolution begins, not only once it has decided", async () => {
    // The window matters: resolving is asynchronous (prepare stages durably),
    // and a detached callee's turn — a `oneWay` + `transaction: "supported"`
    // call, whose caller resolved the moment its call returned — can land in
    // it. Enlisting then would still be too late for the participant snapshot
    // the round is already working from, so the set must close at the start.
    const agent = new TransactionAgent(new FakeTimeProvider());
    const info = agent.startTransaction();
    const prepare = deferred<boolean>();
    const blocking: TransactionParticipant = {
      prepare: () => prepare.promise,
      commit: () => {},
      abort: () => {},
      recordCommit: () => {},
    };
    info.participants.set(
      participantKey(enlist("a", "s", blocking).id),
      enlist("a", "s", blocking),
    );

    const resolving = agent.resolve(info);
    expect(info.status).toBe("resolving");
    expect(() => inTurn(info, () => requireTransaction())).toThrow(TransactionAlreadyResolvedError);

    prepare.resolve(true);
    await resolving;
    expect(info.status).toBe("committed");
  });

  it("leaves an active transaction's status alone for a resource to enlist normally", async () => {
    const agent = new TransactionAgent(new FakeTimeProvider());
    const info = agent.startTransaction();

    expect(info.status).toBe("active");
    expect(inTurn(info, () => requireTransaction())).toBe(info);
  });
});
