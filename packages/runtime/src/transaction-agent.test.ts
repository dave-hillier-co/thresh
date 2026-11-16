import { describe, expect, it } from "vitest";
import {
  TransactionAbortedError,
  TransactionAlreadyResolvedError,
  TransactionBrokenLockError,
  TransactionInDoubtError,
} from "@thresh/core/errors";
import type {
  AccessCounter,
  AgentCompletedParticipant,
  EnlistedParticipant,
  ParticipantId,
  TransactionalStatus,
  TransactionInfo,
  TransactionManager,
  TransactionParticipant,
} from "@thresh/core/transaction-info";
import { participantKey } from "@thresh/core/transaction-info";
import { GrainId } from "@thresh/core/grain-id";
import { FakeTimeProvider } from "@thresh/runtime/test-support/fake-time-provider";
import { invocationContext, requireTransaction } from "@thresh/runtime/invocation-context";
import { TransactionAgent } from "@thresh/runtime/transaction-agent";

/**
 * A participant that records every protocol message the agent sends it and
 * answers as scripted. It plays both roles, so the agent may elect it TM.
 * The agent's own job is only to start the protocol; what a resource does with
 * the messages is covered by the `@thresh/transactions` tests.
 */
class RecordingParticipant implements TransactionParticipant, TransactionManager {
  readonly received: string[] = [];
  prepareAndCommitArgs: { writers: ParticipantId[]; total: number } | undefined;

  constructor(
    private readonly answer: () => Promise<TransactionalStatus> = async () => "ok",
    private readonly readOnlyAnswer: TransactionalStatus = "ok",
  ) {}

  async commitReadOnly(): Promise<TransactionalStatus> {
    this.received.push("commitReadOnly");
    return this.readOnlyAnswer;
  }
  async prepare(
    _transactionId: string,
    _accessCount: AccessCounter,
    _timeStamp: number,
    manager: ParticipantId,
  ): Promise<void> {
    this.received.push(`prepare(tm=${manager.grainId.key})`);
  }
  async abort(): Promise<void> {
    this.received.push("abort");
  }
  async cancel(_id: string, _ts: number, status: TransactionalStatus): Promise<void> {
    this.received.push(`cancel(${status})`);
  }
  async confirm(): Promise<void> {
    this.received.push("confirm");
  }
  async prepareAndCommit(
    _id: string,
    _access: AccessCounter,
    _ts: number,
    writers: ParticipantId[],
    total: number,
  ): Promise<TransactionalStatus> {
    this.received.push("prepareAndCommit");
    this.prepareAndCommitArgs = { writers, total };
    return this.answer();
  }
  async prepared(): Promise<void> {
    this.received.push("prepared");
  }
  async ping(): Promise<void> {
    this.received.push("ping");
  }
}

/** A participant with only the resource role: it may never be elected TM. */
class ResourceOnlyParticipant implements TransactionParticipant {
  readonly received: string[] = [];
  async commitReadOnly(): Promise<TransactionalStatus> {
    this.received.push("commitReadOnly");
    return "ok";
  }
  async prepare(): Promise<void> {
    this.received.push("prepare");
  }
  async abort(): Promise<void> {
    this.received.push("abort");
  }
  async cancel(): Promise<void> {
    this.received.push("cancel");
  }
  async confirm(): Promise<void> {
    this.received.push("confirm");
  }
}

/** A resource the agent completes itself (the port's `TransactionCommitter`). */
class AgentCompleted implements AgentCompletedParticipant {
  readonly completedByAgent = true as const;
  readonly received: string[] = [];
  constructor(private readonly onCommit: () => Promise<void> = async () => undefined) {}
  async commit(): Promise<void> {
    this.received.push("commit");
    await this.onCommit();
  }
  async abort(): Promise<void> {
    this.received.push("abort");
  }
}

const id = (key: string): ParticipantId => ({ grainId: new GrainId("test", key), stateName: "s" });

function enlist(
  info: TransactionInfo,
  key: string,
  participant: EnlistedParticipant["participant"],
  access: AccessCounter = { reads: 0, writes: 1 },
): void {
  info.participants.set(participantKey(id(key)), { id: id(key), participant, access });
}

/** Let one-way sends land. */
const settle = () => new Promise<void>((resolve) => setImmediate(resolve));

describe("TransactionAgent.resolve starts the commit and leaves the rest to the TM (T1)", () => {
  it("prepares every participant but the TM one-way, and awaits prepareAndCommit on the TM", async () => {
    const agent = new TransactionAgent(new FakeTimeProvider());
    const info = agent.startTransaction();
    const tm = new RecordingParticipant();
    const writer = new RecordingParticipant();
    const reader = new RecordingParticipant();
    enlist(info, "tm", tm);
    enlist(info, "writer", writer);
    enlist(info, "reader", reader, { reads: 1, writes: 0 });

    await agent.resolve(info);
    await settle();

    expect(tm.received).toEqual(["prepareAndCommit"]);
    expect(tm.prepareAndCommitArgs?.writers.map((p) => p.grainId.key)).toEqual(["tm", "writer"]);
    expect(tm.prepareAndCommitArgs?.total).toBe(3);
    // Nothing but the one-way prepare: the TM confirms them, not the agent.
    expect(writer.received).toEqual(["prepare(tm=tm)"]);
    expect(reader.received).toEqual(["prepare(tm=tm)"]);
    expect(info.status).toBe("committed");
  });

  it("commits a read-only transaction in one phase with commitReadOnly", async () => {
    const agent = new TransactionAgent(new FakeTimeProvider());
    const info = agent.startTransaction(true);
    const a = new RecordingParticipant();
    const b = new RecordingParticipant();
    enlist(info, "a", a, { reads: 1, writes: 0 });
    enlist(info, "b", b, { reads: 2, writes: 0 });

    await agent.resolve(info);

    expect(a.received).toEqual(["commitReadOnly"]);
    expect(b.received).toEqual(["commitReadOnly"]);
  });

  it("aborts every participant of a read-only transaction when one reports a broken lock", async () => {
    const agent = new TransactionAgent(new FakeTimeProvider());
    const info = agent.startTransaction(true);
    const a = new RecordingParticipant();
    const b = new RecordingParticipant(undefined, "brokenLock");
    enlist(info, "a", a, { reads: 1, writes: 0 });
    enlist(info, "b", b, { reads: 1, writes: 0 });

    await expect(agent.resolve(info)).rejects.toBeInstanceOf(TransactionBrokenLockError);

    expect(a.received).toEqual(["commitReadOnly", "abort"]);
    expect(b.received).toEqual(["commitReadOnly", "abort"]);
  });

  it("cancels the other write participants when the TM reports a definite abort", async () => {
    const agent = new TransactionAgent(new FakeTimeProvider());
    const info = agent.startTransaction();
    const tm = new RecordingParticipant(async () => "lockValidationFailed");
    const writer = new RecordingParticipant();
    const reader = new RecordingParticipant();
    enlist(info, "tm", tm);
    enlist(info, "writer", writer);
    enlist(info, "reader", reader, { reads: 1, writes: 0 });

    const failure = agent.resolve(info);

    await expect(failure).rejects.toBeInstanceOf(TransactionAbortedError);
    await settle();
    expect(writer.received).toEqual(["prepare(tm=tm)", "cancel(lockValidationFailed)"]);
    expect(reader.received).toEqual(["prepare(tm=tm)"]);
  });

  it(
    "raises TransactionInDoubtError, and cancels nobody, when prepareAndCommit fails without " +
      "an answer: the TM may have committed",
    async () => {
      const agent = new TransactionAgent(new FakeTimeProvider());
      const info = agent.startTransaction();
      const tm = new RecordingParticipant(async () => {
        throw new Error("the TM's reply was lost");
      });
      const writer = new RecordingParticipant();
      enlist(info, "tm", tm);
      enlist(info, "writer", writer);

      const failure = agent.resolve(info);

      await expect(failure).rejects.toBeInstanceOf(TransactionInDoubtError);
      await expect(failure).rejects.not.toBeInstanceOf(TransactionAbortedError);
      await settle();
      expect(writer.received).toEqual(["prepare(tm=tm)"]);
    },
  );
});

/** Run `fn` as if it were a turn executing inside `tx`. */
function inTurn<R>(tx: TransactionInfo, fn: () => R): R {
  return invocationContext.run({ senderId: undefined, ownerId: undefined, transaction: tx }, fn);
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
  // committed and never aborted. The agent therefore records where the
  // transaction stands, and `requireTransaction` refuses a late enlistment
  // outright rather than letting it take a lock nothing will release.
  it("marks the transaction committed once its boundary has committed it", async () => {
    const agent = new TransactionAgent(new FakeTimeProvider());
    const info = agent.startTransaction();
    const a = new RecordingParticipant();
    enlist(info, "a", a);

    await agent.resolve(info);

    expect(info.status).toBe("committed");
    expect(a.received).toEqual(["prepareAndCommit"]);
  });

  it("marks the transaction aborted when its boundary aborts it", async () => {
    const agent = new TransactionAgent(new FakeTimeProvider());
    const info = agent.startTransaction();
    const a = new RecordingParticipant();
    enlist(info, "a", a);

    await agent.abort(info);

    expect(info.status).toBe("aborted");
    expect(a.received).toEqual(["abort"]);
  });

  it("refuses an enlistment once the transaction has resolved, before any lock is taken", async () => {
    const agent = new TransactionAgent(new FakeTimeProvider());
    const info = agent.startTransaction();
    await agent.resolve(info);

    expect(() => inTurn(info, () => requireTransaction())).toThrow(TransactionAlreadyResolvedError);
  });

  it("seals the participant set as soon as resolution begins, not only once it has decided", async () => {
    // The window matters: resolving is asynchronous (the TM stages durably),
    // and a detached callee's turn — a `oneWay` + `transaction: "supported"`
    // call, whose caller resolved the moment its call returned — can land in
    // it. Enlisting then would still be too late for the participant snapshot
    // the round is already working from, so the set must close at the start.
    const agent = new TransactionAgent(new FakeTimeProvider());
    const info = agent.startTransaction();
    const answer = deferred<TransactionalStatus>();
    enlist(info, "a", new RecordingParticipant(() => answer.promise));

    const resolving = agent.resolve(info);
    expect(info.status).toBe("resolving");
    expect(() => inTurn(info, () => requireTransaction())).toThrow(TransactionAlreadyResolvedError);

    answer.resolve("ok");
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

describe("TransactionAgent manager election", () => {
  // Orleans elects the transaction manager from the *write* participants that
  // support the Manager role (`CollateParticipants`, `ParticipantId.Role`).
  it("skips a resource-only writer that enlisted before a manager-capable one", async () => {
    const agent = new TransactionAgent(new FakeTimeProvider());
    const info = agent.startTransaction();
    const resourceOnly = new ResourceOnlyParticipant();
    const manager = new RecordingParticipant();
    enlist(info, "first", resourceOnly);
    enlist(info, "state", manager);

    await agent.resolve(info);
    await settle();

    expect(manager.received).toEqual(["prepareAndCommit"]);
    expect(resourceOnly.received).toEqual(["prepare"]);
  });

  it("aborts a transaction no write participant can manage", async () => {
    const agent = new TransactionAgent(new FakeTimeProvider());
    const info = agent.startTransaction();
    const resourceOnly = new ResourceOnlyParticipant();
    enlist(info, "only", resourceOnly);

    await expect(agent.resolve(info)).rejects.toBeInstanceOf(TransactionAbortedError);
    expect(resourceOnly.received).toEqual(["abort"]);
  });
});

describe("TransactionAgent and an agent-completed resource (the port's committer)", () => {
  it("commits it only after the TM has committed, outside the protocol", async () => {
    const agent = new TransactionAgent(new FakeTimeProvider());
    const info = agent.startTransaction();
    const committer = new AgentCompleted();
    const manager = new RecordingParticipant();
    enlist(info, "committer", committer);
    enlist(info, "state", manager);

    await agent.resolve(info);

    expect(manager.prepareAndCommitArgs?.writers.map((p) => p.grainId.key)).toEqual(["state"]);
    expect(manager.prepareAndCommitArgs?.total).toBe(1);
    expect(committer.received).toEqual(["commit"]);
  });

  it("raises TransactionInDoubtError, not an abort, when its commit step fails", async () => {
    const agent = new TransactionAgent(new FakeTimeProvider());
    const info = agent.startTransaction();
    const committer = new AgentCompleted(async () => {
      throw new Error("external resource threw during commit");
    });
    enlist(info, "committer", committer);
    enlist(info, "state", new RecordingParticipant());

    const failure = agent.resolve(info);

    await expect(failure).rejects.toBeInstanceOf(TransactionInDoubtError);
    await expect(failure).rejects.not.toBeInstanceOf(TransactionAbortedError);
  });

  it("does not commit it when the TM aborts", async () => {
    const agent = new TransactionAgent(new FakeTimeProvider());
    const info = agent.startTransaction();
    const committer = new AgentCompleted();
    enlist(info, "committer", committer);
    enlist(info, "state", new RecordingParticipant(async () => "prepareTimeout"));

    await expect(agent.resolve(info)).rejects.toBeInstanceOf(TransactionAbortedError);
    expect(committer.received).toEqual([]);
  });

  it("commits it directly when it is the only writer", async () => {
    const agent = new TransactionAgent(new FakeTimeProvider());
    const info = agent.startTransaction();
    const committer = new AgentCompleted();
    enlist(info, "committer", committer);

    await agent.resolve(info);

    expect(committer.received).toEqual(["commit"]);
    expect(info.status).toBe("committed");
  });
});
