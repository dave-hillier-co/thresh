import { describe, expect, it } from "vitest";
import { TransactionAbortedError, TransactionInDoubtError } from "@thresh/core/errors";
import type { EnlistedParticipant, TransactionParticipant } from "@thresh/core/transaction-info";
import { participantKey } from "@thresh/core/transaction-info";
import { GrainId } from "@thresh/core/grain-id";
import { FakeTimeProvider } from "@thresh/runtime/test-support/fake-time-provider";
import { TransactionAgent } from "@thresh/runtime/transaction-agent";

/** A trivial in-memory participant recording prepare/commit/abort calls. */
class FakeParticipant implements TransactionParticipant {
  committed = false;
  aborted = false;

  constructor(private readonly onCommit?: () => void | Promise<void>) {}

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

  recordCommit(): void {
    // not elected manager in these tests
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
});
