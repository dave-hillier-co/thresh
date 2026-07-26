// Ported from dotnet/orleans test/Transactions/Orleans.Transactions.Tests/Memory/TocFaultTransactionMemoryTests.cs @ v10.1.0 (MIT).
//
// TocFaultTransactionMemoryTests inherits its two [Theory]s from
// TocFaultTransactionTestRunnerxUnit
// (src/Orleans.Transactions.TestKit.xUnit/TocFaultTransactionTestRunner.cs).
import { expect } from "vitest";
import { defineGrain, useTransactionalState } from "@thresh/core/define-grain";
import { defineGrainInterface } from "@thresh/core/grain-interface";
import type { GrainWithStringKey } from "@thresh/core/key-kinds";
import { TransactionInDoubtError } from "@thresh/core/errors";
import { TransactionCommitter } from "@thresh/transactions/transaction-committer";
import type { TransactionCommitOperation } from "@thresh/transactions/transaction-committer";
import { TestCluster } from "@thresh/testing/test-cluster";
import { orleansTest } from "@thresh/testing/orleans-test";

// MultiGrainWriteTransactionWithCommitFailure carries
// `[SkippableTheory(Skip = "https://github.com/dotnet/orleans/issues/9556")]`
// upstream — permanently disabled pending that issue, like the sibling
// TocGoldenPathMemoryTests.MultiGrainWriteTransaction (see
// toc-golden-path-memory-tests.test.ts).
orleansTest.excluded(
  "skipped upstream: SkippableTheory(Skip) pending dotnet/orleans#9556",
  "Orleans.Transactions.Tests.TocFaultTransactionMemoryTests.MultiGrainWriteTransactionWithCommitFailure",
);

// MultiGrainWriteTransactionWithCommitException: an `ITransactionCommitterTestGrain`
// acts as a non-grain "commit service" resource (upstream `RemoteCommitService`),
// enlisted directly in the 2PC commit phase via `@thresh/transactions`'
// `TransactionCommitter` (ported alongside `TransactionInDoubtError`,
// `@thresh/core/errors`, and `TransactionAgent`'s commit-phase failure handling —
// see `transaction-committer.ts`/`transaction-agent.ts`). A `ThrowOperation`
// throws once the manager has already durably recorded the commit, which
// surfaces as `TransactionInDoubtError` rather than an ordinary abort — the
// other, already-added grains keep their write regardless.

interface Counter {
  value: number;
}

interface CommitTestGrain extends GrainWithStringKey {
  add(delta: number): Promise<number>;
  get(): Promise<number>;
}

/** The non-grain "service" a `ThrowOperation`/`PassOperation` acts against (Orleans `IRemoteCommitService`). */
interface RemoteCommitService {
  calls: { transactionId: string; data: string }[];
}

interface CommitterTestGrain extends GrainWithStringKey {
  commit(operation: TransactionCommitOperation<RemoteCommitService>): Promise<void>;
}

interface CommitCoordinatorGrain extends GrainWithStringKey {
  multiGrainAdd(
    committerKey: string,
    operation: TransactionCommitOperation<RemoteCommitService>,
    grainKeys: string[],
    delta: number,
  ): Promise<void>;
}

/** Orleans `PassOperation`: always succeeds. */
const passOperation = (data: string): TransactionCommitOperation<RemoteCommitService> => ({
  commit: (transactionId, service) => {
    service.calls.push({ transactionId, data });
    return true;
  },
});

/** Orleans `ThrowOperation`: throws during the commit step itself. */
const throwOperation = (data: string): TransactionCommitOperation<RemoteCommitService> => ({
  commit: (transactionId, service) => {
    service.calls.push({ transactionId, data });
    throw new Error(`transaction ${transactionId} threw with data: ${data}`);
  },
});

const CommitTestGrainInterface = defineGrainInterface<CommitTestGrain>("CommitTestGrain", {
  options: {
    add: { transaction: "createOrJoin" },
    get: { transaction: "createOrJoin" },
  },
});

const CommitterTestGrainInterface = defineGrainInterface<CommitterTestGrain>("CommitterTestGrain", {
  options: { commit: { transaction: "join" } },
});

const CommitCoordinatorGrainInterface = defineGrainInterface<CommitCoordinatorGrain>(
  "CommitCoordinatorGrain",
  { options: { multiGrainAdd: { transaction: "create" } } },
);

const CommitTestGrainImpl = defineGrain<CommitTestGrain>("CommitTestGrain", (ctx) => {
  const data = useTransactionalState<Counter>(ctx, "data", { initial: () => ({ value: 0 }) });
  return {
    add: (delta) =>
      data.performUpdate((s) => {
        s.value += delta;
        return s.value;
      }),
    get: () => data.performRead((s) => s.value),
  };
});

/** The remote commit service instance, shared across activations like upstream's singleton `RemoteCommitService`. */
const remoteCommitService: RemoteCommitService = { calls: [] };

const CommitterTestGrainImpl = defineGrain<CommitterTestGrain>("CommitterTestGrain", (ctx) => {
  const committer = new TransactionCommitter(ctx.id, "committer", remoteCommitService);
  return {
    commit: (operation) => {
      committer.onCommit(operation);
      return Promise.resolve();
    },
  };
});

const CommitCoordinatorGrainImpl = defineGrain<CommitCoordinatorGrain>(
  "CommitCoordinatorGrain",
  (ctx) => ({
    multiGrainAdd: async (committerKey, operation, grainKeys, delta) => {
      const committer = ctx.getGrain(CommitterTestGrainInterface, committerKey);
      await Promise.all([
        ...grainKeys.map((k) => ctx.getGrain(CommitTestGrainInterface, k).add(delta)),
        committer.commit(operation),
      ]);
    },
  }),
);

async function buildCluster(): Promise<TestCluster> {
  return TestCluster.start({
    initialSilos: 1,
    grains: [
      { ctor: CommitTestGrainImpl, interfaces: [CommitTestGrainInterface] },
      { ctor: CommitterTestGrainImpl, interfaces: [CommitterTestGrainInterface] },
      { ctor: CommitCoordinatorGrainImpl, interfaces: [CommitCoordinatorGrainInterface] },
    ],
  });
}

let nextKey = 0;
const newKey = (prefix: string): string => `${prefix}-${(nextKey += 1)}`;

orleansTest(
  "Orleans.Transactions.Tests.TocFaultTransactionMemoryTests.MultiGrainWriteTransactionWithCommitException",
  async () => {
    const cluster = await buildCluster();
    try {
      const expected = 5;
      const committerKey = newKey("committer");
      const grainKeys = [newKey("grain"), newKey("grain"), newKey("grain")];
      const coordinator = cluster.getGrain(CommitCoordinatorGrainInterface, newKey("coord"));

      // Golden path: a PassOperation commits cleanly.
      await coordinator.multiGrainAdd(committerKey, passOperation("pass"), grainKeys, expected);

      // A ThrowOperation fails only the committer's own commit step, after the
      // manager already recorded the commit — TransactionInDoubtError, not an
      // ordinary abort.
      await expect(
        coordinator.multiGrainAdd(committerKey, throwOperation("throw"), grainKeys, expected),
      ).rejects.toBeInstanceOf(TransactionInDoubtError);

      // Every grain's add still landed, in both rounds: the in-doubt failure on
      // the committer does not roll back the other participants' already-durable writes.
      for (const key of grainKeys) {
        expect(await cluster.getGrain(CommitTestGrainInterface, key).get()).toBe(2 * expected);
      }
    } finally {
      await cluster.dispose();
    }
  },
);
