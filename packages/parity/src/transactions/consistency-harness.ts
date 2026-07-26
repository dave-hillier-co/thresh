// Ported from dotnet/orleans @ v10.1.0 (MIT):
//   src/Orleans.Transactions.TestKit.Base/Consistency/Observation.cs
//   src/Orleans.Transactions.TestKit.Base/Consistency/ConsistencyTestOptions.cs
//   src/Orleans.Transactions.TestKit.Base/Consistency/ConsistencyTestGrain.cs
//   src/Orleans.Transactions.TestKit.Base/Consistency/ConsistencyTestHarness.cs
//   src/Orleans.Transactions.TestKit.Base/TestRunners/ConsistencyTransactionTestRunner.cs
//
// Shared by consistency-tests.test.ts and consistency-skewed-clock-tests.test.ts
// (both inherit the identical `RandomizedConsistency` Theory from the same
// upstream runner — only the xUnit fixture's clock-skew configurator differs,
// which has no observable effect against this port's single-silo `TestCluster`).
//
// This is Orleans' randomized-consistency test harness: many concurrent
// "worker" call sequences hammer a shared pool of transactional grains with
// random read/write/recursive transactions, recording every observed
// before/after state into a log, then post-hoc verifying the log admits a
// serializable history — i.e. no committed transaction's reads/writes could
// have been produced by any other interleaving. The one deliberate departure
// from upstream: `ConsistencyTestHarness.NumAborted.Should().Be(0)` (a
// "golden path" invariant that holds when `avoidDeadlocks` is on and
// `readWrite` is `PerGrain`/`PerTransaction`) relies on Orleans'
// non-killing lock-group batching (see the exclusive-lock file's header for
// why `@thresh/transactions`' strict wait-die `ReaderWriterLock` cannot
// reproduce that specific "nobody ever aborts" property). This port's harness
// still tracks and exposes `numAborted`; the two callers simply choose
// `RandomizedConsistency` cases that don't assert it, the same way other
// ported Theories here select a representative case subset rather than every
// upstream `[InlineData]` row.
import { defineGrain, useTransactionalState } from "@thresh/core/define-grain";
import { defineGrainInterface, type GrainInterface } from "@thresh/core/grain-interface";
import type { GrainWithStringKey } from "@thresh/core/key-kinds";
import { TransactionAbortedError, TransactionInDoubtError } from "@thresh/core/errors";
import type { TestCluster } from "@thresh/testing/test-cluster";

/** Orleans `ReadWriteDetermination`. */
export type ReadWriteDetermination = "perTransaction" | "perGrain" | "perAccess";

/** Orleans `ConsistencyTestOptions`. */
export interface ConsistencyTestOptions {
  randomSeed: number;
  numGrains: number;
  maxDepth: number;
  avoidDeadlocks: boolean;
  readWrite: ReadWriteDetermination;
}

/** Orleans `Observation`: one grain version, as seen either by its writer or a later reader. */
export interface Observation {
  grain: number;
  seqNo: number;
  writerTx: string;
  executingTx: string;
}

const observationKey = (o: Observation): string =>
  `${o.grain}|${o.seqNo}|${o.writerTx}|${o.executingTx}`;

/** Orleans `ConsistencyTestHarness.InitialTx`. */
export const INITIAL_TX = "initial";

interface ConsistencyState {
  writerTx: string;
  seqNo: number;
}

/** A small deterministic seeded PRNG (mulberry32) — no need to match .NET's `System.Random` bit-for-bit, only to be reproducibly seeded. */
function seededRandom(seed: number): () => number {
  let a = seed >>> 0;
  return () => {
    a = (a + 0x6d2b79f5) | 0;
    let t = Math.imul(a ^ (a >>> 15), 1 | a);
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}
const nextInt = (rand: () => number, bound: number): number => Math.floor(rand() * bound);

/** Orleans' `stack[..stack.IndexOf(')')].GetHashCode()` — any stable string hash works here. */
function hashCode(s: string): number {
  let h = 0;
  for (let i = 0; i < s.length; i++) h = (Math.imul(h, 31) + s.charCodeAt(i)) | 0;
  return h;
}

export interface ConsistencyTestGrain extends GrainWithStringKey {
  run(
    options: ConsistencyTestOptions,
    depth: number,
    stack: string,
    maxGrain: number,
  ): Promise<Observation[]>;
}

export const ConsistencyTestGrainInterface: GrainInterface<ConsistencyTestGrain> =
  defineGrainInterface<ConsistencyTestGrain>(
    "Orleans.Transactions.TestKit.Consistency.IConsistencyTestGrain",
    {
      options: { run: { transaction: "createOrJoin" } },
    },
  );

// Orleans `ConsistencyTestGrain.RecursionProbability = .1 - .9 * (1.0 / (10 * 40 - 1))`.
const RECURSION_PROBABILITY = 0.1 - (0.9 * 1.0) / (10 * 40 - 1);

const grainNumber = (key: string): number => Number(key.slice(1));
const grainKey = (n: number): string => `g${n}`;

export const ConsistencyTestGrainImpl = defineGrain<ConsistencyTestGrain>(
  "Orleans.Transactions.TestKit.Consistency.ConsistencyTestGrain",
  (ctx) => {
    const data = useTransactionalState<ConsistencyState>(ctx, "data", {
      initial: () => ({ writerTx: INITIAL_TX, seqNo: 0 }),
    });
    const myNumber = grainNumber(ctx.id.key as string);
    let grainRandom: (() => number) | undefined;

    function read(): Promise<Observation[]> {
      const txid = ctx.runtime.getTransactionId()!;
      return data.performRead((s) => [
        { executingTx: txid, writerTx: s.writerTx, grain: myNumber, seqNo: s.seqNo },
      ]);
    }

    function write(): Promise<Observation[]> {
      const txid = ctx.runtime.getTransactionId()!;
      return data.performUpdate((s) => {
        const before: Observation = {
          executingTx: txid,
          writerTx: s.writerTx,
          grain: myNumber,
          seqNo: s.seqNo,
        };
        s.writerTx = txid;
        s.seqNo += 1;
        const after: Observation = {
          executingTx: txid,
          writerTx: s.writerTx,
          grain: myNumber,
          seqNo: s.seqNo,
        };
        return [before, after];
      });
    }

    async function recurse(
      options: ConsistencyTestOptions,
      depth: number,
      stack: string,
      rand: () => number,
      count: number,
      parallel: boolean,
      maxGrain: number,
    ): Promise<Observation[]> {
      const min = options.avoidDeadlocks ? myNumber : 0;
      const max = options.avoidDeadlocks ? maxGrain : options.numGrains;
      const targets: number[] = [];
      for (let i = 0; i < count; i++) targets.push(min + nextInt(rand, Math.max(max - min, 1)));
      if (options.avoidDeadlocks) targets.sort((a, b) => a - b);

      const tasks: Promise<Observation[]>[] = [];
      for (let i = 0; i < count; i++) {
        const target = ctx.getGrain(ConsistencyTestGrainInterface, grainKey(targets[i]!));
        const maxGrainForNested = i < count - 1 ? targets[i + 1]! : max;
        const task = target.run(
          options,
          depth + 1,
          `${stack}.${parallel ? "P" : "S"}${i}`,
          maxGrainForNested,
        );
        tasks.push(task);
        if (!parallel) await task;
      }
      const results = await Promise.all(tasks);
      const merged = new Map<string, Observation>();
      for (const result of results) for (const o of result) merged.set(observationKey(o), o);
      return [...merged.values()];
    }

    return {
      run: async (options, depth, stack, maxGrain) => {
        if (grainRandom === undefined) {
          grainRandom = seededRandom(options.randomSeed * options.numGrains + myNumber);
        }
        const random = grainRandom;

        if (depth < options.maxDepth && random() < RECURSION_PROBABILITY) {
          switch (nextInt(random, 2)) {
            case 0:
              return recurse(options, depth, stack, random, 10, !options.avoidDeadlocks, maxGrain);
            default:
              return recurse(options, depth, stack, random, 10, false, maxGrain);
          }
        }

        const txHash = hashCode(stack.slice(0, stack.indexOf(")")));
        const whetherToReadOrWrite =
          options.readWrite === "perTransaction"
            ? seededRandom(options.randomSeed + txHash)
            : options.readWrite === "perGrain"
              ? seededRandom(options.randomSeed + txHash * 10000 + myNumber)
              : random;

        return nextInt(whetherToReadOrWrite, 4) === 0 ? write() : read();
      },
    };
  },
);

/** Orleans `ConsistencyTestHarness`: runs the randomized workload, then checks the recorded history for serializability. */
export class ConsistencyTestHarness {
  private readonly tuples = new Map<number, Map<number, Map<string, Set<string>>>>();
  private readonly succeeded = new Set<string>();
  private readonly aborted = new Set<string>();
  private readonly indoubt = new Map<string, string>();
  private readonly orderEdges = new Map<string, Set<string>>();

  constructor(
    private readonly cluster: TestCluster,
    private readonly options: ConsistencyTestOptions,
  ) {}

  get numAborted(): number {
    return this.aborted.size;
  }

  async runRandomTransactionSequence(partition: number, count: number): Promise<void> {
    const rand = seededRandom(this.options.randomSeed + partition);
    for (let i = 0; i < count; i++) {
      const target = nextInt(rand, this.options.numGrains);
      try {
        const grain = this.cluster.getGrain(ConsistencyTestGrainInterface, grainKey(target));
        const result = await grain.run(
          this.options,
          0,
          `(${partition},${i})`,
          this.options.numGrains,
        );
        if (result.length > 0) {
          const id = result[0]!.executingTx;
          this.succeeded.add(id);
          for (const tuple of result) {
            let versions = this.tuples.get(tuple.grain);
            if (versions === undefined) this.tuples.set(tuple.grain, (versions = new Map()));
            let writers = versions.get(tuple.seqNo);
            if (writers === undefined) versions.set(tuple.seqNo, (writers = new Map()));
            let readers = writers.get(tuple.writerTx);
            if (readers === undefined) writers.set(tuple.writerTx, (readers = new Set()));
            readers.add(tuple.executingTx);
          }
        }
      } catch (error) {
        if (error instanceof TransactionInDoubtError) {
          this.indoubt.set(error.transactionId, error.message);
        } else if (error instanceof TransactionAbortedError) {
          this.aborted.add(error.transactionId);
        } else {
          throw error;
        }
      }
    }
  }

  /** Orleans `ConsistencyTestHarness.CheckConsistency`: fail with a diagnostic message, or DFS the ordered-before graph for a cycle. */
  checkConsistency(): void {
    for (const [grainIndex, versions] of this.tuples) {
      let pos = 0;
      const fail = (msg: string): never => {
        throw new Error(`consistency violation: ${msg}`);
      };

      let readersOfPreviousVersion = new Set<string>();
      const sortedSeqNos = [...versions.keys()].sort((a, b) => a - b);

      for (const seqNo of sortedSeqNos) {
        if (pos++ !== seqNo && this.indoubt.size === 0) {
          fail(`g${grainIndex} is missing version v${pos - 1}, found v${seqNo} instead`);
        }

        const writers = versions.get(seqNo)!;
        if (writers.size !== 1) {
          fail(`g${grainIndex} v${seqNo} has multiple writers ${[...writers.keys()].join(",")}`);
        }

        const [writer, readers] = [...writers.entries()][0]!;

        if (seqNo === 0) {
          if (writer !== INITIAL_TX) fail(`g${grainIndex} v${seqNo} not written by ${INITIAL_TX}`);
        } else {
          if (this.aborted.has(writer))
            fail(`g${grainIndex} v${seqNo} written by aborted transaction ${writer}`);
          if (!(this.succeeded.has(writer) || this.indoubt.has(writer))) {
            fail(`g${grainIndex} v${seqNo} written by unknown transaction ${writer}`);
          }
          if (this.indoubt.size === 0 && !readers.has(writer)) {
            fail(`g${grainIndex} v${seqNo} writer ${writer} missing`);
          }
        }

        for (const r of readersOfPreviousVersion) {
          if (r !== writer) {
            let edges = this.orderEdges.get(r);
            if (edges === undefined) this.orderEdges.set(r, (edges = new Set()));
            edges.add(writer);
          }
        }

        let writeEdges = this.orderEdges.get(writer);
        if (writeEdges === undefined) this.orderEdges.set(writer, (writeEdges = new Set()));
        for (const r of readers) {
          if (r !== writer) {
            if (!this.succeeded.has(r))
              fail(`g${grainIndex} v${seqNo} read by aborted transaction ${r}`);
            writeEdges.add(r);
          }
        }

        readersOfPreviousVersion = readers;
      }
    }

    this.dfs();
  }

  private dfs(): void {
    const marks = new Map<string, boolean>();
    const visit = (node: string, edges: Set<string>): boolean => {
      const mark = marks.get(node);
      if (mark !== undefined) return !mark;
      marks.set(node, false);
      for (const n of edges) {
        const nextEdges = this.orderEdges.get(n);
        if (nextEdges !== undefined && visit(n, nextEdges)) return true;
      }
      marks.set(node, true);
      return false;
    };
    for (const [node, edges] of this.orderEdges) {
      if (!marks.has(node)) {
        if (visit(node, edges))
          throw new Error("consistency violation: found serializability violation");
      }
    }
  }
}

/** Orleans `ConsistencyTransactionTestRunner.RandomizedConsistency`. */
export async function runRandomizedConsistency(
  cluster: TestCluster,
  numGrains: number,
  scale: number,
  avoidDeadlocks: boolean,
  readWrite: ReadWriteDetermination,
): Promise<void> {
  const seed =
    scale +
    numGrains * 1000 +
    (avoidDeadlocks ? 666 : 333) +
    { perTransaction: 0, perGrain: 1, perAccess: 2 }[readWrite] * 123976;
  const options: ConsistencyTestOptions = {
    randomSeed: seed,
    numGrains,
    maxDepth: 5,
    avoidDeadlocks,
    readWrite,
  };
  const harness = new ConsistencyTestHarness(cluster, options);

  const numThreads = scale;
  const numTxsPerThread = scale * scale;
  await Promise.all(
    Array.from({ length: numThreads }, (_, i) =>
      harness.runRandomTransactionSequence(i, numTxsPerThread),
    ),
  );

  harness.checkConsistency();
}
