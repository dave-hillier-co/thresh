// Ported from dotnet/orleans test/Transactions/Orleans.Transactions.Tests/Memory/TransactionAttributionTest.cs @ v10.1.0 (MIT).
//
// TransactionAttributionTest inherits every [Fact] from
// TransactionAttributionTestRunner
// (test/Transactions/Orleans.Transactions.Tests/Runners/
// TransactionAttributionTestRunner.cs), which drives six grain variants — one
// per `[Transaction(TransactionOption...)]` value (no attribute, Suppress,
// CreateOrJoin, Create, Join, Supported, NotAllowed) — through
// `ITransactionAttributionGrain.GetNestedTransactionIds`, a recursive call
// tree that has each tier read `TransactionContext.GetTransactionInfo()?.Id`
// and report it back up, so the test can assert which tiers see no ambient
// transaction, which see a fresh one, and which see the parent's.
//
// `GrainRuntime.getTransactionId()`/`isInTransaction()` (@tsva/core/grain-runtime)
// now expose that introspection, delegating to the runtime-internal
// `currentTransaction()` (@tsva/runtime/invocation-context). The transaction
// boundary itself (`InvokeMethodOptions.transaction` /
// `GrainFactory.resolveTransaction`, @tsva/runtime/grain-factory) was already
// faithful — this file was gapped purely for lack of a way for grain code to
// read the outcome. `packages/parity/src/grains/{interfaces,impl}/
// transaction-attribution-grain*.ts` ports upstream's seven grain variants,
// keyed by `TransactionOption` (plus `"none"`) rather than by C# attribute.
import { expect } from "vitest";
import { TestCluster } from "@tsva/testing/test-cluster";
import { orleansTest } from "@tsva/testing/orleans-test";
import {
  attributionGrainInterfaceFor,
  CreateAttributionGrainInterface,
  CreateOrJoinAttributionGrainInterface,
  JoinAttributionGrainInterface,
  NoAttributionGrainInterface,
  NotAllowedAttributionGrainInterface,
  SuppressAttributionGrainInterface,
  SupportedAttributionGrainInterface,
  type AttributionOption,
  type AttributionTierEntry,
} from "@tsva/parity/grains/interfaces/transaction-attribution-grain-interfaces";
import {
  CreateAttributionGrain,
  CreateOrJoinAttributionGrain,
  JoinAttributionGrain,
  NoAttributionGrain,
  NotAllowedAttributionGrain,
  SuppressAttributionGrain,
  SupportedAttributionGrain,
} from "@tsva/parity/grains/impl/transaction-attribution-grain";

let nextKey = 0;
const newKey = (): string => `attribution-${(nextKey += 1)}`;

async function buildCluster(): Promise<TestCluster> {
  return TestCluster.start({
    initialSilos: 1,
    grains: [
      { ctor: NoAttributionGrain, interfaces: [NoAttributionGrainInterface] },
      { ctor: SuppressAttributionGrain, interfaces: [SuppressAttributionGrainInterface] },
      { ctor: CreateOrJoinAttributionGrain, interfaces: [CreateOrJoinAttributionGrainInterface] },
      { ctor: CreateAttributionGrain, interfaces: [CreateAttributionGrainInterface] },
      { ctor: JoinAttributionGrain, interfaces: [JoinAttributionGrainInterface] },
      { ctor: SupportedAttributionGrain, interfaces: [SupportedAttributionGrainInterface] },
      { ctor: NotAllowedAttributionGrain, interfaces: [NotAllowedAttributionGrainInterface] },
    ],
  });
}

function entry(option: AttributionOption): AttributionTierEntry {
  return { key: newKey(), option };
}

function topGrain(cluster: TestCluster, option: AttributionOption) {
  return cluster.getGrain(attributionGrainInterfaceFor(option), newKey());
}

orleansTest(
  "Orleans.Transactions.Tests.TransactionAttributionTest.AllSupportedAttributesFromOutsideTransactionTest",
  async () => {
    const cluster = await buildCluster();
    try {
      const top = topGrain(cluster, "none");
      const tiers: AttributionTierEntry[][] = [
        [
          entry("none"),
          entry("suppress"),
          entry("createOrJoin"),
          entry("create"),
          entry("supported"),
          entry("notAllowed"),
        ],
      ];

      const results = await top.getNestedTransactionIds(0, tiers);

      // 2 tiers: the top call, and its 6 direct children.
      expect(results).toHaveLength(2);

      const topIds = results[0]!;
      expect(topIds).toHaveLength(1);
      expect(topIds[0]).toBeUndefined();

      const subIds = results[1]!;
      expect(subIds).toHaveLength(6);
      expect(subIds[0]).toBeUndefined(); // no attribute
      expect(subIds[1]).toBeUndefined(); // suppress
      expect(subIds[2]).toBeDefined(); // createOrJoin
      expect(subIds[3]).toBeDefined(); // create
      expect(subIds[2]).not.toBe(subIds[3]); // distinct fresh transactions
      expect(subIds[4]).toBeUndefined(); // supported
      expect(subIds[5]).toBeUndefined(); // notAllowed
    } finally {
      await cluster.dispose();
    }
  },
);

// Left gapped: upstream asserts a nested call through a method with *no*
// `[Transaction]` attribute at all sees no ambient transaction, even when
// called from inside an active one. This framework's `InvokeMethodOptions`
// (@tsva/core/invoke-options) documents the opposite as deliberate boundary
// behaviour: "Absent means the method is non-transactional: an ambient
// transaction still flows through to nested calls, but this method's state
// does not participate" — so `getTransactionId()` inside a `"none"`-option
// method called from within a transaction reports the ambient transaction's
// id, not `undefined`. Verified directly: every other tier in this same test
// (suppress/createOrJoin/create/join/supported) behaves exactly as upstream
// asserts; only the "no attribute" tier's expectation diverges, on
// `InvokeMethodOptions.transaction`/`GrainFactory.resolveTransaction`
// boundary semantics this task must not change. Changing that behaviour is
// out of scope here (see CLAUDE.md task boundaries); tracked as an
// attribution-boundary divergence rather than an introspection gap.
orleansTest.gap(
  "GAP-TRANSACTION-CONTEXT-INTROSPECTION",
  "Orleans.Transactions.Tests.TransactionAttributionTest.AllSupportedAttributesFromWithinTransactionTest",
);

orleansTest("Orleans.Transactions.Tests.TransactionAttributionTest.CreateOrJoinTest", async () => {
  const cluster = await buildCluster();
  try {
    const top = topGrain(cluster, "createOrJoin");
    const tiers: AttributionTierEntry[][] = [[entry("createOrJoin"), entry("createOrJoin")]];

    const results = await top.getNestedTransactionIds(0, tiers);

    expect(results).toHaveLength(2);
    const topIds = results[0]!;
    expect(topIds).toHaveLength(1);
    expect(topIds[0]).toBeDefined();

    const subIds = results[1]!;
    expect(subIds).toHaveLength(2);
    expect(subIds[0]).toBe(topIds[0]);
    expect(subIds[1]).toBe(topIds[0]);
  } finally {
    await cluster.dispose();
  }
});

orleansTest("Orleans.Transactions.Tests.TransactionAttributionTest.CreateTest", async () => {
  const cluster = await buildCluster();
  try {
    const top = topGrain(cluster, "create");
    const tiers: AttributionTierEntry[][] = [[entry("create"), entry("create")]];

    const results = await top.getNestedTransactionIds(0, tiers);

    expect(results).toHaveLength(2);
    const topIds = results[0]!;
    expect(topIds).toHaveLength(1);
    expect(topIds[0]).toBeDefined();

    const subIds = results[1]!;
    expect(subIds).toHaveLength(2);
    expect(subIds[0]).not.toBe(topIds[0]);
    expect(subIds[1]).not.toBe(topIds[0]);
    expect(subIds[0]).not.toBe(subIds[1]);
  } finally {
    await cluster.dispose();
  }
});

orleansTest("Orleans.Transactions.Tests.TransactionAttributionTest.SupportedTest", async () => {
  const cluster = await buildCluster();
  try {
    const top = topGrain(cluster, "create");
    const tiers: AttributionTierEntry[][] = [[entry("supported")], [entry("createOrJoin")]];

    const results = await top.getNestedTransactionIds(0, tiers);

    expect(results).toHaveLength(3);
    const topIds = results[0]!;
    expect(topIds).toHaveLength(1);
    expect(topIds[0]).toBeDefined();

    const tier1Ids = results[1]!;
    expect(tier1Ids).toHaveLength(1);
    expect(tier1Ids[0]).toBe(topIds[0]);

    const tier2Ids = results[2]!;
    expect(tier2Ids).toHaveLength(1);
    expect(tier2Ids[0]).toBe(topIds[0]);
  } finally {
    await cluster.dispose();
  }
});

orleansTest("Orleans.Transactions.Tests.TransactionAttributionTest.JoinFailTest", async () => {
  const cluster = await buildCluster();
  try {
    const fail = topGrain(cluster, "none");
    const tiers: AttributionTierEntry[][] = [[entry("join")]];

    await expect(fail.getNestedTransactionIds(0, tiers)).rejects.toThrow();
  } finally {
    await cluster.dispose();
  }
});

orleansTest(
  "Orleans.Transactions.Tests.TransactionAttributionTest.NotAllowedFailTest",
  async () => {
    const cluster = await buildCluster();
    try {
      const fail = topGrain(cluster, "create");
      const tiers: AttributionTierEntry[][] = [[entry("notAllowed")]];

      await expect(fail.getNestedTransactionIds(0, tiers)).rejects.toThrow();
    } finally {
      await cluster.dispose();
    }
  },
);
