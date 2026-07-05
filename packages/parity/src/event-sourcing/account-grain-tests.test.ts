// Ported from dotnet/orleans test/Orleans.EventSourcing.Tests/EventSourcingTests/AccountGrainTests.cs @ v10.1.0 (MIT).
import { afterAll, beforeAll, describe, expect } from "vitest";
import { orleansTest } from "@tsva/testing/orleans-test";
import { TestCluster } from "@tsva/testing/test-cluster";
import {
  AccountGrainPersistStateOnly,
  IAccountGrain,
} from "@tsva/parity/grains/impl/account-grain";
import { Guid } from "@tsva/core/guid";

describe("Tester.EventSourcingTests.AccountGrainTests", () => {
  let cluster: TestCluster;

  beforeAll(async () => {
    cluster = await TestCluster.start({
      initialSilos: 2,
      grains: [{ ctor: AccountGrainPersistStateOnly, interfaces: [IAccountGrain] }],
    });
  });

  afterAll(async () => {
    await cluster.dispose();
  });

  // Upstream skipped: https://github.com/dotnet/orleans/issues/5605. It also
  // exercises `TestGrains.AccountGrain`'s `LogStorage` consistency provider
  // (RaiseEvent/ConfirmEvents/RetrieveConfirmedEvents), which does not exist
  // here (GAP-EVENT-SOURCING) — excluded on both counts.
  orleansTest.excluded(
    "skipped upstream (dotnet/orleans#5605); also needs the LogStorage consistency provider (GAP-EVENT-SOURCING)",
    "Tester.EventSourcingTests.AccountGrainTests.AccountWithLog",
  );

  orleansTest("Tester.EventSourcingTests.AccountGrainTests.AccountWithoutLog", async () => {
    const account = cluster.getGrain(IAccountGrain, `Account-${Guid.newGuid().toString()}`);

    expect(await account.balance()).toBe(0);

    const initialDepositGuid = Guid.newGuid();
    await account.deposit(100, initialDepositGuid, "initial deposit");
    expect(await account.balance()).toBe(100);

    const firstWithdrawalGuid = Guid.newGuid();
    let success = await account.withdraw(70, firstWithdrawalGuid, "first withdrawal");
    expect(success).toBe(true);
    expect(await account.balance()).toBe(30);

    const secondWithdrawalGuid = Guid.newGuid();
    success = await account.withdraw(70, secondWithdrawalGuid, "second withdrawal");
    expect(success).toBe(false);
    expect(await account.balance()).toBe(30);

    await expect(account.getTransactionLog()).rejects.toThrow(/NotSupportedException/);
  });
});
