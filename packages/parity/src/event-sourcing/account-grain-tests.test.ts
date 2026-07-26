// Ported from dotnet/orleans test/Orleans.EventSourcing.Tests/EventSourcingTests/AccountGrainTests.cs @ v10.1.0 (MIT).
import { afterAll, beforeAll, describe, expect } from "vitest";
import { orleansTest } from "@thresh/testing/orleans-test";
import { TestCluster } from "@thresh/testing/test-cluster";
import {
  AccountGrainPersistStateOnly,
  IAccountGrain,
} from "@thresh/parity/grains/impl/account-grain";
import { Guid } from "@thresh/core/guid";

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

  // Upstream skipped: https://github.com/dotnet/orleans/issues/5605. This
  // framework now has `JournaledGrain` (see `@thresh/core/journaled-grain`), but
  // there is no reason to port a real `AccountGrain` event-log variant just to
  // exercise a test upstream itself never runs.
  orleansTest.excluded(
    "skipped upstream (dotnet/orleans#5605)",
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
