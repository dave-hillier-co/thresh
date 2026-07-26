import { describe, expect, it, vi } from "vitest";
import { grain, transactionalState } from "@thresh/core/decorators";
import { Grain } from "@thresh/core/grain";
import { GrainId } from "@thresh/core/grain-id";
import { defineGrainInterface } from "@thresh/core/grain-interface";
import { IManagementGrain } from "@thresh/core/management-grain";
import type { GrainType } from "@thresh/core/grain-type";
import type { GrainWithStringKey } from "@thresh/core/key-kinds";
import { SiloAddress } from "@thresh/core/silo-address";
import type { TransactionalState } from "@thresh/core/transactional-state";
import { InProcessNetwork } from "@thresh/messaging/in-process-transport";
import { MemoryTransactionalStorage } from "@thresh/transactions/memory-transactional-storage";
import { FakeTimeProvider } from "@thresh/runtime/test-support/fake-time-provider";
import { createSilo } from "@thresh/hosting/silo-builder";

interface Balance {
  cents: number;
}

interface Account extends GrainWithStringKey {
  ping(): Promise<string>;
}

const Account = defineGrainInterface<Account>("KeepaliveAccount.iface");

// A remote, never-registered "TM" grain type — every call routed to it fails,
// standing in for a TM whose silo crashed and never comes back.
const unreachableTm = { grainId: new GrainId("GhostLedger" as GrainType, "tm"), stateName: "ledger" };

@grain({ name: "KeepaliveAccount" })
class AccountGrain extends Grain implements Account {
  @transactionalState("balance", { initial: (): Balance => ({ cents: 0 }) })
  private bal!: TransactionalState<Balance>;

  // Any call activates the grain (and its facet) without needing to run a
  // transaction — activation alone is enough to trigger `load()`/recovery.
  async ping(): Promise<string> {
    return "pong";
  }
}

const local = new SiloAddress("silo-0", "uid-0", "silo-0:11111");
const accountId = (key: string) => new GrainId("KeepaliveAccount" as GrainType, key);

/** Seed an account with a prepared-but-uncommitted record whose TM is unreachable. */
async function seedInDoubt(storage: MemoryTransactionalStorage, key: string): Promise<void> {
  await storage.store(
    "balance",
    accountId(key),
    undefined,
    { timeStamp: 1, commitRecords: {} },
    [
      {
        sequenceId: 1,
        transactionId: "T-crashed",
        timeStamp: 1,
        transactionManager: unreachableTm,
        state: { cents: 250 },
      },
    ],
    undefined,
    undefined,
  );
}

function buildSilo(storage: MemoryTransactionalStorage, time: FakeTimeProvider) {
  return createSilo({ clusterId: "tx-keepalive", local, time })
    .useStaticMembership([local])
    .useInProcessTransport(new InProcessNetwork())
    .useMemoryTransactionalStorage(storage)
    .registerGrain(AccountGrain, { interfaces: [Account] })
    .build();
}

describe("transactional keepalive lifecycle on grain deactivation (#23 follow-up)", () => {
  it("stops the confirmation-worker timer once the activation deactivates", async () => {
    const time = new FakeTimeProvider();
    const storage = new MemoryTransactionalStorage();
    await seedInDoubt(storage, "a");

    const setTimerSpy = vi.spyOn(time, "setTimer");
    const clearTimerSpy = vi.spyOn(time, "clearTimer");

    const silo = buildSilo(storage, time);
    await silo.start();
    try {
      // Activating resolves the in-doubt record's TM, which is unreachable:
      // the record stays in-doubt and the confirmation worker is scheduled
      // against the silo's clock, at the default 1s initial backoff — distinct
      // from the silo's other periodic timers (collector sweep, membership
      // heartbeat, ...), which use much longer intervals.
      await silo.getGrain(Account, "a").ping();

      const confirmationCallIndex = setTimerSpy.mock.calls.findIndex(([, delay]) => delay === 1_000);
      expect(confirmationCallIndex).toBeGreaterThanOrEqual(0);
      const confirmationHandle = setTimerSpy.mock.results[confirmationCallIndex]!.value as unknown;

      // Force-deactivate the (single, always-idle) activation — the hosting
      // wiring under test must stop its facet's confirmation worker here.
      const management = silo.getGrain(IManagementGrain, 0n);
      await management.forceActivationCollection({ ms: 0 });

      // The confirmation worker's own timer handle was cleared: it can never
      // fire again, however far the clock moves.
      expect(clearTimerSpy).toHaveBeenCalledWith(confirmationHandle);
    } finally {
      await silo.stop();
    }
  });
});
