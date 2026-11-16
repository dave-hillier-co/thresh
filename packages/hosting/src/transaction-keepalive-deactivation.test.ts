import { describe, expect, it, vi } from "vitest";
import { grain, transactionalState } from "@thresh/core/decorators";
import { Grain } from "@thresh/core/grain";
import { GrainId } from "@thresh/core/grain-id";
import { defineGrainInterface } from "@thresh/core/grain-interface";
import type { GrainType } from "@thresh/core/grain-type";
import type { GrainKey } from "@thresh/core/key-kinds";
import { SiloAddress } from "@thresh/core/silo-address";
import type { TransactionalState } from "@thresh/core/transactional-state";
import { InProcessNetwork } from "@thresh/messaging/in-process-transport";
import { MemoryTransactionalStorage } from "@thresh/transactions/memory-transactional-storage";
import { FakeTimeProvider } from "@thresh/runtime/test-support/fake-time-provider";
import { createSilo } from "@thresh/hosting/silo-builder";

interface Balance {
  cents: number;
}

interface Account extends GrainKey<string> {
  ping(): Promise<string>;
}

const Account = defineGrainInterface<Account>("KeepaliveAccount.iface");

// A remote, never-registered "TM" grain type — every call routed to it fails,
// standing in for a TM whose silo crashed and never comes back.
const unreachableTm = {
  grainId: new GrainId("GhostLedger" as GrainType, "tm"),
  stateName: "ledger",
};

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
  it("stops the prepared record's ping timer once the activation deactivates", async () => {
    const time = new FakeTimeProvider();
    const storage = new MemoryTransactionalStorage();
    await seedInDoubt(storage, "a");

    const setTimerSpy = vi.spyOn(time, "setTimer");
    const clearTimerSpy = vi.spyOn(time, "clearTimer");

    const silo = buildSilo(storage, time);
    await silo.start();
    try {
      // `start()` itself arms a couple of periodic silo-wide timers (e.g. the
      // cross-silo load publisher) that also use a 1s period, so only look for
      // the ping timer among calls made from here on.
      const callsBeforeActivation = setTimerSpy.mock.calls.length;

      // Activating queues the prepared record again and pings its TM, which
      // is unreachable: the record stays in doubt and the next ping is
      // scheduled against the silo's clock at Orleans' 60 s
      // RemoteTransactionPingFrequency.
      await silo.getGrain(Account, "a").ping();
      for (let i = 0; i < 20; i++) await new Promise((r) => setTimeout(r, 0));

      const pingTimerIndex = setTimerSpy.mock.calls.findIndex(
        ([, delay], index) => index >= callsBeforeActivation && delay === 60_000,
      );
      expect(pingTimerIndex).toBeGreaterThanOrEqual(0);

      // An unresolved prepared record delays deactivation by at most five
      // seconds (T6, Orleans `ActivationLifetime.OnStopAsync`): shutting the
      // silo down waits out the bound on the silo's clock, then deactivates
      // the activation — and the hosting wiring under test must stop its
      // facet's workers there.
      let stopped = false;
      const stopping = silo.stop().then(() => (stopped = true));
      for (let i = 0; i < 200 && !stopped; i++) {
        time.advance(100);
        await new Promise((r) => setTimeout(r, 0));
      }
      await stopping;
      expect(silo.isActive(accountId("a"))).toBe(false);

      // The ping timer can never fire again, however far the clock
      // moves: nothing re-arms a timer from here on.
      const armedAfterStop = setTimerSpy.mock.calls.length;
      time.advance(600_000);
      await new Promise((r) => setTimeout(r, 0));
      expect(setTimerSpy.mock.calls.length).toBe(armedAfterStop);
      expect(clearTimerSpy).toHaveBeenCalled();
    } finally {
      if (silo.isActive(accountId("a"))) await silo.stop();
    }
  });
});
