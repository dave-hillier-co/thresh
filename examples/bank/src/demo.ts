import { SiloAddress } from "@thresh/core/silo-address";
import { InProcessNetwork } from "@thresh/messaging/in-process-transport";
import { MemoryGrainStorage } from "@thresh/persistence/memory-grain-storage";
import { createSilo } from "@thresh/hosting/silo-builder";
import type { SiloHost } from "@thresh/hosting/silo-host";
import { AccountGrain } from "@thresh/example-bank/account-grain-functional";
import { account, type AccountState } from "@thresh/example-bank/interfaces";

export interface BankDemoResult {
  alice: AccountState;
  bob: AccountState;
  /** Alice's statement after a silo restart — proves the folded snapshot is durable. */
  aliceAfterRestart: AccountState;
}

const local = new SiloAddress("silo-0", "uid-0", "silo-0:11111");

export function buildBankSilo(storage: MemoryGrainStorage): SiloHost {
  return createSilo({ clusterId: "bank", local })
    .useStaticMembership([local])
    .useInProcessTransport(new InProcessNetwork())
    .useMemoryStorage(storage)
    .registerGrain(AccountGrain, { interfaces: [account] })
    .build();
}

/**
 * Runs the bank end-to-end: deposits and a transfer are expressed as events
 * folded into immutable account state, persisted as snapshots. Then a fresh silo
 * over the same store reloads the snapshots — only the folded state is durable,
 * the events are transient.
 */
export async function runBankDemo(): Promise<BankDemoResult> {
  const storage = new MemoryGrainStorage(); // the durable backend, shared across "restarts"

  const silo = buildBankSilo(storage);
  await silo.start();
  let alice: AccountState;
  let bob: AccountState;
  try {
    await silo.getGrain(account, "alice").deposit(10_000); // $100.00
    await silo.getGrain(account, "alice").deposit(5_000); // $50.00
    await silo.getGrain(account, "bob").deposit(2_000); // $20.00
    await silo.getGrain(account, "alice").transferTo("bob", 3_000); // $30.00 → bob
    alice = await silo.getGrain(account, "alice").statement();
    bob = await silo.getGrain(account, "bob").statement();
  } finally {
    await silo.stop(); // pod dies; only the folded snapshot is durable
  }

  const restarted = buildBankSilo(storage); // new pod, same durable store
  await restarted.start();
  try {
    const aliceAfterRestart = await restarted.getGrain(account, "alice").statement();
    return { alice, bob, aliceAfterRestart };
  } finally {
    await restarted.stop();
  }
}
