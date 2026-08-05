import { describe, expect, it } from "vitest";
import { SiloAddress } from "@thresh/core/silo-address";
import { InProcessNetwork } from "@thresh/messaging/in-process-transport";
import { createSilo } from "@thresh/hosting/silo-builder";
import type { SiloHost } from "@thresh/hosting/silo-host";
import {
  teller,
  txAccount,
  TellerGrain,
  TxAccountGrain,
} from "@thresh/example-bank/account-transactional";

const local = new SiloAddress("silo-0", "uid-0", "silo-0:11111");

function buildSilo(): SiloHost {
  return createSilo({ clusterId: "bank-tx", local })
    .useStaticMembership([local])
    .useInProcessTransport(new InProcessNetwork())
    .registerGrain(TxAccountGrain, { interfaces: [txAccount] })
    .registerGrain(TellerGrain, { interfaces: [teller] })
    .build();
}

describe("transactional bank account (Phase 7)", () => {
  it("moves money between accounts atomically in one transaction", async () => {
    const silo = buildSilo();
    await silo.start();
    try {
      const tellerRef = silo.getGrain(teller, "main");
      await tellerRef.open("alice", 10_000);
      await tellerRef.transfer("alice", "bob", 3_000);

      expect(await silo.getGrain(txAccount, "alice").balance()).toBe(7_000);
      expect(await silo.getGrain(txAccount, "bob").balance()).toBe(3_000);
    } finally {
      await silo.stop();
    }
  });

  it("rolls back the whole transfer when the debit overdraws (no half-apply)", async () => {
    const silo = buildSilo();
    await silo.start();
    try {
      const tellerRef = silo.getGrain(teller, "main");
      await tellerRef.open("alice", 1_000);

      await expect(tellerRef.transfer("alice", "bob", 5_000)).rejects.toThrow(/insufficient/);

      // The credit to bob was rolled back along with the failed debit.
      expect(await silo.getGrain(txAccount, "alice").balance()).toBe(1_000);
      expect(await silo.getGrain(txAccount, "bob").balance()).toBe(0);
    } finally {
      await silo.stop();
    }
  });
});
