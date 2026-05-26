import { describe, expect, it } from "vitest";
import { SiloAddress } from "@tsva/core/silo-address";
import { InProcessNetwork } from "@tsva/messaging/in-process-transport";
import { createSilo } from "@tsva/hosting/silo-builder";
import type { SiloHost } from "@tsva/hosting/silo-host";
import {
  ITeller,
  ITxAccount,
  TellerGrain,
  TxAccountGrain,
} from "@tsva/example-bank/account-transactional";

const local = new SiloAddress("silo-0", "uid-0", "silo-0:11111");

function buildSilo(): SiloHost {
  return createSilo({ clusterId: "bank-tx", local })
    .useStaticMembership([local])
    .useInProcessTransport(new InProcessNetwork())
    .registerGrain(TxAccountGrain, { interfaces: [ITxAccount] })
    .registerGrain(TellerGrain, { interfaces: [ITeller] })
    .build();
}

describe("transactional bank account (Phase 7)", () => {
  it("moves money between accounts atomically in one transaction", async () => {
    const silo = buildSilo();
    await silo.start();
    try {
      const teller = silo.getGrain(ITeller, "main");
      await teller.open("alice", 10_000);
      await teller.transfer("alice", "bob", 3_000);

      expect(await silo.getGrain(ITxAccount, "alice").balance()).toBe(7_000);
      expect(await silo.getGrain(ITxAccount, "bob").balance()).toBe(3_000);
    } finally {
      await silo.stop();
    }
  });

  it("rolls back the whole transfer when the debit overdraws (no half-apply)", async () => {
    const silo = buildSilo();
    await silo.start();
    try {
      const teller = silo.getGrain(ITeller, "main");
      await teller.open("alice", 1_000);

      await expect(teller.transfer("alice", "bob", 5_000)).rejects.toThrow(/insufficient/);

      // The credit to bob was rolled back along with the failed debit.
      expect(await silo.getGrain(ITxAccount, "alice").balance()).toBe(1_000);
      expect(await silo.getGrain(ITxAccount, "bob").balance()).toBe(0);
    } finally {
      await silo.stop();
    }
  });
});
