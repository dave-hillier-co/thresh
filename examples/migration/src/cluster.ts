import { SiloAddress } from "@tsva/core/silo-address";
import { FakeTimeProvider } from "@tsva/core/test-support/fake-time-provider";
import { InProcessNetwork } from "@tsva/messaging/in-process-transport";
import { MemoryGrainStorage } from "@tsva/persistence/memory-grain-storage";
import { createSilo } from "@tsva/hosting/silo-builder";
import type { SiloHost } from "@tsva/hosting/silo-host";
import { CartGrain } from "@tsva/example-migration/cart-grain";
import { ICart } from "@tsva/example-migration/interfaces";

const CLUSTER = "carts";

export function siloAddress(n: number): SiloAddress {
  return new SiloAddress(`silo-${n}`, `uid-${n}`, `silo-${n}:11111`);
}

export interface MigrationCluster {
  readonly silos: SiloHost[];
  readonly addresses: SiloAddress[];
  /** Drives idle collection — advance it to trigger a scheduled migration. */
  readonly time: FakeTimeProvider;
  start(): Promise<void>;
  stop(): Promise<void>;
}

/**
 * Two in-process silos sharing one membership view, one storage provider, and one
 * fake clock. `random: () => 0` makes placement deterministic (first ring
 * candidate), and idle collection runs on the fake clock so a test/demo can
 * trigger a scheduled migration precisely by advancing time.
 */
export function buildMigrationCluster(): MigrationCluster {
  const network = new InProcessNetwork();
  const storage = new MemoryGrainStorage();
  const time = new FakeTimeProvider();
  const addresses = [siloAddress(0), siloAddress(1)];

  const silos = addresses.map((local) =>
    createSilo({
      clusterId: CLUSTER,
      local,
      time,
      collectionAgeSeconds: 30,
      collectionIntervalSeconds: 10,
      random: () => 0,
    })
      .useStaticMembership(addresses)
      .useInProcessTransport(network)
      .useMemoryStorage(storage)
      .registerGrain(CartGrain, { interfaces: [ICart] })
      .build(),
  );

  return {
    silos,
    addresses,
    time,
    start: async () => {
      for (const silo of silos) await silo.start();
    },
    stop: async () => {
      for (const silo of silos) await silo.stop();
    },
  };
}
