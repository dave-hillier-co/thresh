import { describe, expect, it } from "vitest";
import { grain } from "@thresh/core/decorators";
import { Grain } from "@thresh/core/grain";
import { GrainId } from "@thresh/core/grain-id";
import { getGrainMetadata } from "@thresh/core/grain-metadata";
import { SiloAddress } from "@thresh/core/silo-address";
import { registerTransactionalField } from "@thresh/core/transactional-state-metadata";
import type { ActivationData } from "@thresh/runtime/activation";
import { Catalog, type RegisteredGrain } from "@thresh/runtime/catalog";
import { GrainFactory } from "@thresh/runtime/grain-factory";
import { FakeTimeProvider } from "@thresh/runtime/test-support/fake-time-provider";

// T6: a transactional resource holding a lock or an undecided prepared record
// DELAYS a deactivation by at most 5 seconds, on every path — Orleans
// `ActivationLifetime.OnStopAsync` — rather than exempting the activation from
// collection and migration for as long as the block lasts.

const BOUND_MS = 5_000;

/** Stands in for a transactional-state facet: only `blocksDeactivation` matters here. */
interface Blocker {
  blocksDeactivation: boolean;
}

@grain()
class BlockedGrain extends Grain {
  ledger: Blocker = { blocksDeactivation: true };
  constructor() {
    super();
    registerTransactionalField(this, {
      fieldName: "ledger",
      stateName: "ledger",
      initial: () => undefined,
    });
  }
  async ping(): Promise<string> {
    return "pong";
  }
}

const metadata = getGrainMetadata(BlockedGrain)!;
const target = new SiloAddress("silo-9", "uid-9", "silo-9:1");
const flush = (): Promise<void> => new Promise((r) => setTimeout(r, 0));

interface Harness {
  time: FakeTimeProvider;
  catalog: Catalog;
  activation: ActivationData;
  id: GrainId;
  blocker: Blocker;
  migrations: SiloAddress[];
}

async function harness(key: string): Promise<Harness> {
  const time = new FakeTimeProvider();
  const migrations: SiloAddress[] = [];
  const catalog = new Catalog({
    grainTypes: new Map<string, RegisteredGrain>([
      [metadata.grainType, { ctor: BlockedGrain, metadata }],
    ]),
    factory: new GrainFactory(() => metadata.grainType, time),
    time,
    defaultCollectionAgeSeconds: 60,
    // The hand-off to another silo is the network boundary.
    migrate: async (activation, to) => {
      await activation.dehydrate(to);
      if (to !== undefined) migrations.push(to);
      return true;
    },
  });
  const id = new GrainId(metadata.grainType, key);
  const activation = await catalog.getOrCreate(id);
  await flush();
  const blocker = (activation.instance as BlockedGrain).ledger;
  return { time, catalog, activation, id, blocker, migrations };
}

type Path = (h: Harness) => Promise<unknown>;

const paths: Array<[string, Path]> = [
  [
    "idle sweep",
    (h) => {
      h.time.advance(61_000);
      return h.catalog.collectIdle();
    },
  ],
  ["forced collection", (h) => h.catalog.collectIdle(0)],
  [
    "migration",
    (h) =>
      h.activation.deactivate(
        { code: "migrating", description: "rebalanced" },
        { migrateTo: target },
      ),
  ],
  [
    "deactivateOnIdle",
    (h) => {
      // The request starts the deactivation itself; joining it waits for the end.
      h.activation.requestDeactivation();
      return h.activation.deactivate({ code: "application-requested", description: "joined" });
    },
  ],
];

/** Advance the fake clock by `ms` in poll-sized steps, letting promises settle between. */
async function advance(time: FakeTimeProvider, ms: number): Promise<void> {
  for (let elapsed = 0; elapsed < ms; elapsed += 10) {
    time.advance(Math.min(10, ms - elapsed));
    await flush();
  }
}

describe("a transaction block delays deactivation by at most 5 seconds (T6)", () => {
  for (const [name, run] of paths) {
    it(`${name}: deactivates once the block clears`, async () => {
      const h = await harness(`clears-${name}`);
      let done = false;
      const path = run(h).then(() => (done = true));
      await advance(h.time, 1_000);
      expect(done).toBe(false);
      expect(h.activation.state).toBe("deactivating");

      h.blocker.blocksDeactivation = false;
      await advance(h.time, 20);
      await path;

      expect(h.activation.state).toBe("invalid");
      expect(h.catalog.get(h.id)?.activationId).not.toBe(h.activation.activationId);
      if (name === "migration") expect(h.migrations).toEqual([target]);
    });

    it(`${name}: deactivates after the 5-second bound when the block never clears`, async () => {
      const h = await harness(`bound-${name}`);
      let done = false;
      const path = run(h).then(() => (done = true));
      await advance(h.time, BOUND_MS - 20);
      expect(done).toBe(false);

      await advance(h.time, 40);
      await path;

      expect(h.activation.state).toBe("invalid");
      expect(h.catalog.get(h.id)?.activationId).not.toBe(h.activation.activationId);
    });
  }
});
