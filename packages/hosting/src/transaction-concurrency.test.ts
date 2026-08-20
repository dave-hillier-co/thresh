import { afterEach, describe, expect, it } from "vitest";
import { grain, transactionalState } from "@thresh/core/decorators";
import { Grain } from "@thresh/core/grain";
import { defineGrainInterface } from "@thresh/core/grain-interface";
import type { GrainKey } from "@thresh/core/key-kinds";
import { SiloAddress } from "@thresh/core/silo-address";
import type { TransactionalState } from "@thresh/core/transactional-state";
import { InProcessNetwork } from "@thresh/messaging/in-process-transport";
import { createSilo } from "@thresh/hosting/silo-builder";

interface Count {
  n: number;
}

interface Counter extends GrainKey<string> {
  bump(): Promise<void>;
  value(): Promise<number>;
}

interface Runner extends GrainKey<string> {
  /** Bump the counter inside a transaction, then hold the transaction open. */
  bumpAndHold(counter: string): Promise<void>;
  /** Bump the counter inside its own transaction and commit. */
  bump(counter: string): Promise<void>;
}

const Counter = defineGrainInterface<Counter>("TxConcurrencyCounter", {
  options: {
    bump: { transaction: "join" },
    value: { transaction: "createOrJoin", readOnly: true },
  },
});
const Runner = defineGrainInterface<Runner>("TxConcurrencyRunner", {
  options: { bumpAndHold: { transaction: "create" }, bump: { transaction: "create" } },
});

// A module-level gate lets the test hold the first transaction open (lock still
// held, but its turn on the counter has completed) while a second transaction
// contends for the same resource.
let gate: Promise<void> | undefined;
let releaseGate: (() => void) | undefined;

@grain()
class CounterGrain extends Grain implements Counter {
  @transactionalState("n", { initial: (): Count => ({ n: 0 }) })
  private state!: TransactionalState<Count>;

  async bump(): Promise<void> {
    await this.state.performUpdate((s) => {
      s.n += 1;
    });
  }

  async value(): Promise<number> {
    return this.state.performRead((s) => s.n);
  }
}

@grain()
class RunnerGrain extends Grain implements Runner {
  async bumpAndHold(counter: string): Promise<void> {
    // The bump turn on the counter completes here, so the counter activation is
    // free — but this transaction still holds the write lock until it commits.
    await this.getGrain(Counter, counter).bump();
    if (gate !== undefined) await gate;
  }

  async bump(counter: string): Promise<void> {
    await this.getGrain(Counter, counter).bump();
  }
}

const local = new SiloAddress("silo-0", "uid-0", "silo-0:11111");

function buildSilo() {
  return createSilo({ clusterId: "c1", local })
    .useStaticMembership([local])
    .useInProcessTransport(new InProcessNetwork())
    .registerGrain(CounterGrain, { interfaces: [Counter] })
    .registerGrain(RunnerGrain, { interfaces: [Runner] })
    .build();
}

describe("transaction concurrency (Slice 3, wait-die)", () => {
  afterEach(() => {
    gate = undefined;
    releaseGate = undefined;
  });

  it("aborts the younger of two contending transactions, then commits the older", async () => {
    const silo = buildSilo();
    await silo.start();
    try {
      gate = new Promise<void>((resolve) => {
        releaseGate = resolve;
      });

      // T1 (older — started first, so a lower causal timestamp) bumps "c" and
      // holds its write lock open at the gate.
      const t1 = silo.getGrain(Runner, "r1").bumpAndHold("c");
      await new Promise((r) => setTimeout(r, 0)); // let T1 acquire and hold the lock

      // T2 (younger) tries to bump the same counter; it conflicts with the older
      // holder and must die under wait-die.
      await expect(silo.getGrain(Runner, "r2").bump("c")).rejects.toThrow(/aborted/i);

      releaseGate!();
      await t1;

      // Only T1's bump committed; T2 aborted without applying.
      expect(await silo.getGrain(Counter, "c").value()).toBe(1);
    } finally {
      releaseGate?.();
      await silo.stop();
    }
  });

  it("lets sequential transactions both commit on the same resource", async () => {
    const silo = buildSilo();
    await silo.start();
    try {
      await silo.getGrain(Runner, "r1").bump("c");
      await silo.getGrain(Runner, "r2").bump("c");
      expect(await silo.getGrain(Counter, "c").value()).toBe(2);
    } finally {
      await silo.stop();
    }
  });
});
