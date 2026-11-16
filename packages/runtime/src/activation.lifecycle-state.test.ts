// L2/L3: the activation lifecycle is one explicit state machine, lifecycle
// operations run on a queue of their own that never interleaves with request
// turns (Orleans `ActivationData`'s pending operations and
// `ProcessOperationsAsync`), a deactivation request is a state transition the
// moment it is made (Orleans `Deactivate` sets `Deactivating` at once), and
// one admission gate decides every kind of work.
import { describe, expect, it } from "vitest";
import { grain, reentrant } from "@thresh/core/decorators";
import { InconsistentStateError } from "@thresh/core/errors";
import { Grain } from "@thresh/core/grain";
import { GrainId } from "@thresh/core/grain-id";
import { defineGrainInterface } from "@thresh/core/grain-interface";
import { castGrainReference } from "@thresh/core/grain-reference";
import type {
  DehydrationContext,
  RehydrationContext,
} from "@thresh/core/grain-migration-participant";
import type { GrainKey } from "@thresh/core/key-kinds";
import type { InvocationRequest } from "@thresh/core/request";
import { SiloAddress } from "@thresh/core/silo-address";
import {
  ACTIVATION_TRANSITIONS,
  ActivationData,
  assertActivationTransition,
  type ActivationOptions,
  type ActivationState,
} from "@thresh/runtime/activation";
import { IGrainManagementExtension } from "@thresh/runtime/grain-management-extension";
import { Silo } from "@thresh/runtime/silo";
import { FakeTimeProvider } from "@thresh/runtime/test-support/fake-time-provider";

function deferred<T = void>() {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>((res) => {
    resolve = res;
  });
  return { promise, resolve };
}

const flush = (): Promise<void> => new Promise((r) => setTimeout(r, 0));

// --- (a) a deactivation request stops serving at once -----------------------

interface IBusy extends GrainKey<string> {
  hold(): Promise<string>;
  tag(): Promise<string>;
  write(): Promise<void>;
}
const IBusy = defineGrainInterface<IBusy>("IBusy.lifecycle-state");
const IBusyReentrant = defineGrainInterface<IBusy>("IBusyReentrant.lifecycle-state");

let busyGate = deferred();

@grain()
class BusyGrain extends Grain implements IBusy {
  private readonly instanceTag = Math.random().toString(36).slice(2);
  async hold(): Promise<string> {
    await busyGate.promise;
    return this.instanceTag;
  }
  async tag(): Promise<string> {
    return this.instanceTag;
  }
  async write(): Promise<void> {
    throw new InconsistentStateError("etag mismatch", "e1", "e2");
  }
}

@grain()
@reentrant()
class BusyReentrantGrain extends BusyGrain {}

function busySilo(): Silo {
  const silo = new Silo({
    time: new FakeTimeProvider(),
    defaultCollectionAgeSeconds: 100_000,
    collectionIntervalSeconds: 100_000,
  });
  silo.registerGrain(BusyGrain, { interfaces: [IBusy] });
  silo.registerGrain(BusyReentrantGrain, { interfaces: [IBusyReentrant] });
  silo.start();
  return silo;
}

describe("a deactivation request is a state transition, not a polled flag (L2)", () => {
  it("deactivateOnIdle under overlapping traffic: calls issued afterwards reach a fresh activation", async () => {
    busyGate = deferred();
    const ref = busySilo().getGrain(IBusy, "a");
    const before = await ref.tag();

    const inFlight = ref.hold(); // keeps the activation busy throughout
    const queuedBefore = ref.tag(); // already waiting when the request lands
    await flush();
    await castGrainReference(ref, IGrainManagementExtension).deactivateOnIdle();
    const issuedAfter = ref.tag();
    await flush();
    busyGate.resolve();

    expect(await inFlight).toBe(before);
    expect(await issuedAfter).not.toBe(before);
    // A call that was waiting but never ran is rerouted too, never served stale.
    expect(await queuedBefore).not.toBe(before);
  });

  it("an escaped InconsistentStateError on a busy reentrant grain: the next call reaches a fresh activation", async () => {
    busyGate = deferred();
    const ref = busySilo().getGrain(IBusyReentrant, "b");
    const before = await ref.tag();

    const inFlight = ref.hold();
    await flush();
    await expect(ref.write()).rejects.toBeInstanceOf(InconsistentStateError);
    const issuedAfter = ref.tag();
    await flush();
    busyGate.resolve();

    expect(await inFlight).toBe(before);
    expect(await issuedAfter).not.toBe(before);
  });
});

// --- (b) lifecycle operations never interleave with request turns -----------

const id = new GrainId("LifecycleState", "a");
const silo = new SiloAddress("silo-2", "uid-2", "silo-2:1");

class ProbeGrain extends Grain {
  inFlight = 0;
  observed: string[] = [];
  ticks = 0;
  gate = deferred();
  /** The activation hosting this instance (no `GrainRuntime` in these unit tests). */
  activation!: ActivationData;

  async hold(): Promise<void> {
    this.inFlight++;
    await this.gate.promise;
    this.inFlight--;
  }
  async startTimer(dueMs: number): Promise<void> {
    this.activation.registerTimer(
      async () => {
        this.ticks++;
      },
      { ms: dueMs },
      { ms: dueMs },
    );
  }
  async ping(): Promise<string> {
    return "pong";
  }
  override async onDeactivate(): Promise<void> {
    this.observed.push(`onDeactivate:inFlight=${this.inFlight}`);
  }
  onDehydrate(ctx: DehydrationContext): void {
    this.observed.push(`onDehydrate:inFlight=${this.inFlight}`);
    ctx.set("n", 1);
  }
  onRehydrate(_ctx: RehydrationContext): void {}
}

function makeActivation(
  reentrant: boolean,
  options: ActivationOptions = {},
): { activation: ActivationData; grain: ProbeGrain; time: FakeTimeProvider } {
  const time = new FakeTimeProvider();
  const activation = new ActivationData(id, time, 30_000, reentrant, "act-1", options);
  const grain = new ProbeGrain();
  grain.setContext(activation);
  grain.activation = activation;
  activation.instance = grain;
  activation.beginActivate("incoming-call");
  return { activation, grain, time };
}

const call = (method: string, args: unknown[] = []): InvocationRequest => ({
  target: id,
  interfaceId: 0,
  method,
  args,
  options: {},
});

describe("lifecycle operations run only when no request turn is running (L2)", () => {
  it("on a reentrant grain neither onDeactivate nor the dehydrate snapshot runs while a request is in flight", async () => {
    const { activation, grain } = makeActivation(true, {
      deactivationSteps: {
        migrate: async (a) => {
          await a.dehydrate();
          return true;
        },
      },
    });
    await flush();
    const first = activation.invoke(call("hold"));
    const second = activation.invoke(call("hold"));
    await flush();
    expect(grain.inFlight).toBe(2);

    const deactivating = activation.deactivate(
      { code: "migrating", description: "test" },
      { migrateTo: silo },
    );
    await flush();
    expect(grain.observed).toEqual([]);

    grain.gate.resolve();
    await first;
    await second;
    await expect(deactivating).resolves.toEqual({ migrated: true });
    expect(grain.observed).toEqual(["onDeactivate:inFlight=0", "onDehydrate:inFlight=0"]);
  });
});

// --- (c) timer ticks pass the same gate --------------------------------------

describe("timer ticks are admitted only to a valid activation (L3)", () => {
  it("drops a tick that was already queued when deactivation began", async () => {
    const { activation, grain, time } = makeActivation(false);
    await activation.invoke(call("startTimer", [10]));
    const running = activation.invoke(call("hold"));
    await flush();
    time.advance(10); // the tick queues behind the running call
    await flush();

    const deactivating = activation.deactivate({ code: "idle", description: "test" });
    grain.gate.resolve();
    await running;
    await deactivating;
    expect(grain.ticks).toBe(0);
  });

  it("runs no grain code for a tick that comes due after deactivation began", async () => {
    const { activation, grain, time } = makeActivation(true);
    await activation.invoke(call("startTimer", [10]));
    const running = activation.invoke(call("hold"));
    await flush();

    activation.requestDeactivation();
    time.advance(10); // due while the running call keeps the pipeline waiting
    await flush();
    time.advance(10);
    await flush();
    expect(grain.ticks).toBe(0);

    grain.gate.resolve();
    await running;
  });
});

// --- (d) stream deliveries pass the same gate --------------------------------

describe("a stream delivery reaching a deactivating activation is rerouted, not failed (L3)", () => {
  it("holds the delivery until the activation is gone, then hands back a reroute rejection", async () => {
    const { activation, grain } = makeActivation(false);
    await flush();
    const running = activation.invoke(call("hold"));
    await flush();
    void activation.deactivate({ code: "idle", description: "test" });

    let delivered = false;
    const delivery = activation.runStreamTurn(async () => {
      delivered = true;
    });
    let settled = false;
    const outcome = delivery.then(
      () => undefined,
      (error: unknown) => error,
    );
    void outcome.then(() => (settled = true));
    await flush();
    expect(settled).toBe(false); // held, not failed

    grain.gate.resolve();
    await running;
    const error = await outcome;
    expect(delivered).toBe(false);
    expect(activation.isRerouteRejection(error)).toBe(true);
  });
});

// --- (e) the transition table -------------------------------------------------

describe("the activation state machine (L3)", () => {
  const states = Object.keys(ACTIVATION_TRANSITIONS) as ActivationState[];

  it("allows exactly the transitions its table lists and throws on any other", () => {
    for (const from of states) {
      for (const to of states) {
        const legal = ACTIVATION_TRANSITIONS[from].includes(to);
        if (legal) expect(() => assertActivationTransition(from, to)).not.toThrow();
        else expect(() => assertActivationTransition(from, to)).toThrow(/illegal/i);
      }
    }
  });

  it("reaches every legal transition through the activation's own lifecycle", async () => {
    const seen = new Set<string>();
    const observe = (activation: ActivationData): void => {
      activation.onStateChanged = (from, to) => seen.add(`${from}->${to}`);
    };
    const build = (
      grain: ProbeGrain,
      options: ActivationOptions = {},
      reentrant = false,
    ): { activation: ActivationData; time: FakeTimeProvider } => {
      const time = new FakeTimeProvider();
      const activation = new ActivationData(id, time, 30_000, reentrant, undefined, options);
      grain.setContext(activation);
      grain.activation = activation;
      activation.instance = grain;
      observe(activation);
      return { activation, time };
    };

    // creating -> activating -> valid -> deactivating -> invalid
    {
      const { activation } = build(new ProbeGrain());
      activation.beginActivate("incoming-call");
      await activation.invoke(call("ping"));
      await activation.deactivate({ code: "idle", description: "test" });
    }
    // creating -> deactivating (stopped before it ever activated)
    {
      const { activation } = build(new ProbeGrain());
      await activation.deactivate({ code: "shutting-down", description: "test" });
    }
    // activating -> failed -> invalid
    {
      class Failing extends ProbeGrain {
        override async onActivate(): Promise<void> {
          throw new Error("boom");
        }
      }
      const { activation } = build(new Failing());
      activation.beginActivate("incoming-call");
      await expect(activation.invoke(call("ping"))).rejects.toThrow("boom");
    }
    // activating -> deactivating (asked to go away from onActivate)
    {
      class Leaving extends ProbeGrain {
        override async onActivate(): Promise<void> {
          this.activation.requestDeactivation();
        }
      }
      const { activation } = build(new Leaving());
      activation.beginActivate("incoming-call");
      await expect(activation.invoke(call("ping"))).rejects.toThrow("activation unavailable");
    }
    // valid -> invalid (abandoned as stuck)
    {
      const grain = new ProbeGrain();
      const { activation, time } = build(grain, { maxRequestProcessingTimeMs: 1000 });
      activation.beginActivate("incoming-call");
      await flush();
      void activation.invoke(call("hold"));
      activation.invoke(call("ping")).catch(() => undefined);
      await flush();
      time.advance(1000);
      grain.gate.resolve();
    }

    const legal = states.flatMap((from) =>
      ACTIVATION_TRANSITIONS[from].map((to) => `${from}->${to}`),
    );
    expect([...seen].sort()).toEqual([...legal].sort());
  });
});
