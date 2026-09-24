// Regression coverage for GH #91: a call reaching a deactivating (or, on the
// distributed path, migrating) activation must be HELD and rerouted once the
// process settles — Orleans `ProcessRequestsToInvalidActivation`/
// `RerouteAllQueuedMessages` — not failed with "activation unavailable".
// Only an activation whose activation itself failed (or one that asked to
// deactivate before ever going valid — see `activation.lifecycle.test.ts`'s
// "failed activation" suite and the parity `DeactivateOnIdleWhileActivate`
// test) is rejected outright.
import { describe, expect, it } from "vitest";
import { grain } from "@thresh/core/decorators";
import { Grain } from "@thresh/core/grain";
import { defineGrainInterface } from "@thresh/core/grain-interface";
import { castGrainReference } from "@thresh/core/grain-reference";
import type { GrainKey } from "@thresh/core/key-kinds";
import { IGrainManagementExtension } from "@thresh/runtime/grain-management-extension";
import { Silo } from "@thresh/runtime/silo";
import { FakeTimeProvider } from "@thresh/runtime/test-support/fake-time-provider";

interface ISlow extends GrainKey<string> {
  ping(): Promise<string>;
}

const ISlow = defineGrainInterface<ISlow>("ISlow.reroute");

/** A deferred promise `onDeactivate` awaits, so a test can hold an activation
 * mid-deactivation for as long as it needs to before letting it finish. */
function deferred(): { promise: Promise<void>; resolve: () => void } {
  let resolve!: () => void;
  const promise = new Promise<void>((r) => (resolve = r));
  return { promise, resolve };
}

let onDeactivateGate: Promise<void>;
let activations = 0;

@grain()
class SlowDeactivateGrain extends Grain implements ISlow {
  private readonly instanceTag = Math.random().toString(36).slice(2);

  override async onActivate(): Promise<void> {
    activations++;
  }

  override async onDeactivate(): Promise<void> {
    await onDeactivateGate;
  }

  async ping(): Promise<string> {
    return this.instanceTag;
  }
}

const flush = () => new Promise((r) => setTimeout(r, 0));

function buildSilo(): { silo: Silo; time: FakeTimeProvider } {
  const time = new FakeTimeProvider();
  const silo = new Silo({ time, defaultCollectionAgeSeconds: 30, collectionIntervalSeconds: 10 });
  silo.registerGrain(SlowDeactivateGrain, { interfaces: [ISlow] });
  silo.start();
  return { silo, time };
}

describe("calls reaching a deactivating activation are held and rerouted (GH #91)", () => {
  it("holds a call that arrives while idle collection is awaiting onDeactivate, then serves it on a fresh activation", async () => {
    activations = 0;
    const gate = deferred();
    onDeactivateGate = gate.promise;
    const { silo, time } = buildSilo();

    const ref = silo.getGrain(ISlow, "a");
    await ref.ping();
    expect(activations).toBe(1);

    // Past the collection age: the periodic sweep starts deactivating, and
    // onDeactivate is now awaiting our still-unresolved gate.
    time.advance(31_000);
    await flush();

    // A call arriving right now must NOT see "activation unavailable" — it
    // has to be held until the deactivation finishes.
    const held = ref.ping();
    let settled = false;
    void held.then(() => (settled = true));
    await flush();
    expect(settled).toBe(false); // still held — onDeactivate hasn't resolved yet

    gate.resolve();
    await expect(held).resolves.toEqual(expect.any(String));
    expect(activations).toBe(2); // reactivated fresh once the reroute landed
  });

  it("holds a call racing a pending deactivateOnIdle finalization, then serves it fresh", async () => {
    activations = 0;
    const gate = deferred();
    gate.resolve(); // onDeactivate itself is instant here; only the ASYNC finalize is in flight
    onDeactivateGate = gate.promise;
    const { silo } = buildSilo();

    const ref = silo.getGrain(ISlow, "b");
    await ref.ping();
    expect(activations).toBe(1);

    const ext = castGrainReference(ref, IGrainManagementExtension);
    await ext.deactivateOnIdle();

    // Fire two calls back-to-back: the first observes the pending
    // deactivateOnIdle and drives `finalizeStale`; the second must be held
    // behind it rather than being handed the dying activation directly.
    const [first, second] = [ref.ping(), ref.ping()];
    await expect(first).resolves.toEqual(expect.any(String));
    await expect(second).resolves.toEqual(expect.any(String));
    expect(activations).toBe(2);
  });

  it("holds a call that arrives during silo shutdown's deactivateAll, then reroutes it into the stop gate once settled", async () => {
    activations = 0;
    const gate = deferred();
    onDeactivateGate = gate.promise;
    const { silo } = buildSilo();

    const ref = silo.getGrain(ISlow, "c");
    await ref.ping();
    expect(activations).toBe(1);

    const stopping = silo.stop();
    const held = ref.ping();
    let settled = false;
    held.then(
      () => (settled = true),
      () => (settled = true),
    );
    await flush();
    expect(settled).toBe(false); // held while onDeactivate is still running

    gate.resolve();
    await stopping;
    // Rerouted once the deactivation settled — but a stopping silo refuses to
    // create activations (#108, Orleans only activates while
    // `SiloStatus.Active`), and a single silo has nowhere else to place it,
    // so the reroute surfaces as a draining rejection rather than
    // "activation unavailable" or a fresh activation `deactivateAll` missed.
    await expect(held).rejects.toMatchObject({ kind: "siloDraining" });
    expect(activations).toBe(1);
  });
});
