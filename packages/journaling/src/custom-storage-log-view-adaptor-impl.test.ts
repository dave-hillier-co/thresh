import { describe, expect, it } from "vitest";

import { CustomStorageLogViewAdaptorImpl } from "./custom-storage-log-view-adaptor-impl";

type State = { readonly total: number };
type Event = { readonly add: number };

const initial = (): State => ({ total: 0 });
const transition = (s: State, e: Event): State => ({ total: s.total + e.add });

/**
 * A host implementing the grain side of the custom-storage contract, backed by an array that
 * stands in for whatever the real grain persists (Spiceport's DatastoreGrain writes per-version
 * log rows through a keyed grain-storage provider).
 */
class FakeHost {
  log: Event[] = [];
  reads = 0;
  applyCalls: { updates: readonly Event[]; expectedVersion: number }[] = [];
  failNextApply = 0;
  throwNextApply = 0;
  cleared = 0;

  readStateFromStorage(): Promise<{ version: number; state: State }> {
    this.reads++;
    return Promise.resolve({
      version: this.log.length,
      state: this.log.reduce(transition, initial()),
    });
  }

  applyUpdatesToStorage(updates: readonly Event[], expectedVersion: number): Promise<boolean> {
    this.applyCalls.push({ updates, expectedVersion });
    if (this.throwNextApply > 0) {
      this.throwNextApply--;
      return Promise.reject(new Error("storage unavailable"));
    }
    if (this.failNextApply > 0) {
      this.failNextApply--;
      return Promise.resolve(false);
    }
    if (expectedVersion !== this.log.length) return Promise.resolve(false);
    this.log.push(...updates);
    return Promise.resolve(true);
  }

  clearStoredState(): Promise<void> {
    this.cleared++;
    this.log = [];
    return Promise.resolve();
  }
}

async function adaptorOn(host: FakeHost, opts?: { maxAttempts?: number }) {
  const adaptor = new CustomStorageLogViewAdaptorImpl<State, Event>(
    initial,
    transition,
    host,
    opts,
  );
  await adaptor.read();
  return adaptor;
}

describe("CustomStorageLogViewAdaptorImpl", () => {
  it("reads the state and version from the host's storage on activation", async () => {
    const host = new FakeHost();
    host.log = [{ add: 3 }, { add: 4 }];
    const adaptor = await adaptorOn(host);

    expect(adaptor.confirmedVersion).toBe(2);
    expect(adaptor.confirmedView).toEqual({ total: 7 });
    expect(adaptor.tentativeView).toEqual({ total: 7 });
  });

  it("shows a raised event tentatively but not as confirmed", async () => {
    const adaptor = await adaptorOn(new FakeHost());
    adaptor.submit({ add: 5 });

    expect(adaptor.tentativeView).toEqual({ total: 5 });
    expect(adaptor.confirmedView).toEqual({ total: 0 });
    expect(adaptor.confirmedVersion).toBe(0);
    expect(adaptor.pendingCount).toBe(1);
  });

  it("applies pending events as one batch at the expected version", async () => {
    const host = new FakeHost();
    const adaptor = await adaptorOn(host);
    adaptor.submitRange([{ add: 1 }, { add: 2 }]);
    await adaptor.confirmSubmittedEntries();

    // One CAS for the batch, not one per event: the C# ApplyUpdatesToStorage takes a list.
    expect(host.applyCalls).toHaveLength(1);
    expect(host.applyCalls[0]).toEqual({ updates: [{ add: 1 }, { add: 2 }], expectedVersion: 0 });
    expect(adaptor.confirmedVersion).toBe(2);
    expect(adaptor.confirmedView).toEqual({ total: 3 });
    expect(adaptor.pendingCount).toBe(0);
  });

  it("advances the version by the number of deltas, as the contract requires", async () => {
    const host = new FakeHost();
    const adaptor = await adaptorOn(host);
    adaptor.submitRange([{ add: 1 }, { add: 1 }, { add: 1 }]);
    await adaptor.confirmSubmittedEntries();

    expect(adaptor.confirmedVersion).toBe(3);
    expect(host.log).toHaveLength(3);
  });

  it("re-reads and retries at the new version when the CAS is rejected", async () => {
    const host = new FakeHost();
    const adaptor = await adaptorOn(host);
    // Someone else appended behind our back, so the first CAS at version 0 is rejected.
    host.failNextApply = 1;
    host.log = [{ add: 100 }];

    adaptor.submit({ add: 1 });
    await adaptor.confirmSubmittedEntries();

    expect(host.applyCalls.map((c) => c.expectedVersion)).toEqual([0, 1]);
    expect(adaptor.confirmedVersion).toBe(2);
    // The event is applied on top of what was read back, never lost.
    expect(adaptor.confirmedView).toEqual({ total: 101 });
  });

  it("re-reads and retries when the host throws rather than returning false", async () => {
    const host = new FakeHost();
    const adaptor = await adaptorOn(host);
    host.throwNextApply = 1;

    adaptor.submit({ add: 1 });
    await adaptor.confirmSubmittedEntries();

    expect(adaptor.confirmedVersion).toBe(1);
    expect(adaptor.confirmedView).toEqual({ total: 1 });
  });

  it("gives up with an InconsistentStateError after the attempt budget", async () => {
    const host = new FakeHost();
    const adaptor = await adaptorOn(host, { maxAttempts: 3 });
    host.failNextApply = 99;

    adaptor.submit({ add: 1 });
    await expect(adaptor.confirmSubmittedEntries()).rejects.toThrow(/CAS|version|conflict/i);
    expect(host.applyCalls).toHaveLength(3);
  });

  it("keeps the events pending when it gives up, so a later confirm can retry", async () => {
    const host = new FakeHost();
    const adaptor = await adaptorOn(host, { maxAttempts: 1 });
    host.failNextApply = 1;

    adaptor.submit({ add: 1 });
    await expect(adaptor.confirmSubmittedEntries()).rejects.toThrow();
    expect(adaptor.pendingCount).toBe(1);

    await adaptor.confirmSubmittedEntries();
    expect(adaptor.confirmedVersion).toBe(1);
  });

  it("joins concurrent confirms into one in-flight loop", async () => {
    const host = new FakeHost();
    const adaptor = await adaptorOn(host);
    adaptor.submit({ add: 1 });
    adaptor.submit({ add: 2 });

    await Promise.all([adaptor.confirmSubmittedEntries(), adaptor.confirmSubmittedEntries()]);

    // Two overlapping loops would each swap-and-apply, and one would lose the CAS.
    expect(host.applyCalls).toHaveLength(1);
    expect(adaptor.confirmedVersion).toBe(2);
  });

  it("clears stored state and resyncs from storage", async () => {
    const host = new FakeHost();
    host.log = [{ add: 9 }];
    const adaptor = await adaptorOn(host);

    await adaptor.clearLog();

    expect(host.cleared).toBe(1);
    expect(adaptor.confirmedVersion).toBe(0);
    expect(adaptor.confirmedView).toEqual({ total: 0 });
  });

  // Orleans guards the post-write view update separately (`transitionssuccessful`): the write
  // already succeeded and is durable, so a throwing transition must not be reported as a failed
  // write -- the adaptor re-reads instead, because storage is the truth.
  it("resyncs from storage when applying the confirmed events to the view throws", async () => {
    const host = new FakeHost();
    let explode = false;
    const adaptor = new CustomStorageLogViewAdaptorImpl<State, Event>(
      initial,
      (s, e) => {
        if (explode) throw new Error("bad transition");
        return transition(s, e);
      },
      host,
    );
    await adaptor.read();

    adaptor.submit({ add: 4 });
    explode = true;
    await adaptor.confirmSubmittedEntries();
    explode = false;

    // The write landed, so the version and view reflect storage rather than being left
    // half-applied or rolled back.
    expect(host.log).toEqual([{ add: 4 }]);
    expect(adaptor.confirmedVersion).toBe(1);
    expect(adaptor.confirmedView).toEqual({ total: 4 });
    expect(adaptor.pendingCount).toBe(0);
  });

  it("does not support retrieveLogSegment - storage owns the log, not the adaptor", async () => {
    const adaptor = await adaptorOn(new FakeHost());

    // Orleans' PrimaryBasedLogViewAdaptor.RetrieveLogSegment throws NotSupportedException, and
    // the custom-storage adaptor does not override it.
    expect(() => adaptor.retrieveLogSegment(0, 0)).toThrow(/not supported/i);
  });
});
