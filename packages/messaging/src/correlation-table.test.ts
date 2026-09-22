import { describe, expect, it } from "vitest";

import { CorrelationTable, type CorrelationTimer } from "./correlation-table.js";
import type { Message } from "./message.js";

const response = (correlationId: bigint): Message =>
  ({ correlationId, direction: "response" }) as Message;

/** A timer the test fires by hand, so a call's deadline is reached without waiting for one. */
class ManualTimer implements CorrelationTimer {
  private readonly armed: Array<() => void> = [];

  set(callback: () => void): unknown {
    this.armed.push(callback);
    return callback;
  }

  clear(handle: unknown): void {
    const index = this.armed.indexOf(handle as () => void);
    if (index >= 0) this.armed.splice(index, 1);
  }

  /** How many deadlines are still set — zero once every entry has been released. */
  outstanding(): number {
    return this.armed.length;
  }

  /** Fire every armed deadline, as the event loop would once their delays elapsed. */
  fire(): void {
    for (const callback of [...this.armed]) {
      this.clear(callback);
      callback();
    }
  }
}

/**
 * Run `body` with the process's unhandled-rejection listeners replaced, and report what it left
 * behind. Node reports a rejection that no handler claimed only after the microtask queue drains,
 * so the collection waits a macrotask first. Vitest owns listeners of its own; leaving them in
 * place would fail the whole file instead of this assertion.
 */
async function unhandledRejectionsFrom(body: () => void): Promise<unknown[]> {
  const existing = process.listeners("unhandledRejection");
  process.removeAllListeners("unhandledRejection");
  const unhandled: unknown[] = [];
  const capture = (reason: unknown): void => {
    unhandled.push(reason);
  };
  process.on("unhandledRejection", capture);
  try {
    body();
    await new Promise((resolve) => setTimeout(resolve, 10));
  } finally {
    process.off("unhandledRejection", capture);
    for (const listener of existing) {
      process.on("unhandledRejection", listener as (reason: unknown) => void);
    }
  }
  return unhandled;
}

describe("CorrelationTable", () => {
  it("rejects only the lost peer's calls with rejectFor, leaving others pending", async () => {
    const table = new CorrelationTable();
    const toA = table.register(1n, undefined, "silo-a");
    const toB = table.register(2n, undefined, "silo-b");
    const untagged = table.register(3n);

    table.rejectFor("silo-a", new Error("connection to silo-a was lost"));

    await expect(toA).rejects.toThrow("connection to silo-a was lost");
    // The other peer's call and the untagged call are still completable.
    table.complete(response(2n));
    table.complete(response(3n));
    await expect(toB).resolves.toMatchObject({ correlationId: 2n });
    await expect(untagged).resolves.toMatchObject({ correlationId: 3n });
  });

  it("rejectFor on a peer with nothing outstanding is a no-op", async () => {
    const table = new CorrelationTable();
    const pending = table.register(1n, undefined, "silo-a");
    table.rejectFor("silo-b", new Error("lost"));
    table.complete(response(1n));
    await expect(pending).resolves.toMatchObject({ correlationId: 1n });
  });

  it("rejectAll still fails everything outstanding", async () => {
    const table = new CorrelationTable();
    const toA = table.register(1n, undefined, "silo-a");
    const untagged = table.register(2n);
    table.rejectAll(new Error("shutting down"));
    await expect(toA).rejects.toThrow("shutting down");
    await expect(untagged).rejects.toThrow("shutting down");
  });

  // A caller can abandon a call — stop awaiting it, or never reach its `await` because the send
  // that followed `register` threw — while the entry stays armed. When its deadline fires, that
  // rejection must reach nothing process-level: under Node's default
  // `--unhandled-rejections=throw` an unobserved one terminates the process, which is how a silo
  // (or the migration example) dies long after a call it had already given up on.
  it("an abandoned call's timeout rejection never reaches the process unhandled", async () => {
    const timer = new ManualTimer();
    const table = new CorrelationTable(timer);

    const unhandled = await unhandledRejectionsFrom(() => {
      table.register(1n, 50); // nobody ever awaits this one
      timer.fire(); // ... and its deadline elapses
    });

    expect(unhandled).toEqual([]);
  });

  it("still rejects the call a caller IS awaiting, with the call's own timeout error", async () => {
    const timer = new ManualTimer();
    const table = new CorrelationTable(timer);
    const pending = table.register(1n, 50);

    timer.fire();

    await expect(pending).rejects.toThrow("grain call 1 timed out after 50ms");
  });

  // The send is the one step that can fail after the entry is armed, and a request that never left
  // can never be answered. Releasing it here is what keeps the table to live calls: without it the
  // entry sits until its deadline and fires on a call no caller is waiting for.
  it("fail settles a call whose request never left, and clears its deadline", async () => {
    const timer = new ManualTimer();
    const table = new CorrelationTable(timer);
    const pending = table.register(1n, 50);

    expect(table.fail(1n, new Error("no listener at silo-1"))).toBe(true);

    await expect(pending).rejects.toThrow("no listener at silo-1");
    // Nothing is left armed: a late reply cannot complete it, and its deadline can no longer fire.
    expect(timer.outstanding()).toBe(0);
    expect(table.complete(response(1n))).toBe(false);
    expect(table.fail(1n, new Error("already settled"))).toBe(false);
  });

  it("fail leaves other calls, and calls for other peers, untouched", async () => {
    const timer = new ManualTimer();
    const table = new CorrelationTable(timer);
    const untouched = table.register(2n, 50, "silo-b");

    table.fail(1n, new Error("no listener"));

    expect(timer.outstanding()).toBe(1);
    table.complete(response(2n));
    await expect(untouched).resolves.toMatchObject({ correlationId: 2n });
  });
});
