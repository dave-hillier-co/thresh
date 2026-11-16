import { describe, expect, it } from "vitest";

import { CorrelationTable, type CorrelationTimer } from "./correlation-table.js";
import type { Message } from "./message.js";

/** The epoch the tables in this file stamp their calls with. */
const EPOCH = 5;

const response = (correlationId: bigint, correlationEpoch = EPOCH): Message =>
  ({ correlationId, correlationEpoch, direction: "response" }) as Message;

/** Stand-ins for two connections: the table only compares them by identity. */
const connectionToA = { name: "connection to silo-a" };
const connectionToB = { name: "connection to silo-b" };

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
  it("completes a call with a response that arrives on the connection the call went out on", async () => {
    const table = new CorrelationTable(undefined, EPOCH);
    const pending = table.register(1n, undefined, connectionToA);

    expect(table.complete(response(1n), connectionToA)).toBe(true);

    await expect(pending).resolves.toMatchObject({ correlationId: 1n });
    expect(table.misrouted).toBe(0);
  });

  it("ignores and counts a response with a known id that arrives on a different connection", async () => {
    const table = new CorrelationTable(undefined, EPOCH);
    const pending = table.register(1n, undefined, connectionToA);

    expect(table.complete(response(1n), connectionToB)).toBe(false);
    expect(table.misrouted).toBe(1);

    // The call is still waiting for its own reply.
    expect(table.complete(response(1n), connectionToA)).toBe(true);
    await expect(pending).resolves.toMatchObject({ correlationId: 1n });
  });

  it("ignores and counts a response with a known id stamped with another process's epoch", async () => {
    const table = new CorrelationTable(undefined, EPOCH);
    const pending = table.register(1n, undefined, connectionToA);

    expect(table.complete(response(1n, EPOCH + 1), connectionToA)).toBe(false);
    const unstamped = { correlationId: 1n, direction: "response" } as Message;
    expect(table.complete(unstamped, connectionToA)).toBe(false);
    expect(table.misrouted).toBe(2);

    expect(table.complete(response(1n), connectionToA)).toBe(true);
    await expect(pending).resolves.toMatchObject({ correlationId: 1n });
  });

  it("does not count a reply for a call that is no longer outstanding as misrouted", () => {
    const table = new CorrelationTable(undefined, EPOCH);
    expect(table.complete(response(9n), connectionToA)).toBe(false);
    expect(table.misrouted).toBe(0);
  });

  it("rejects exactly the calls pending on a lost connection, leaving others pending", async () => {
    const table = new CorrelationTable(undefined, EPOCH);
    const toA = table.register(1n, undefined, connectionToA);
    const toB = table.register(2n, undefined, connectionToB);
    const unbound = table.register(3n);

    table.rejectFor(connectionToA, new Error("connection to silo-a was lost"));

    await expect(toA).rejects.toThrow("connection to silo-a was lost");
    // The other connection's call and the unbound call are still completable.
    table.complete(response(2n), connectionToB);
    table.complete(response(3n));
    await expect(toB).resolves.toMatchObject({ correlationId: 2n });
    await expect(unbound).resolves.toMatchObject({ correlationId: 3n });
  });

  it("rejectFor on a connection with nothing outstanding is a no-op", async () => {
    const table = new CorrelationTable(undefined, EPOCH);
    const pending = table.register(1n, undefined, connectionToA);
    table.rejectFor(connectionToB, new Error("lost"));
    table.complete(response(1n), connectionToA);
    await expect(pending).resolves.toMatchObject({ correlationId: 1n });
  });

  it("rejectAll still fails everything outstanding", async () => {
    const table = new CorrelationTable(undefined, EPOCH);
    const toA = table.register(1n, undefined, connectionToA);
    const unbound = table.register(2n);
    table.rejectAll(new Error("shutting down"));
    await expect(toA).rejects.toThrow("shutting down");
    await expect(unbound).rejects.toThrow("shutting down");
  });

  // A caller can abandon a call — stop awaiting it, or never reach its `await` because the send
  // that followed `register` threw — while the entry stays armed. When its deadline fires, that
  // rejection must reach nothing process-level: under Node's default
  // `--unhandled-rejections=throw` an unobserved one terminates the process, which is how a silo
  // (or the migration example) dies long after a call it had already given up on.
  it("an abandoned call's timeout rejection never reaches the process unhandled", async () => {
    const timer = new ManualTimer();
    const table = new CorrelationTable(timer, EPOCH);

    const unhandled = await unhandledRejectionsFrom(() => {
      table.register(1n, 50); // nobody ever awaits this one
      timer.fire(); // ... and its deadline elapses
    });

    expect(unhandled).toEqual([]);
  });

  it("still rejects the call a caller IS awaiting, with the call's own timeout error", async () => {
    const timer = new ManualTimer();
    const table = new CorrelationTable(timer, EPOCH);
    const pending = table.register(1n, 50);

    timer.fire();

    await expect(pending).rejects.toThrow("grain call 1 timed out after 50ms");
  });

  // The send is the one step that can fail after the entry is armed, and a request that never left
  // can never be answered. Releasing it here is what keeps the table to live calls: without it the
  // entry sits until its deadline and fires on a call no caller is waiting for.
  it("fail settles a call whose request never left, and clears its deadline", async () => {
    const timer = new ManualTimer();
    const table = new CorrelationTable(timer, EPOCH);
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
    const table = new CorrelationTable(timer, EPOCH);
    const untouched = table.register(2n, 50, connectionToB);

    table.fail(1n, new Error("no listener"));

    expect(timer.outstanding()).toBe(1);
    table.complete(response(2n), connectionToB);
    await expect(untouched).resolves.toMatchObject({ correlationId: 2n });
  });
});
