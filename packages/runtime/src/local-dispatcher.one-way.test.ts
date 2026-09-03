import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { grain } from "@thresh/core/decorators";
import { Grain } from "@thresh/core/grain";
import { GrainId } from "@thresh/core/grain-id";
import { defineGrainInterface } from "@thresh/core/grain-interface";
import type { GrainKey } from "@thresh/core/key-kinds";
import type { LogFields, Logger } from "@thresh/core/logger";
import type { InvocationRequest } from "@thresh/core/request";
import type { Catalog } from "@thresh/runtime/catalog";
import { LocalDispatcher } from "@thresh/runtime/local-dispatcher";
import { Silo } from "@thresh/runtime/silo";
import { waitFor } from "@thresh/testing/wait";

// A `oneWay` call must be fire-and-forget regardless of WHERE the callee was
// placed: a call to a remote silo resolves as soon as the message hits the
// transport (`ClusterNode.sendRemote`), so a call to a LOCAL activation must
// likewise resolve without waiting for the callee's turn — otherwise the same
// `await grain.fireAndForget()` blocks or doesn't depending on placement, a
// location-transparency break Orleans does not have.

interface ISink extends GrainKey<string> {
  /** Blocks until the test releases it; the caller must not wait for that. */
  slowNotify(): Promise<void>;
  /** Throws inside the callee's turn; the caller must never see it. */
  boom(): Promise<void>;
  record(tag: string): Promise<void>;
  ping(): Promise<string>;
}

const ISink = defineGrainInterface<ISink>("ISink.local-one-way", {
  options: {
    slowNotify: { oneWay: true },
    boom: { oneWay: true },
    record: { oneWay: true },
  },
});

interface RecordedLog {
  message: string;
  fields?: LogFields;
}

/** A `Logger` that just records its warnings, so the detached-failure log is assertable. */
function recordingLogger(): { logger: Logger; warnings: RecordedLog[] } {
  const warnings: RecordedLog[] = [];
  return {
    warnings,
    logger: {
      debug: () => undefined,
      info: () => undefined,
      warn: (message, fields) => {
        warnings.push({ message, ...(fields !== undefined ? { fields } : {}) });
      },
      error: () => undefined,
    },
  };
}

function deferred(): { promise: Promise<void>; resolve: () => void } {
  let resolve!: () => void;
  const promise = new Promise<void>((r) => (resolve = r));
  return { promise, resolve };
}

let release = deferred();
let entered = deferred();
let finished = false;
let order: string[] = [];

@grain()
class SinkGrain extends Grain implements ISink {
  async slowNotify(): Promise<void> {
    entered.resolve();
    await release.promise;
    finished = true;
  }

  async boom(): Promise<void> {
    throw new Error("one-way boom");
  }

  async record(tag: string): Promise<void> {
    order.push(tag);
  }

  async ping(): Promise<string> {
    return "pong";
  }
}

describe("LocalDispatcher one-way delivery", () => {
  let silo: Silo;

  beforeEach(() => {
    release = deferred();
    entered = deferred();
    finished = false;
    order = [];
    silo = new Silo();
    silo.registerGrain(SinkGrain, { interfaces: [ISink] });
    silo.start();
  });

  it("resolves the caller before the local callee's turn completes", async () => {
    const g = silo.getGrain(ISink, "slow");

    await expect(g.slowNotify()).resolves.toBeUndefined();

    // The caller is back while the callee is still mid-turn...
    await entered.promise;
    expect(finished).toBe(false);
    // ...and the callee nevertheless runs to completion once unblocked.
    release.resolve();
    await waitFor(() => finished);
  });

  it("still awaits a non-oneWay call to the same activation", async () => {
    await expect(silo.getGrain(ISink, "slow").ping()).resolves.toBe("pong");
  });

  it("neither rejects the caller nor leaks an unhandled rejection when the callee throws", async () => {
    const unhandled: unknown[] = [];
    const onUnhandledRejection = (reason: unknown): void => {
      unhandled.push(reason);
    };
    process.on("unhandledRejection", onUnhandledRejection);
    try {
      await expect(silo.getGrain(ISink, "boom").boom()).resolves.toBeUndefined();
      // Unhandled-rejection detection is a macrotask behind the throw.
      await new Promise((r) => setTimeout(r, 20));
      expect(unhandled).toEqual([]);
    } finally {
      process.off("unhandledRejection", onUnhandledRejection);
    }
  });

  it("keeps successive one-way calls to one activation in admission order", async () => {
    const g = silo.getGrain(ISink, "fifo");
    // Activate first, so all three calls take the same (already-resolved)
    // delivery path and the assertion is about admission order, not about
    // racing activation.
    expect(await g.ping()).toBe("pong");

    await Promise.all([g.record("1"), g.record("2"), g.record("3")]);

    await waitFor(() => order.length === 3);
    expect(order).toEqual(["1", "2", "3"]);
  });
});

describe("LocalDispatcher one-way failure logging", () => {
  const target = new GrainId("Thing", "a");
  const req = (): InvocationRequest => ({
    target,
    interfaceId: 0,
    method: "notify",
    args: [],
    options: { oneWay: true },
    reentrancyId: "r1",
  });

  let unhandled: unknown[];
  const onUnhandledRejection = (reason: unknown): void => {
    unhandled.push(reason);
  };

  beforeEach(() => {
    unhandled = [];
    process.on("unhandledRejection", onUnhandledRejection);
  });

  afterEach(() => {
    process.off("unhandledRejection", onUnhandledRejection);
  });

  it("logs the detached turn's failure — the caller, by definition, cannot see it", async () => {
    const failure = new Error("callee exploded");
    const activation = { invoke: vi.fn().mockRejectedValue(failure) };
    const catalog = {
      isStatelessWorkerType: () => false,
      getOrCreate: async () => activation,
      pickOrScaleWorker: () => activation,
    } as unknown as Catalog;
    const { logger, warnings } = recordingLogger();

    await expect(new LocalDispatcher(catalog, logger).invoke(req())).resolves.toBeUndefined();
    await new Promise((r) => setTimeout(r, 20));

    expect(activation.invoke).toHaveBeenCalledTimes(1);
    expect(warnings).toHaveLength(1);
    expect(warnings[0]!.message).toContain("one-way");
    expect(warnings[0]!.fields).toMatchObject({
      grainId: target.toString(),
      method: "notify",
      error: failure,
    });
    expect(unhandled).toEqual([]);
  });
});
