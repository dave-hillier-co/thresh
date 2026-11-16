import { describe, expect, it } from "vitest";
import type { Logger, LogFields } from "@thresh/core/logger";
import type { Catalog } from "@thresh/runtime/catalog";
import { ActivationCollector } from "@thresh/runtime/activation-collector";
import { FakeTimeProvider } from "@thresh/runtime/test-support/fake-time-provider";

function recordingLogger(): { logger: Logger; warnings: Array<[string, LogFields | undefined]> } {
  const warnings: Array<[string, LogFields | undefined]> = [];
  return {
    warnings,
    logger: {
      debug() {},
      info() {},
      warn(message, fields) {
        warnings.push([message, fields]);
      },
      error() {},
    },
  };
}

const flush = (): Promise<void> => new Promise((r) => setTimeout(r, 0));

describe("ActivationCollector", () => {
  it("does not produce an unhandled rejection when collectIdle rejects, and keeps sweeping", async () => {
    const time = new FakeTimeProvider();
    const { logger, warnings } = recordingLogger();
    let calls = 0;
    const catalog = {
      collectIdle: async () => {
        calls++;
        throw new Error("sweep boom");
      },
    } as unknown as Catalog;

    const unhandled: unknown[] = [];
    const onUnhandledRejection = (reason: unknown) => unhandled.push(reason);
    process.on("unhandledRejection", onUnhandledRejection);

    const collector = new ActivationCollector(catalog, time, 1000, logger);
    try {
      collector.start();
      time.advance(1000);
      await flush();
      time.advance(1000);
      await flush();

      expect(calls).toBe(2);
      expect(unhandled).toHaveLength(0);
      expect(
        warnings.some(([msg, fields]) => msg.includes("sweep") && fields?.error instanceof Error),
      ).toBe(true);
    } finally {
      process.off("unhandledRejection", onUnhandledRejection);
      collector.stop();
    }
  });

  // L7: Orleans' collection loop awaits each sweep before waiting for the next
  // tick (`ActivationCollector.RunActivationCollectionLoop`), so sweeps never
  // overlap.
  it("does not start a second sweep for a tick that arrives while one is running", async () => {
    const time = new FakeTimeProvider();
    let calls = 0;
    let finishSweep: () => void = () => undefined;
    const catalog = {
      collectIdle: () => {
        calls++;
        return new Promise<void>((resolve) => (finishSweep = resolve));
      },
    } as unknown as Catalog;

    const collector = new ActivationCollector(catalog, time, 1000);
    try {
      collector.start();
      time.advance(1000);
      await flush();
      expect(calls).toBe(1);

      time.advance(1000); // the first sweep is still in flight
      time.advance(1000);
      await flush();
      expect(calls).toBe(1);

      // The ticks it missed coalesce into one sweep once it ends (Orleans'
      // `PeriodicTimer`), which is again never overlapped.
      finishSweep();
      await flush();
      expect(calls).toBe(2);
      time.advance(1000);
      await flush();
      expect(calls).toBe(2);
      finishSweep();
      await flush();
      expect(calls).toBe(3);
    } finally {
      collector.stop();
    }
  });
});
