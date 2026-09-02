import { describe, expect, it } from "vitest";
import { Grain } from "@thresh/core/grain";
import { GrainId } from "@thresh/core/grain-id";
import type { Logger, LogFields } from "@thresh/core/logger";
import { ActivationData, type ActivationOptions } from "@thresh/runtime/activation";
import { FakeTimeProvider } from "@thresh/runtime/test-support/fake-time-provider";

function deferred<T = void>() {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>((res) => {
    resolve = res;
  });
  return { promise, resolve };
}

const flush = () => new Promise((r) => setTimeout(r, 0));

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

const id = new GrainId("ErrorHandlingGrain", "a");

class PlainGrain extends Grain {}

class BadDeactivateGrain extends Grain {
  deactivateGate = deferred<void>();

  override async onDeactivate(): Promise<void> {
    await this.deactivateGate.promise;
    throw new Error("onDeactivate boom");
  }
}

function makeActivation<T extends Grain>(
  time: FakeTimeProvider,
  grain: T,
  options: ActivationOptions = {},
): ActivationData {
  const activation = new ActivationData(id, time, 30_000, false, "act-1", options);
  grain.setContext(activation);
  activation.instance = grain;
  activation.beginActivate("incoming-call");
  return activation;
}

describe("ActivationData timer callback errors (Orleans TimerQueueTimer.TimerTick)", () => {
  it("logs a throwing timer callback via the configured logger and keeps the periodic timer running", async () => {
    const time = new FakeTimeProvider();
    const { logger, warnings } = recordingLogger();
    const activation = makeActivation(time, new PlainGrain(), { logger });
    await flush();

    let ticks = 0;
    activation.registerTimer(
      async () => {
        ticks++;
        throw new Error("timer callback boom");
      },
      { ms: 1000 },
      { ms: 1000 },
    );

    time.advance(1000);
    await flush();
    time.advance(1000);
    await flush();

    expect(ticks).toBe(2);
    expect(
      warnings.some(
        ([msg, fields]) =>
          msg.includes("grain timer") &&
          fields?.grainId === id.toString() &&
          fields?.error instanceof Error,
      ),
    ).toBe(true);
    // Logged once per failing tick, not just the first.
    expect(warnings.filter(([msg]) => msg.includes("grain timer")).length).toBe(2);
  });
});

describe("ActivationData onDeactivate hook errors", () => {
  it("logs via the configured logger instead of silently discarding the error", async () => {
    const time = new FakeTimeProvider();
    const { logger, warnings } = recordingLogger();
    const grain = new BadDeactivateGrain();
    const activation = makeActivation(time, grain, { logger });
    await flush();

    grain.deactivateGate.resolve();
    await activation.deactivate({ code: "application-requested", description: "test" });

    expect(activation.state).toBe("invalid");
    expect(
      warnings.some(
        ([msg, fields]) =>
          msg.includes("onDeactivate") &&
          fields?.grainId === id.toString() &&
          fields?.error instanceof Error,
      ),
    ).toBe(true);
  });
});
