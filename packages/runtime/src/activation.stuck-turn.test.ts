import { describe, expect, it } from "vitest";
import { RejectionError } from "@thresh/core/errors";
import { Grain } from "@thresh/core/grain";
import { GrainId } from "@thresh/core/grain-id";
import type { Logger, LogFields } from "@thresh/core/logger";
import type { InvocationRequest } from "@thresh/core/request";
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

const id = new GrainId("StuckGrain", "a");

class WedgedGrain extends Grain {
  gate = deferred<string>();
  deactivateCalls: unknown[] = [];

  async block(): Promise<string> {
    return this.gate.promise;
  }

  async quick(): Promise<string> {
    return "quick:done";
  }

  override async onDeactivate(reason: unknown): Promise<void> {
    this.deactivateCalls.push(reason);
  }
}

function makeActivation(
  time: FakeTimeProvider,
  options: ActivationOptions = {},
): { activation: ActivationData; grain: WedgedGrain } {
  const activation = new ActivationData(id, time, 30_000, false, "act-1", options);
  const grain = new WedgedGrain();
  grain.setContext(activation);
  activation.instance = grain;
  activation.beginActivate("incoming-call");
  return { activation, grain };
}

const call = (method = "block"): InvocationRequest => ({
  target: id,
  interfaceId: 0,
  method,
  args: [],
  options: {},
  reentrancyId: `r-${Math.random()}`,
});

describe("ActivationData stuck-turn deactivation (Orleans DeactivateStuckActivation)", () => {
  it("marks the activation invalid once the blocking turn exceeds maxRequestProcessingTimeMs, without running onDeactivate", async () => {
    const time = new FakeTimeProvider();
    const { activation, grain } = makeActivation(time, { maxRequestProcessingTimeMs: 1000 });
    await flush();

    void activation.invoke(call());
    await flush();
    time.advance(1000);

    expect(activation.state).toBe("invalid");
    // Orleans' `DeactivateStuckActivation` never awaits `OnDeactivateAsync` —
    // scheduling it here would only queue another turn behind the wedged one.
    expect(grain.deactivateCalls).toHaveLength(0);

    grain.gate.resolve("done"); // let the wedged turn settle so the test cleans up
  });

  it("rejects a turn queued behind the stuck one instead of leaving it queued forever", async () => {
    const time = new FakeTimeProvider();
    const { activation, grain } = makeActivation(time, { maxRequestProcessingTimeMs: 1000 });
    await flush();

    void activation.invoke(call()); // running -> becomes the blocking turn
    const queued = activation.invoke(call("quick"));
    await flush();
    time.advance(1000);

    await expect(queued).rejects.toBeInstanceOf(RejectionError);
    await expect(queued).rejects.toMatchObject({ kind: "noActivation" });

    grain.gate.resolve("done");
  });

  it("rejects a call made after the activation is already stuck the same way, rather than queuing it", async () => {
    const time = new FakeTimeProvider();
    const { activation, grain } = makeActivation(time, { maxRequestProcessingTimeMs: 1000 });
    await flush();

    void activation.invoke(call());
    await flush();
    time.advance(1000);
    expect(activation.state).toBe("invalid");

    await expect(activation.invoke(call("quick"))).rejects.toBeInstanceOf(RejectionError);

    grain.gate.resolve("done");
  });

  it("leaves the wedged turn itself running to completion — it is not aborted", async () => {
    const time = new FakeTimeProvider();
    const { activation, grain } = makeActivation(time, { maxRequestProcessingTimeMs: 1000 });
    await flush();

    const wedged = activation.invoke(call());
    await flush();
    time.advance(1000);

    grain.gate.resolve("finally done");
    await expect(wedged).resolves.toBe("finally done");
  });

  it("calls the onStuck hook with this activation once it is deactivated", async () => {
    const time = new FakeTimeProvider();
    const stuckActivations: ActivationData[] = [];
    const { activation, grain } = makeActivation(time, {
      maxRequestProcessingTimeMs: 1000,
      onStuck: (a) => stuckActivations.push(a),
    });
    await flush();

    void activation.invoke(call());
    await flush();
    time.advance(1000);

    expect(stuckActivations).toEqual([activation]);
    grain.gate.resolve("done");
  });

  it("warns via the configured logger", async () => {
    const time = new FakeTimeProvider();
    const { logger, warnings } = recordingLogger();
    const { activation, grain } = makeActivation(time, {
      maxRequestProcessingTimeMs: 1000,
      logger,
    });
    await flush();

    void activation.invoke(call());
    await flush();
    time.advance(1000);

    expect(warnings.some(([msg]) => msg.includes("MaxRequestProcessingTime"))).toBe(true);
    grain.gate.resolve("done");
  });
});
