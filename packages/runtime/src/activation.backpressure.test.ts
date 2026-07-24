import { describe, expect, it } from "vitest";
import { LimitExceededException } from "@tsva/core/errors";
import { Grain } from "@tsva/core/grain";
import { GrainId } from "@tsva/core/grain-id";
import type { Logger, LogFields } from "@tsva/core/logger";
import type { InvocationRequest } from "@tsva/core/request";
import { ActivationData, type ActivationOptions } from "@tsva/runtime/activation";
import { FakeTimeProvider } from "@tsva/runtime/test-support/fake-time-provider";

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

const id = new GrainId("BackpressureGrain", "a");

class BlockingGrain extends Grain {
  gate = deferred();
  deactivateGate = deferred<void>();

  async block(): Promise<string> {
    await this.gate.promise;
    return "done";
  }

  override async onDeactivate(): Promise<void> {
    await this.deactivateGate.promise;
  }
}

function makeActivation(
  time: FakeTimeProvider,
  options: ActivationOptions = {},
): { activation: ActivationData; grain: BlockingGrain } {
  const activation = new ActivationData(id, time, 30_000, false, "act-1", options);
  const grain = new BlockingGrain();
  grain.setContext(activation);
  activation.instance = grain;
  activation.beginActivate("incoming-call");
  return { activation, grain };
}

const call = (): InvocationRequest => ({
  target: id,
  interfaceId: 0,
  method: "block",
  args: [],
  options: {},
  reentrancyId: `r-${Math.random()}`,
});

describe("ActivationData back-pressure (Orleans MaxEnqueuedRequestsSoftLimit/HardLimit)", () => {
  it("rejects a call with LimitExceededException once the queue is at the hard limit", async () => {
    const time = new FakeTimeProvider();
    const { activation } = makeActivation(time, { maxEnqueuedRequestsHardLimit: 1 });
    await flush(); // let the activation turn settle so `invoke` calls actually queue

    void activation.invoke(call()); // running
    void activation.invoke(call()); // queued (queue length 0 -> 1)
    await expect(activation.invoke(call())).rejects.toBeInstanceOf(LimitExceededException);
  });

  it("warns via the configured logger once the soft limit is reached, but still admits the turn", async () => {
    const time = new FakeTimeProvider();
    const { logger, warnings } = recordingLogger();
    const { activation, grain } = makeActivation(time, {
      maxEnqueuedRequestsSoftLimit: 1,
      logger,
    });
    await flush();

    void activation.invoke(call()); // running
    void activation.invoke(call()); // queued: admitted without warning (queue was empty)
    const third = activation.invoke(call()); // queue already has 1 >= soft(1): warns
    await flush();
    expect(warnings.some(([msg]) => msg.includes("MaxEnqueuedRequestsSoftLimit"))).toBe(true);

    grain.gate.resolve();
    await expect(third).resolves.toBe("done");
  });
});

describe("ActivationData deactivation timeout (Orleans CollectionOptions.DeactivationTimeout)", () => {
  it("force-invalidates the activation once onDeactivate exceeds the timeout, without waiting for the hook", async () => {
    const time = new FakeTimeProvider();
    const { logger, warnings } = recordingLogger();
    const { activation } = makeActivation(time, { deactivationTimeoutMs: 1000, logger });
    await flush();

    const deactivatePromise = activation.deactivate({
      code: "application-requested",
      description: "test",
    });
    // Advance past the timeout while onDeactivate's own gate is still open
    // (never resolved) — deactivation must proceed anyway.
    time.advance(1000);
    await deactivatePromise;

    expect(activation.state).toBe("invalid");
    expect(warnings.some(([msg]) => msg.includes("DeactivationTimeout"))).toBe(true);
  });

  it("waits for a well-behaved onDeactivate that settles before the timeout", async () => {
    const time = new FakeTimeProvider();
    const { activation, grain } = makeActivation(time, { deactivationTimeoutMs: 1000 });
    await flush();

    const deactivatePromise = activation.deactivate({
      code: "application-requested",
      description: "test",
    });
    grain.deactivateGate.resolve();
    await deactivatePromise;

    expect(activation.state).toBe("invalid");
  });

  it("waits indefinitely when no deactivation timeout is configured (unchanged default behaviour)", async () => {
    const time = new FakeTimeProvider();
    const { activation, grain } = makeActivation(time);
    await flush();

    let settled = false;
    const deactivatePromise = activation
      .deactivate({ code: "application-requested", description: "test" })
      .then(() => {
        settled = true;
      });
    time.advance(1_000_000);
    await flush();
    expect(settled).toBe(false);

    grain.deactivateGate.resolve();
    await deactivatePromise;
    expect(settled).toBe(true);
  });
});
