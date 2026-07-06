import { describe, expect, it } from "vitest";
import { GrainExtensionNotInstalledException } from "@tsva/core/errors";
import { Grain } from "@tsva/core/grain";
import { GrainId } from "@tsva/core/grain-id";
import { defineGrainInterface } from "@tsva/core/grain-interface";
import type { InvocationRequest } from "@tsva/core/request";
import { ActivationData } from "@tsva/runtime/activation";
import { FakeTimeProvider } from "@tsva/runtime/test-support/fake-time-provider";

/** A trivial `GrainExtension` interface — an orthogonal method surface. */
interface ITestExtension {
  ping(): Promise<string>;
}

const ITestExtension = defineGrainInterface<ITestExtension>("ITestExtension.extension-test", {
  extension: true,
});

/** An ordinary (non-extension) interface, for the regression case. */
interface ICounter {
  get(): Promise<number>;
}

const ICounter = defineGrainInterface<ICounter>("ICounter.extension-test");

class CounterGrain extends Grain {
  pingCalls = 0;

  async ping(): Promise<string> {
    // If dispatch ever fell through to the instance for the extension call,
    // this would run and the test would see it — it must not.
    this.pingCalls += 1;
    return "from-instance";
  }

  async get(): Promise<number> {
    return 42;
  }
}

const id = new GrainId("Counter", "a");

function makeActivation(): { activation: ActivationData; grain: CounterGrain } {
  const activation = new ActivationData(id, new FakeTimeProvider(), 30_000, false, "act-1");
  const grain = new CounterGrain();
  grain.setContext(activation);
  activation.instance = grain;
  activation.beginActivate("incoming-call");
  return { activation, grain };
}

function extensionCall(): InvocationRequest {
  return {
    target: id,
    interfaceId: ITestExtension.id,
    method: "ping",
    args: [],
    options: {},
    reentrancyId: "r1",
  };
}

function counterCall(): InvocationRequest {
  return {
    target: id,
    interfaceId: ICounter.id,
    method: "get",
    args: [],
    options: {},
    reentrancyId: "r2",
  };
}

describe("grain extension dispatch", () => {
  it("throws GrainExtensionNotInstalledException when nothing is bound", async () => {
    const { activation } = makeActivation();

    await expect(activation.invoke(extensionCall())).rejects.toThrow(
      GrainExtensionNotInstalledException,
    );
  });

  it("dispatches to the bound extension object, not the grain instance", async () => {
    const { activation, grain } = makeActivation();
    let factoryRuns = 0;
    const bound = activation.getOrSetExtension(ITestExtension.id, () => {
      factoryRuns += 1;
      return { ping: async () => "from-extension" };
    });

    const result = await activation.invoke(extensionCall());

    expect(result).toBe("from-extension");
    expect(grain.pingCalls).toBe(0);
    expect(factoryRuns).toBe(1);
    expect(bound.ping()).resolves.toBe("from-extension");
  });

  it("is idempotent: a second getOrSetExtension returns the same instance without re-running the factory", () => {
    const { activation } = makeActivation();
    let factoryRuns = 0;
    const first = activation.getOrSetExtension(ITestExtension.id, () => {
      factoryRuns += 1;
      return { ping: async () => "one" };
    });
    const second = activation.getOrSetExtension(ITestExtension.id, () => {
      factoryRuns += 1;
      return { ping: async () => "two" };
    });

    expect(second).toBe(first);
    expect(factoryRuns).toBe(1);
  });

  it("still dispatches non-extension interface calls to the grain instance (regression)", async () => {
    const { activation } = makeActivation();

    const result = await activation.invoke(counterCall());

    expect(result).toBe(42);
  });
});
