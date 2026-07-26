// Ported from dotnet/orleans test/Orleans.DefaultCluster.Tests/ErrorGrainTest.cs @ v10.1.0 (MIT).
import { afterAll, beforeAll, describe, expect } from "vitest";
import { orleansTest } from "@thresh/testing/orleans-test";
import { TestCluster } from "@thresh/testing/test-cluster";
import { Guid } from "@thresh/core/guid";
import {
  AsyncSimpleGrain,
  ISimpleGrainWithAsyncMethods,
} from "@thresh/parity/grains/impl/async-simple-grain";
import { EchoGrain, IEchoGrain } from "@thresh/parity/grains/impl/echo-task-grain";
import { ErrorGrain, IErrorGrain } from "@thresh/parity/grains/impl/error-grain";
import {
  PromiseForwardGrain,
  IPromiseForwardGrain,
} from "@thresh/parity/grains/impl/promise-forward-grain";
import { ISimpleGrain, SimpleGrain } from "@thresh/parity/grains/impl/simple-grain";
import { randomIntegerKey } from "@thresh/parity/support/keys";

// Ported from dotnet/orleans test/Orleans.DefaultCluster.Tests/LocalErrorGrain.cs @ v10.1.0
// (MIT): "Not a real Orleans grain - used as a local mock for testing error cases", so it
// is a plain class rather than a registered grain.
class LocalErrorGrain {
  private a = 0;
  private b = 0;

  async setA(a: number): Promise<void> {
    this.a = a;
  }

  async setB(b: number): Promise<void> {
    this.b = b;
  }

  async getAxB(): Promise<number> {
    return this.a * this.b;
  }

  async getAxBError(): Promise<number> {
    await Promise.resolve();
    throw new Error("GetAxBError-Exception");
  }
}

describe("DefaultCluster.Tests.ErrorGrainTest", () => {
  let cluster: TestCluster;

  beforeAll(async () => {
    cluster = await TestCluster.start({
      initialSilos: 2,
      grains: [
        { ctor: ErrorGrain, interfaces: [IErrorGrain] },
        { ctor: SimpleGrain, interfaces: [ISimpleGrain] },
        { ctor: AsyncSimpleGrain, interfaces: [ISimpleGrainWithAsyncMethods] },
        { ctor: PromiseForwardGrain, interfaces: [IPromiseForwardGrain] },
        { ctor: EchoGrain, interfaces: [IEchoGrain] },
      ],
    });
  });

  afterAll(async () => {
    await cluster.dispose();
  });

  orleansTest("DefaultCluster.Tests.ErrorGrainTest.ErrorGrain_GetGrain", async () => {
    const grain = cluster.getGrain(IErrorGrain, randomIntegerKey());
    await grain.getA();
  });

  orleansTest("DefaultCluster.Tests.ErrorGrainTest.ErrorHandlingLocalError", async () => {
    const localGrain = new LocalErrorGrain();

    const intPromise = localGrain.getAxBError();
    await expect(intPromise).rejects.toThrow("GetAxBError-Exception");
  });

  orleansTest("DefaultCluster.Tests.ErrorGrainTest.ErrorHandlingGrainError1", async () => {
    const grain = cluster.getGrain(IErrorGrain, randomIntegerKey());

    const intPromise = grain.getAxBError();
    await expect(intPromise).rejects.toThrow("GetAxBError-Exception");
    // Multiple awaits on the same rejected promise consistently throw the same error.
    await expect(intPromise).rejects.toThrow("GetAxBError-Exception");
  });

  orleansTest.excluded(
    "skipped upstream: https://github.com/dotnet/orleans/issues/9558",
    "DefaultCluster.Tests.ErrorGrainTest.ErrorHandlingTimedMethod",
  );

  orleansTest("DefaultCluster.Tests.ErrorGrainTest.ErrorHandlingTimedMethodWithError", async () => {
    const grain = cluster.getGrain(IErrorGrain, randomIntegerKey());
    // Upstream waits 2000ms; shortened since no timing assertion depends on it.
    await expect(grain.longMethodWithError(50)).rejects.toThrow("LongMethodWithError");
  });

  orleansTest(
    "DefaultCluster.Tests.ErrorGrainTest.StressHandlingMultipleDelayedRequests",
    async () => {
      const grain = cluster.getGrain(IErrorGrain, randomIntegerKey());
      const tasks: Promise<void>[] = [];
      let once = true;
      for (let i = 0; i < 500; i++) {
        const promise = grain.delayMethod(1);
        tasks.push(promise);
        if (once) {
          once = false;
          await promise;
        }
      }
      await Promise.all(tasks);
    },
    20_000,
  );

  orleansTest(
    "DefaultCluster.Tests.ErrorGrainTest.ArgumentTypes_ListOfGrainReferences",
    async () => {
      const grain = cluster.getGrain(IErrorGrain, randomIntegerKey());
      const list = [
        cluster.getGrain(IErrorGrain, randomIntegerKey()),
        cluster.getGrain(IErrorGrain, randomIntegerKey()),
      ];
      await grain.addChildren(list);
    },
  );

  orleansTest.excluded(
    ".NET-specific: tests that `RuntimeContext.Current` (thread-affinity/scheduler " +
      "context) is preserved across an `await Task.Delay`; JS is single-threaded, so " +
      "there is no analogous execution-context-loss failure mode to guard against",
    "DefaultCluster.Tests.ErrorGrainTest.AC_DelayedExecutor_2",
  );

  orleansTest("DefaultCluster.Tests.ErrorGrainTest.SimpleGrain_AsyncMethods", async () => {
    const grain = cluster.getGrain(ISimpleGrainWithAsyncMethods, randomIntegerKey());
    await grain.setA_Async(10);
    await grain.setB_Async(30);

    const value = await grain.getAxB_Async();
    expect(value).toBe(300);
  });

  orleansTest("DefaultCluster.Tests.ErrorGrainTest.SimpleGrain_PromiseForward", async () => {
    const forwardGrain: ISimpleGrain = cluster.getGrain(IPromiseForwardGrain, randomIntegerKey());
    const result = await forwardGrain.getAxBArgs(5, 6);
    expect(result).toBe(30);
  });

  orleansTest("DefaultCluster.Tests.ErrorGrainTest.SimpleGrain_GuidDistribution", async () => {
    // Upstream logs GrainId hash-code distribution across two families of GUID
    // patterns (0x1111.."00000000-0000-0000-0000-XXXXXXXXXXXX" and the reverse,
    // "XXXXXXXX-0000-0000-0000-000000000000") purely for manual inspection; it makes
    // no assertions. Getting the references without throwing is the entire
    // observable behavior.
    const n = 0x1111;
    for (let i = 0; i < 5; i++) {
      cluster.getGrain(
        IEchoGrain,
        Guid.parse(`00000000-0000-0000-0000-${(n + i).toString(16).padStart(12, "0")}`),
      );
    }
    for (let i = 0; i < 5; i++) {
      cluster.getGrain(
        IEchoGrain,
        Guid.parse(`${(n + i).toString(16).padStart(8, "0")}-0000-0000-0000-000000000000`),
      );
    }
  });

  // ObserverTest_Disconnect(2) call a runner whose entire body is commented out
  // upstream ("this is for manual repro & validation in the debugger"); no code
  // executes and no assertion is made.
  orleansTest.excluded(
    "upstream test body is entirely commented out (manual debugger repro); no code executes",
    "DefaultCluster.Tests.ErrorGrainTest.ObserverTest_Disconnect",
  );

  orleansTest.excluded(
    "upstream test body is entirely commented out (manual debugger repro); no code executes",
    "DefaultCluster.Tests.ErrorGrainTest.ObserverTest_Disconnect2",
  );
});
