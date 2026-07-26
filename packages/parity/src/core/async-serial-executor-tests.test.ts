// Ported from dotnet/orleans test/Orleans.Core.Tests/OrleansRuntime/AsyncSerialExecutorTests.cs @ v10.1.0 (MIT).
import { describe, expect, it } from "vitest";
import { AsyncSerialExecutor } from "@thresh/core/async-serial-executor";

describe("UnitTests.OrleansRuntime.AsyncSerialExecutorTests", () => {
  let operationsInProgress = 0;

  async function operation(opNumber: number): Promise<void> {
    if (operationsInProgress > 0) {
      throw new Error(
        `1: Operation ${opNumber} found ${operationsInProgress} operationsInProgress.`,
      );
    }
    operationsInProgress++;

    await new Promise((resolve) => setTimeout(resolve, 0));
    if (operationsInProgress !== 1) {
      throw new Error(
        `2: Operation ${opNumber} found ${operationsInProgress} operationsInProgress.`,
      );
    }

    await new Promise((resolve) => setTimeout(resolve, 0));
    if (operationsInProgress !== 1) {
      throw new Error(
        `3: Operation ${opNumber} found ${operationsInProgress} operationsInProgress.`,
      );
    }

    operationsInProgress--;
  }

  // UnitTests.OrleansRuntime.AsyncSerialExecutorTests.AsyncSerialExecutorTests_Small
  it("runs a small number of submitted operations without interleaving", async () => {
    const executor = new AsyncSerialExecutor();
    operationsInProgress = 0;

    const tasks = [
      executor.addNext(() => operation(1)),
      executor.addNext(() => operation(2)),
      executor.addNext(() => operation(3)),
    ];

    await Promise.all(tasks);
  });

  // UnitTests.OrleansRuntime.AsyncSerialExecutorTests.AsyncSerialExecutorTests_SerialSubmit
  it("runs operations submitted one after another, in order, without interleaving", async () => {
    const executor = new AsyncSerialExecutor();
    operationsInProgress = 0;

    const tasks: Array<Promise<void>> = [];
    for (let i = 0; i < 10; i++) {
      const capture = i;
      tasks.push(executor.addNext(() => operation(capture)));
    }
    await Promise.all(tasks);
  });

  // UnitTests.OrleansRuntime.AsyncSerialExecutorTests.AsyncSerialExecutorTests_ParallelSubmit
  it("runs operations submitted concurrently from multiple callers without interleaving", async () => {
    const executor = new AsyncSerialExecutor();
    operationsInProgress = 0;

    const tasks: Array<Promise<void>> = [];
    const enqueueTasks: Array<Promise<void>> = [];
    for (let i = 0; i < 10; i++) {
      const capture = i;
      enqueueTasks.push(
        Promise.resolve().then(() => {
          tasks.push(executor.addNext(() => operation(capture)));
        }),
      );
    }
    await Promise.all(enqueueTasks);
    await Promise.all(tasks);
    expect(tasks).toHaveLength(10);
  });
});
