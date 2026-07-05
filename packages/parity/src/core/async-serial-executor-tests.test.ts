// Ported from dotnet/orleans test/Orleans.Core.Tests/OrleansRuntime/AsyncSerialExecutorTests.cs @ v10.1.0 (MIT).
import { describe } from "vitest";
import { orleansTest } from "@tsva/testing/orleans-test";

describe("UnitTests.OrleansRuntime.AsyncSerialExecutorTests", () => {
  orleansTest.gap(
    "GAP-ASYNC-SERIAL-EXECUTOR",
    "UnitTests.OrleansRuntime.AsyncSerialExecutorTests.AsyncSerialExecutorTests_Small",
  );
  orleansTest.gap(
    "GAP-ASYNC-SERIAL-EXECUTOR",
    "UnitTests.OrleansRuntime.AsyncSerialExecutorTests.AsyncSerialExecutorTests_SerialSubmit",
  );
  orleansTest.gap(
    "GAP-ASYNC-SERIAL-EXECUTOR",
    "UnitTests.OrleansRuntime.AsyncSerialExecutorTests.AsyncSerialExecutorTests_ParallelSubmit",
  );
});
