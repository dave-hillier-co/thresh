// Ported from dotnet/orleans test/Orleans.Runtime.Tests/ExceptionPropagationTests.cs @ v10.1.0 (MIT).
import { afterAll, beforeAll, describe, expect } from "vitest";
import { orleansTest } from "@thresh/testing/orleans-test";
import { TestCluster } from "@thresh/testing/test-cluster";
import { ExceptionGrain, IExceptionGrain } from "@thresh/parity/grains/impl/exception-grain";
import { randomIntegerKey } from "@thresh/parity/support/keys";

describe("UnitTests.General.ExceptionPropagationTests", () => {
  let cluster: TestCluster;

  beforeAll(async () => {
    cluster = await TestCluster.start({
      grains: [{ ctor: ExceptionGrain, interfaces: [IExceptionGrain] }],
    });
  });

  afterAll(async () => {
    await cluster.dispose();
  });

  orleansTest(
    "UnitTests.General.ExceptionPropagationTests.ExceptionsPropagatedFromGrainToClient",
    async () => {
      const grain = cluster.getGrain(IExceptionGrain, 0n);

      await expect(grain.throwsInvalidOperationException()).rejects.toThrow("Test exception");
      await expect(grain.throwsNullReferenceException()).rejects.toThrow("null null null");
    },
  );

  orleansTest("UnitTests.General.ExceptionPropagationTests.BasicExceptionPropagation", async () => {
    const grain = cluster.getGrain(IExceptionGrain, randomIntegerKey());
    await expect(grain.throwsInvalidOperationException()).rejects.toThrow("Test exception");
  });

  // Uses `.Wait()` to unwrap the raw `AggregateException` without any async
  // state-machine modification, then asserts the original CLR stack trace
  // contains the (PascalCase) method name; JS Promises have no `.Wait()`/
  // `AggregateException` analogue, and this framework's grain methods are
  // camelCase, so the exact substring assertion cannot be replicated.
  orleansTest.excluded(
    ".NET-specific: relies on Task.Wait()/AggregateException unwrapping and a PascalCase method-name substring in the CLR stack trace",
    "UnitTests.General.ExceptionPropagationTests.ExceptionContainsOriginalStackTrace",
  );

  orleansTest.excluded(
    ".NET-specific: relies on a PascalCase method-name substring in the CLR stack trace, not applicable to camelCase JS methods",
    "UnitTests.General.ExceptionPropagationTests.ExceptionContainsOriginalStackTraceWhenRethrowingLocally",
  );

  orleansTest.excluded(
    ".NET-specific: AggregateException has no JS/TS analogue",
    "UnitTests.General.ExceptionPropagationTests.ExceptionPropagationDoesNotUnwrapAggregateExceptions",
  );

  orleansTest.excluded(
    ".NET-specific: AggregateException has no JS/TS analogue",
    "UnitTests.General.ExceptionPropagationTests.ExceptionPropagationDoesNoFlattenAggregateExceptions",
  );

  orleansTest(
    "UnitTests.General.ExceptionPropagationTests.TaskCancelationPropagation",
    async () => {
      // Upstream asserts a `TaskCanceledException`; the analogue here is the
      // `GrainTaskCanceledError` the grain's canceled-task stand-in throws.
      // Assert on the message rather than the class: the grain's stand-in may
      // raise any of the three cancellation shapes depending on which abort
      // path wins, and the message is the part that is stable across all of
      // them. (Error type itself now DOES survive the wire — see
      // `cluster.error-fidelity.test.ts`.)
      const grain = cluster.getGrain(IExceptionGrain, randomIntegerKey());
      await expect(grain.canceled()).rejects.toThrow(/cancel/i);
    },
  );

  orleansTest(
    "UnitTests.General.ExceptionPropagationTests.GrainForwardingExceptionPropagation",
    async () => {
      const grain = cluster.getGrain(IExceptionGrain, randomIntegerKey());
      const otherGrainId = randomIntegerKey();

      await expect(grain.grainCallToThrowsInvalidOperationException(otherGrainId)).rejects.toThrow(
        "Test exception",
      );
    },
  );

  orleansTest.excluded(
    ".NET-specific: AggregateException has no JS/TS analogue",
    "UnitTests.General.ExceptionPropagationTests.GrainForwardingExceptionPropagationDoesNotUnwrapAggregateExceptions",
  );

  orleansTest(
    "UnitTests.General.ExceptionPropagationTests.SynchronousExceptionThrownShouldResultInFaultedTask",
    async () => {
      const grain = cluster.getGrain(IExceptionGrain, randomIntegerKey());

      // start the grain call but don't await it nor wrap in try/catch, to make sure it doesn't throw synchronously
      const grainCallTask = grain.throwsSynchronousInvalidOperationException();
      await expect(grainCallTask).rejects.toThrow("Test exception");

      const grainCallTask2 = grain.throwsSynchronousInvalidOperationException();
      await expect(grainCallTask2).rejects.toThrow("Test exception");
    },
  );

  orleansTest.excluded(
    "skipped upstream: Implementation of issue #1378 is still pending; also relies on AggregateException, which has no JS/TS analogue",
    "UnitTests.General.ExceptionPropagationTests.ExceptionPropagationForwardsEntireAggregateException",
  );

  orleansTest.excluded(
    ".NET-specific: AggregateException has no JS/TS analogue",
    "UnitTests.General.ExceptionPropagationTests.SynchronousAggregateExceptionThrownShouldResultInFaultedTaskWithOriginalAggregateExceptionUnmodifiedAsInnerException",
  );

  // The remaining cases inject failures via custom `IFieldCodec`/`IDeepCopier`
  // registrations (`UndeserializableTypeCodec`/`UnserializableTypeCodec`) that
  // throw `NotSupportedException` mid (de)serialization across
  // client<->grain and grain<->grain boundaries — .NET serializer internals
  // with no equivalent hook in this framework's message pipeline.
  orleansTest.excluded(
    ".NET serializer internals: custom IFieldCodec registration injects a deserialization failure",
    "UnitTests.General.ExceptionPropagationTests.ExceptionPropagation_GrainCallsClient_Request_Deserialization_Failure",
  );

  orleansTest.excluded(
    ".NET serializer internals: custom IFieldCodec registration injects a serialization failure",
    "UnitTests.General.ExceptionPropagationTests.ExceptionPropagation_GrainCallsClient_Response_Serialization_Failure",
  );

  orleansTest.excluded(
    ".NET serializer internals: custom IFieldCodec registration injects a deserialization failure",
    "UnitTests.General.ExceptionPropagationTests.ExceptionPropagation_GrainCallsClient_Response_Deserialization_Failure",
  );

  orleansTest.excluded(
    ".NET serializer internals: custom IFieldCodec registration injects a serialization failure",
    "UnitTests.General.ExceptionPropagationTests.ExceptionPropagation_GrainCallsClient_Request_Serialization_Failure",
  );

  orleansTest.excluded(
    ".NET serializer internals: custom IFieldCodec registration injects a deserialization failure",
    "UnitTests.General.ExceptionPropagationTests.ExceptionPropagation_GrainCallsGrain_Request_Deserialization_Failure",
  );

  orleansTest.excluded(
    ".NET serializer internals: custom IFieldCodec registration injects a serialization failure",
    "UnitTests.General.ExceptionPropagationTests.ExceptionPropagation_GrainCallsGrain_Request_Serialization_Failure",
  );

  orleansTest.excluded(
    ".NET serializer internals: custom IFieldCodec registration injects a serialization failure",
    "UnitTests.General.ExceptionPropagationTests.ExceptionPropagation_GrainCallsGrain_Response_Serialization_Failure",
  );

  orleansTest.excluded(
    ".NET serializer internals: custom IFieldCodec registration injects a deserialization failure",
    "UnitTests.General.ExceptionPropagationTests.ExceptionPropagation_GrainCallsGrain_Response_Deserialization_Failure",
  );

  orleansTest.excluded(
    ".NET serializer internals: custom IFieldCodec registration injects a deserialization failure",
    "UnitTests.General.ExceptionPropagationTests.ExceptionPropagation_ClientCallsGrain_Request_Deserialization_Failure",
  );

  orleansTest.excluded(
    ".NET serializer internals: custom IFieldCodec registration injects a serialization failure",
    "UnitTests.General.ExceptionPropagationTests.ExceptionPropagation_ClientCallsGrain_Request_Serialization_Failure",
  );

  orleansTest.excluded(
    ".NET serializer internals: custom IFieldCodec registration injects a serialization failure",
    "UnitTests.General.ExceptionPropagationTests.ExceptionPropagation_ClientCallsGrain_Response_Serialization_Failure",
  );

  orleansTest.excluded(
    ".NET serializer internals: custom IFieldCodec registration injects a deserialization failure",
    "UnitTests.General.ExceptionPropagationTests.ExceptionPropagation_ClientCallsGrain_Response_Deserialization_Failure",
  );
});
