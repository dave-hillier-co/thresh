// Ported from dotnet/orleans test/Orleans.Core.Tests/ServiceLifecycleTests.cs @ v10.1.0 (MIT).
import { describe, it } from "vitest";
import { orleansTest } from "@tsva/testing/orleans-test";

describe("NonSilo.Tests.ServiceLifecycleTests", () => {
  orleansTest.excluded(
    "tests Orleans' internal staged ServiceLifecycle<T>/SiloLifecycleSubject notification pipeline, a .NET hosting primitive with no public equivalent in this framework",
    "NonSilo.Tests.ServiceLifecycleTests.BasicCallbackExecution",
  );
  orleansTest.excluded(
    "tests Orleans' internal staged ServiceLifecycle<T>/SiloLifecycleSubject notification pipeline, a .NET hosting primitive with no public equivalent in this framework",
    "NonSilo.Tests.ServiceLifecycleTests.Stage_WaitAsync",
  );
  orleansTest.excluded(
    "tests Orleans' internal staged ServiceLifecycle<T>/SiloLifecycleSubject notification pipeline, a .NET hosting primitive with no public equivalent in this framework",
    "NonSilo.Tests.ServiceLifecycleTests.Stage_WaitAsync_Cancellation",
  );
  orleansTest.excluded(
    "tests Orleans' internal staged ServiceLifecycle<T>/SiloLifecycleSubject notification pipeline, a .NET hosting primitive with no public equivalent in this framework",
    "NonSilo.Tests.ServiceLifecycleTests.Stage_NotifyCompleted_IsIdempotent",
  );
  orleansTest.excluded(
    "tests Orleans' internal staged ServiceLifecycle<T>/SiloLifecycleSubject notification pipeline, a .NET hosting primitive with no public equivalent in this framework",
    "NonSilo.Tests.ServiceLifecycleTests.CallbackDisposal_PreventsExecution",
  );
  orleansTest.excluded(
    "tests Orleans' internal staged ServiceLifecycle<T>/SiloLifecycleSubject notification pipeline, a .NET hosting primitive with no public equivalent in this framework",
    "NonSilo.Tests.ServiceLifecycleTests.CancellationToken_TriggeredOnStageCompletion",
  );
  orleansTest.excluded(
    "tests Orleans' internal staged ServiceLifecycle<T>/SiloLifecycleSubject notification pipeline, a .NET hosting primitive with no public equivalent in this framework",
    "NonSilo.Tests.ServiceLifecycleTests.ErrorHandling_TerminateOnErrorFalse",
  );
  orleansTest.excluded(
    "tests Orleans' internal staged ServiceLifecycle<T>/SiloLifecycleSubject notification pipeline, a .NET hosting primitive with no public equivalent in this framework",
    "NonSilo.Tests.ServiceLifecycleTests.ErrorHandling_TerminateOnErrorTrue",
  );
  orleansTest.excluded(
    "tests Orleans' internal staged ServiceLifecycle<T>/SiloLifecycleSubject notification pipeline, a .NET hosting primitive with no public equivalent in this framework",
    "NonSilo.Tests.ServiceLifecycleTests.ErrorHandling_TerminateOnErrorTrue_MultipleFailures",
  );
  orleansTest.excluded(
    "tests Orleans' internal staged ServiceLifecycle<T>/SiloLifecycleSubject notification pipeline, a .NET hosting primitive with no public equivalent in this framework",
    "NonSilo.Tests.ServiceLifecycleTests.LateRegistration_ExecutedImmediately",
  );
  orleansTest.excluded(
    "tests Orleans' internal staged ServiceLifecycle<T>/SiloLifecycleSubject notification pipeline, a .NET hosting primitive with no public equivalent in this framework",
    "NonSilo.Tests.ServiceLifecycleTests.ConcurrentCallbacks_RegistrationSafe",
  );
  orleansTest.excluded(
    "tests Orleans' internal staged ServiceLifecycle<T>/SiloLifecycleSubject notification pipeline, a .NET hosting primitive with no public equivalent in this framework",
    "NonSilo.Tests.ServiceLifecycleTests.MultipleStages_ExecuteInOrder",
  );
  orleansTest.excluded(
    "tests Orleans' internal staged ServiceLifecycle<T>/SiloLifecycleSubject notification pipeline, a .NET hosting primitive with no public equivalent in this framework",
    "NonSilo.Tests.ServiceLifecycleTests.BackgroundWorker_StopsOnCancellation",
  );
  orleansTest.excluded(
    "tests Orleans' internal staged ServiceLifecycle<T>/SiloLifecycleSubject notification pipeline, a .NET hosting primitive with no public equivalent in this framework",
    "NonSilo.Tests.ServiceLifecycleTests.Lifecycle_CancellationToken_PassedToCallback",
  );

  // vitest requires at least one runtime test per file; all upstream Facts are
  // excluded above, so this placeholder keeps the file a valid suite.
  it.skip("(all tests in this file are orleansTest.excluded - see above)", () => undefined);
});
