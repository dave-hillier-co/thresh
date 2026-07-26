// Ported from dotnet/orleans test/Grains/TestGrainInterfaces/IGrainServiceTestGrain.cs
// and test/Grains/TestGrains/GrainService/ITestGrainService.cs @ v10.1.0 (MIT).
//
// Upstream splits `ITestGrainService` (the `GrainService` itself) from
// `ITestGrainServiceClient` (the per-silo `GrainServiceClient<T>` a grain gets
// injected via DI). This port has no DI container to inject a client through,
// so `GrainServiceTestGrain` reaches the service directly via
// `runtime.getGrainService<TestGrainService>(name)` (see `grain-service.ts`'s
// class doc) — only `IGrainServiceTestGrain` (the ported test's own surface)
// and `IEchoExtension` (the grain-extension the service hosts) need declaring
// here.
import { defineGrainInterface } from "@thresh/core/grain-interface";
import type { GrainWithIntegerKey } from "@thresh/core/key-kinds";

export interface IGrainServiceTestGrain extends GrainWithIntegerKey {
  getHelloWorldUsingCustomService(): Promise<string>;
  callHasStarted(): Promise<boolean>;
  callHasStartedInBackground(): Promise<boolean>;
  callHasInit(): Promise<boolean>;
  getServiceConfigProperty(): Promise<string>;
  echoViaExtension(what: string): Promise<string>;
}

export const IGrainServiceTestGrain = defineGrainInterface<IGrainServiceTestGrain>(
  "UnitTests.GrainInterfaces.IGrainServiceTestGrain",
);

/**
 * The `IEchoExtension` the ported `TestGrainService` hosts (upstream
 * `Tester.GrainServiceTests.EchoExtension` / `IEchoExtension`), reached via
 * `TestGrainService.asReference(IEchoExtension)`. Not extended onto any grain
 * activation — it lives on the grain SERVICE, not on a grain.
 */
export interface IEchoExtension {
  echo(what: string): Promise<string>;
}

export const IEchoExtension = defineGrainInterface<IEchoExtension>("Tester.IEchoExtension", {
  extension: true,
});
