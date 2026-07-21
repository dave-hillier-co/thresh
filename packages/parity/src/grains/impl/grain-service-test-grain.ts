// Ported from dotnet/orleans test/Orleans.Runtime.Tests/GrainServiceTests/TestGrainService.cs
// and test/Grains/TestGrains/GrainService/GrainServiceTestGrain.cs @ v10.1.0 (MIT).
//
// `TestGrainService` and `EchoExtension` are registered on the silo via
// `SiloBuilder.addGrainService` in the test file itself (mirroring upstream's
// `AddTestGrainService` `ISiloBuilder` extension, an inline configurator
// rather than a shared package export — Orleans keeps it local to the test
// fixture too). `GrainServiceTestGrain` reaches the service through
// `runtime.getGrainService` instead of an injected `ITestGrainServiceClient`
// (see the interfaces file's class doc for why).
import { grain } from "@tsva/core/decorators";
import { Grain } from "@tsva/core/grain";
import { GrainService } from "@tsva/runtime/grain-service";
import {
  IEchoExtension,
  IGrainServiceTestGrain,
} from "@tsva/parity/grains/interfaces/grain-service-test-grain-interfaces";

export { IGrainServiceTestGrain };

/** Upstream `Tester.GrainServiceTests.EchoExtension`. */
export class EchoExtension implements IEchoExtension {
  echo(what: string): Promise<string> {
    return Promise.resolve(what);
  }
}

/** The name `TestGrainService` is registered under (`builder.addGrainService(name, ...)`). */
export const TEST_GRAIN_SERVICE_NAME = "testGrainService";

/**
 * Upstream `Tester.TestGrainService` (`Orleans.Runtime.Services.GrainService`
 * subclass), including the `EchoExtension` it hosts via `AddGrainExtension`.
 */
export class TestGrainService extends GrainService {
  private started = false;
  private startedInBackground = false;
  private didInit = false;

  constructor(private readonly configProperty: string) {
    super();
    this.addExtension(IEchoExtension, new EchoExtension());
  }

  override init(): void {
    this.didInit = true;
  }

  override start(): void {
    this.started = true;
    super.start();
  }

  protected override startInBackground(): void {
    this.startedInBackground = true;
  }

  getHelloWorldUsingCustomService(): Promise<string> {
    return Promise.resolve("Hello World from Test Grain Service");
  }

  hasStarted(): Promise<boolean> {
    return Promise.resolve(this.started);
  }

  hasStartedInBackground(): Promise<boolean> {
    return Promise.resolve(this.startedInBackground);
  }

  hasInit(): Promise<boolean> {
    return Promise.resolve(this.didInit);
  }

  getServiceConfigProperty(): Promise<string> {
    return Promise.resolve(this.configProperty);
  }
}

/** Upstream `UnitTests.Grains.GrainServiceTestGrain`. */
@grain({ name: "UnitTests.Grains.GrainServiceTestGrain" })
export class GrainServiceTestGrain extends Grain implements IGrainServiceTestGrain {
  private service(): TestGrainService {
    return this.runtime.getGrainService<TestGrainService>(TEST_GRAIN_SERVICE_NAME);
  }

  getHelloWorldUsingCustomService(): Promise<string> {
    return this.service().getHelloWorldUsingCustomService();
  }

  callHasStarted(): Promise<boolean> {
    return this.service().hasStarted();
  }

  callHasStartedInBackground(): Promise<boolean> {
    return this.service().hasStartedInBackground();
  }

  callHasInit(): Promise<boolean> {
    return this.service().hasInit();
  }

  getServiceConfigProperty(): Promise<string> {
    return this.service().getServiceConfigProperty();
  }

  echoViaExtension(what: string): Promise<string> {
    return this.service().asReference(IEchoExtension).echo(what);
  }
}
