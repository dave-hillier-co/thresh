// Ported from dotnet/orleans test/Grains/TestGrains/ActivateDeactivateWatcherGrain.cs and
// test/Grains/TestInternalGrains/ActivateDeactivateTestGrain.cs @ v10.1.0 (MIT).
// Upstream identifies an activation instance by `RuntimeHelpers.GetHashCode(this)`;
// there is no equivalent in JS, so each of these grains mints a fresh Guid per
// activation instead — serving the same purpose (a value unique per incarnation
// a test can compare across reactivations).
import { grain } from "@tsva/core/decorators";
import { Grain } from "@tsva/core/grain";
import { Guid } from "@tsva/core/guid";
import {
  IActivateDeactivateWatcherGrain,
  IBadActivateDeactivateTestGrain,
  IBadConstructorTestGrain,
  ICreateGrainReferenceTestGrain,
  IDeactivatingWhileActivatingTestGrain,
  ILongRunningActivateDeactivateTestGrain,
  ISimpleActivateDeactivateTestGrain,
  ITailCallActivateDeactivateTestGrain,
  ITaskActionActivateDeactivateTestGrain,
} from "@tsva/parity/grains/interfaces/activate-deactivate-test-grain-interfaces";
import { ITestGrain } from "@tsva/parity/grains/interfaces/test-grain-interfaces";

export {
  IActivateDeactivateWatcherGrain,
  IBadActivateDeactivateTestGrain,
  IBadConstructorTestGrain,
  ICreateGrainReferenceTestGrain,
  IDeactivatingWhileActivatingTestGrain,
  ILongRunningActivateDeactivateTestGrain,
  ISimpleActivateDeactivateTestGrain,
  ITailCallActivateDeactivateTestGrain,
  ITaskActionActivateDeactivateTestGrain,
};

@grain({ name: "UnitTests.Grains.ActivateDeactivateWatcherGrain" })
export class ActivateDeactivateWatcherGrain
  extends Grain
  implements IActivateDeactivateWatcherGrain
{
  private readonly activationCalls: string[] = [];
  private readonly deactivationCalls: string[] = [];

  async getActivateCalls(): Promise<string[]> {
    return [...this.activationCalls];
  }

  async getDeactivateCalls(): Promise<string[]> {
    return [...this.deactivationCalls];
  }

  async clear(): Promise<void> {
    this.activationCalls.length = 0;
    this.deactivationCalls.length = 0;
  }

  async recordActivateCall(activation: string): Promise<void> {
    this.activationCalls.push(activation);
  }

  async recordDeactivateCall(activation: string): Promise<void> {
    this.deactivationCalls.push(activation);
  }
}

@grain({ name: "UnitTests.Grains.SimpleActivateDeactivateTestGrain" })
export class SimpleActivateDeactivateTestGrain
  extends Grain
  implements ISimpleActivateDeactivateTestGrain
{
  private activationTag = "";

  override async onActivate(): Promise<void> {
    this.activationTag = Guid.newGuid().toString();
    const watcher = this.getGrain(IActivateDeactivateWatcherGrain, 0n);
    await watcher.recordActivateCall(this.activationTag);
  }

  override async onDeactivate(): Promise<void> {
    const watcher = this.getGrain(IActivateDeactivateWatcherGrain, 0n);
    await watcher.recordDeactivateCall(this.activationTag);
  }

  async doSomething(): Promise<string> {
    return this.activationTag;
  }

  async doDeactivate(): Promise<void> {
    this.runtime.deactivateOnIdle();
  }
}

// Mirrors SimpleActivateDeactivateTestGrain exactly: upstream's tail-call
// variant exercises .NET IL tail calls in the lifecycle hooks, which have no
// JS analogue, so the observable activate/deactivate behaviour is identical.
@grain({ name: "UnitTests.Grains.TailCallActivateDeactivateTestGrain" })
export class TailCallActivateDeactivateTestGrain
  extends Grain
  implements ITailCallActivateDeactivateTestGrain
{
  private activationTag = "";

  override async onActivate(): Promise<void> {
    this.activationTag = Guid.newGuid().toString();
    const watcher = this.getGrain(IActivateDeactivateWatcherGrain, 0n);
    await watcher.recordActivateCall(this.activationTag);
  }

  override async onDeactivate(): Promise<void> {
    const watcher = this.getGrain(IActivateDeactivateWatcherGrain, 0n);
    await watcher.recordDeactivateCall(this.activationTag);
  }

  async doSomething(): Promise<string> {
    return this.activationTag;
  }

  async doDeactivate(): Promise<void> {
    this.runtime.deactivateOnIdle();
  }
}

// Upstream spawns a sub-task on the default .NET thread-pool scheduler during
// OnActivateAsync and sleeps for a second in both lifecycle hooks, asserting
// the sub-task still observes the activation in progress; this framework has a
// single-threaded turn scheduler with no background thread pool to spawn onto
// and no need for a real sleep to observe that invariant, so this grain keeps
// the watcher bookkeeping and drops the .NET-scheduler-specific assertions and
// the real-time delays.
@grain({ name: "UnitTests.Grains.LongRunningActivateDeactivateTestGrain" })
export class LongRunningActivateDeactivateTestGrain
  extends Grain
  implements ILongRunningActivateDeactivateTestGrain
{
  private activationTag = "";

  override async onActivate(): Promise<void> {
    this.activationTag = Guid.newGuid().toString();
    const watcher = this.getGrain(IActivateDeactivateWatcherGrain, 0n);
    await watcher.recordActivateCall(this.activationTag);
  }

  override async onDeactivate(): Promise<void> {
    const watcher = this.getGrain(IActivateDeactivateWatcherGrain, 0n);
    await watcher.recordDeactivateCall(this.activationTag);
  }

  async doSomething(): Promise<string> {
    return this.activationTag;
  }

  async doDeactivate(): Promise<void> {
    this.runtime.deactivateOnIdle();
  }
}

// Upstream's variant runs the lifecycle hooks through nested Task/ContinueWith
// scheduling to exercise the .NET TaskScheduler; that mechanism has no JS
// analogue (there is one microtask queue, not a pluggable scheduler), so the
// observable activate/deactivate behaviour mirrors the other grains here.
@grain({ name: "UnitTests.Grains.TaskActionActivateDeactivateTestGrain" })
export class TaskActionActivateDeactivateTestGrain
  extends Grain
  implements ITaskActionActivateDeactivateTestGrain
{
  private activationTag = "";

  override async onActivate(): Promise<void> {
    this.activationTag = Guid.newGuid().toString();
    const watcher = this.getGrain(IActivateDeactivateWatcherGrain, 0n);
    await watcher.recordActivateCall(this.activationTag);
  }

  override async onDeactivate(): Promise<void> {
    const watcher = this.getGrain(IActivateDeactivateWatcherGrain, 0n);
    await watcher.recordDeactivateCall(this.activationTag);
  }

  async doSomething(): Promise<string> {
    return this.activationTag;
  }

  async doDeactivate(): Promise<void> {
    this.runtime.deactivateOnIdle();
  }
}

@grain({ name: "UnitTests.Grains.BadActivateDeactivateTestGrain" })
export class BadActivateDeactivateTestGrain
  extends Grain
  implements IBadActivateDeactivateTestGrain
{
  override async onActivate(): Promise<void> {
    throw new Error("Thrown from Application-OnActivateAsync");
  }

  override async onDeactivate(): Promise<void> {
    throw new Error("Thrown from Application-OnDeactivateAsync");
  }

  async throwSomething(): Promise<void> {
    throw new Error("Exception should have been thrown from Activate");
  }

  async getKey(): Promise<bigint> {
    throw new Error("Exception should have been thrown from Activate");
  }
}

@grain({ name: "UnitTests.Grains.BadConstructorTestGrain" })
export class BadConstructorTestGrain extends Grain implements IBadConstructorTestGrain {
  constructor() {
    super();
    throw new Error("Thrown from Constructor");
  }

  override async onActivate(): Promise<void> {
    throw new Error("onActivate should not have been called");
  }

  override async onDeactivate(): Promise<void> {
    throw new Error("onDeactivate should not have been called");
  }

  async doSomething(): Promise<string> {
    throw new Error("doSomething should not have been called");
  }
}

@grain({ name: "UnitTests.Grains.DeactivatingWhileActivatingTestGrain" })
export class DeactivatingWhileActivatingTestGrain
  extends Grain
  implements IDeactivatingWhileActivatingTestGrain
{
  override async onActivate(): Promise<void> {
    this.runtime.deactivateOnIdle();
  }

  async doSomething(): Promise<string> {
    throw new Error("doSomething should not have been called");
  }
}

@grain({ name: "UnitTests.Grains.CreateGrainReferenceTestGrain" })
export class CreateGrainReferenceTestGrain extends Grain implements ICreateGrainReferenceTestGrain {
  // Upstream creates this reference from the grain *constructor*, to show that
  // grain-reference creation is allowed that early; this framework binds a
  // grain's runtime context only once construction completes (`setContext`
  // runs after `new ctor()`), so the earliest a grain can call `getGrain` is
  // `onActivate` — the adapted equivalent of Orleans' constructor phase here.
  private grainRef!: ITestGrain;

  override async onActivate(): Promise<void> {
    this.grainRef = this.getGrain(ITestGrain, 1n);
  }

  async doSomething(): Promise<string> {
    const guid = Guid.newGuid().toString();
    await this.grainRef.setLabel(guid);
    const label = await this.grainRef.getLabel();
    if (label === "") throw new Error("Bad data: Null label returned");
    return Guid.newGuid().toString();
  }

  async forwardCall(otherGrain: IBadActivateDeactivateTestGrain): Promise<void> {
    await otherGrain.throwSomething();
  }
}
