import { beforeEach, describe, expect, it } from "vitest";
import { grain } from "@thresh/core/decorators";
import { InconsistentStateError } from "@thresh/core/errors";
import { Grain } from "@thresh/core/grain";
import { defineGrainInterface } from "@thresh/core/grain-interface";
import type { GrainKey } from "@thresh/core/key-kinds";
import type { DeactivationReason } from "@thresh/core/reasons";
import { Silo } from "@thresh/runtime/silo";
import { FakeTimeProvider } from "@thresh/runtime/test-support/fake-time-provider";

// Orleans deactivates an activation that lets an `InconsistentStateException`
// escape, so the next call gets a fresh activation (which re-reads state)
// rather than a stale one stuck with a mismatched etag — but only the
// activation it ORIGINATED in: `InsideRuntimeClient` checks
// `IsSourceActivation` and clears it before the exception travels on
// (InsideRuntimeClient.cs:326-329), so a caller that merely propagates it is
// left alone.

const deactivations: Array<{ key: string; code: string }> = [];
let instances = 0;

interface IStaleWriter extends GrainKey<string> {
  write(): Promise<void>;
  instance(): Promise<number>;
  startFailingTimer(): Promise<void>;
}
const IStaleWriter = defineGrainInterface<IStaleWriter>("IStaleWriter");

interface IForwarder extends GrainKey<string> {
  forward(target: string): Promise<void>;
  instance(): Promise<number>;
}
const IForwarder = defineGrainInterface<IForwarder>("IForwarder");

@grain()
class StaleWriterGrain extends Grain implements IStaleWriter {
  private readonly serial = ++instances;

  async write(): Promise<void> {
    throw new InconsistentStateError("etag mismatch", "e1", "e2");
  }
  async instance(): Promise<number> {
    return this.serial;
  }
  async startFailingTimer(): Promise<void> {
    this.runtime.registerTimer(
      async () => {
        throw new InconsistentStateError("etag mismatch", "e1", "e2");
      },
      { ms: 10 },
    );
  }
  override async onDeactivate(reason: DeactivationReason): Promise<void> {
    // Not instantaneous, like real cleanup: a call arriving meanwhile must
    // still reach a fresh activation, not this one mid-teardown.
    await new Promise((r) => setTimeout(r, 5));
    deactivations.push({ key: `writer/${this.id.key}`, code: reason.code });
  }
}

@grain()
class ForwarderGrain extends Grain implements IForwarder {
  private readonly serial = ++instances;

  async forward(target: string): Promise<void> {
    await this.getGrain(IStaleWriter, target).write();
  }
  async instance(): Promise<number> {
    return this.serial;
  }
  override async onDeactivate(reason: DeactivationReason): Promise<void> {
    deactivations.push({ key: `forwarder/${this.id.key}`, code: reason.code });
  }
}

const flush = () => new Promise((r) => setTimeout(r, 0));

describe("escaped InconsistentStateError", () => {
  let silo: Silo;
  let time: FakeTimeProvider;

  beforeEach(() => {
    deactivations.length = 0;
    time = new FakeTimeProvider();
    silo = new Silo({
      time,
      defaultCollectionAgeSeconds: 100_000,
      collectionIntervalSeconds: 100_000,
    });
    silo.registerGrain(StaleWriterGrain, { interfaces: [IStaleWriter] });
    silo.registerGrain(ForwarderGrain, { interfaces: [IForwarder] });
    silo.start();
  });

  it("deactivates the activation it originated in, so the next call gets a fresh one", async () => {
    const writer = silo.getGrain(IStaleWriter, "w1");
    const before = await writer.instance();

    await expect(writer.write()).rejects.toBeInstanceOf(InconsistentStateError);

    // The very next call is served — by a fresh activation — rather than
    // rejected as "activation unavailable" by the one being torn down.
    const after = await writer.instance();
    expect(after).not.toBe(before);
    expect(deactivations).toEqual([{ key: "writer/w1", code: "application-error" }]);
  });

  it("does not deactivate a caller that only propagates it", async () => {
    const forwarder = silo.getGrain(IForwarder, "f1");
    const before = await forwarder.instance();

    await expect(forwarder.forward("w2")).rejects.toBeInstanceOf(InconsistentStateError);

    expect(await forwarder.instance()).toBe(before);
    await silo.getGrain(IStaleWriter, "w2").instance(); // let the writer's deactivation land
    expect(deactivations).toEqual([{ key: "writer/w2", code: "application-error" }]);
  });

  it("deactivates the activation when it escapes a timer tick", async () => {
    // A tick is a message invoked through the same `InsideRuntimeClient.Invoke`
    // path in Orleans, so the same rule applies to it.
    const writer = silo.getGrain(IStaleWriter, "t1");
    const before = await writer.instance();
    await writer.startFailingTimer();
    time.advance(10);
    await flush();

    expect(await writer.instance()).not.toBe(before);
    expect(deactivations).toEqual([{ key: "writer/t1", code: "application-error" }]);
  });
});
