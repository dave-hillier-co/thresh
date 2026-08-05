import { describe, expect, it } from "vitest";
import { grain } from "@thresh/core/decorators";
import { Grain } from "@thresh/core/grain";
import { GrainId } from "@thresh/core/grain-id";
import { defineGrainInterface } from "@thresh/core/grain-interface";
import { castGrainReference } from "@thresh/core/grain-reference";
import type { GrainKey } from "@thresh/core/key-kinds";
import { IGrainManagementExtension } from "@thresh/runtime/grain-management-extension";
import { Silo } from "@thresh/runtime/silo";
import { FakeTimeProvider } from "@thresh/runtime/test-support/fake-time-provider";

interface ITagged extends GrainKey<string> {
  tag(): Promise<string>;
}

const ITagged = defineGrainInterface<ITagged>("ITagged.management-extension-test");

let events: string[] = [];

@grain()
class TaggedGrain extends Grain implements ITagged {
  private readonly instanceTag = Math.random().toString(36).slice(2);

  override async onActivate(): Promise<void> {
    events.push(`activate:${String(this.id.key)}`);
  }

  override async onDeactivate(): Promise<void> {
    events.push(`deactivate:${String(this.id.key)}`);
  }

  async tag(): Promise<string> {
    return this.instanceTag;
  }
}

const flush = () => new Promise((r) => setTimeout(r, 0));
const taggedId = (key: string) => new GrainId("Tagged", key);

describe("IGrainManagementExtension.deactivateOnIdle (auto-installed)", () => {
  it("forces a fresh activation on the very next call, race-free, and still runs onDeactivate", async () => {
    events = [];
    const time = new FakeTimeProvider();
    const silo = new Silo({
      time,
      defaultCollectionAgeSeconds: 900,
      collectionIntervalSeconds: 10,
    });
    silo.registerGrain(TaggedGrain, { interfaces: [ITagged] });
    silo.start();

    const ref = silo.getGrain(ITagged, "a");
    const tagBefore = await ref.tag();
    expect(silo.isActive(taggedId("a"))).toBe(true);

    const ext = castGrainReference(ref, IGrainManagementExtension);
    await ext.deactivateOnIdle();

    // Immediately, race-free: the very next call must hit a fresh activation.
    const tagAfter = await ref.tag();
    expect(tagAfter).not.toBe(tagBefore);
    expect(events).toEqual(["activate:a", "deactivate:a", "activate:a"]);
  });

  it("does not affect an activation that never asked to deactivate", async () => {
    events = [];
    const time = new FakeTimeProvider();
    const silo = new Silo({
      time,
      defaultCollectionAgeSeconds: 900,
      collectionIntervalSeconds: 10,
    });
    silo.registerGrain(TaggedGrain, { interfaces: [ITagged] });
    silo.start();

    const ref = silo.getGrain(ITagged, "b");
    const tagBefore = await ref.tag();
    const tagAfter = await ref.tag();
    expect(tagAfter).toBe(tagBefore);
    expect(events).toEqual(["activate:b"]);
    await flush();
  });
});
