import { describe, expect, it } from "vitest";
import {
  defineGrain,
  useDependency,
  useGrainFacet,
  useMayInterleave,
  useOnDehydrate,
  useOnRehydrate,
} from "@thresh/core/define-grain";
import { createDependencyToken } from "@thresh/core/grain-dependency";
import { GrainId } from "@thresh/core/grain-id";
import type { GrainRuntime } from "@thresh/core/grain-runtime";
import { isMigrationParticipant } from "@thresh/core/grain-migration-participant";
import { isRemotelyCallable } from "@thresh/core/remote-method-surface";

const TOKEN = createDependencyToken<string>("t");

describe("the authoring hooks follow the rules of hooks", () => {
  const hooks: ReadonlyArray<readonly [string, () => unknown]> = [
    ["useMayInterleave", () => useMayInterleave(() => true)],
    ["useGrainFacet", () => useGrainFacet("kind")],
    ["useOnDehydrate", () => useOnDehydrate(() => {})],
    ["useOnRehydrate", () => useOnRehydrate(() => {})],
    ["useDependency", () => useDependency(TOKEN)],
  ];

  for (const [name, call] of hooks) {
    it(`${name} throws outside a factory`, () => {
      expect(call).toThrow(new RegExp(`^${name}\\(\\) may only be called synchronously`));
    });

    it(`${name} throws when called after an await in a factory`, async () => {
      let late: unknown;
      const Late = defineGrain(`Late-${name}`, () => {
        // The factory is synchronous; this continuation runs after setup ended.
        void Promise.resolve().then(() => {
          try {
            call();
          } catch (error) {
            late = error;
          }
        });
        return {};
      }).grain;
      const instance = new Late();
      instance.setContext({
        id: new GrainId("Late", "a"),
        runtime: { getDependency: () => "x" } as unknown as GrainRuntime,
      });
      await Promise.resolve();
      await Promise.resolve();
      expect((late as Error).message).toMatch(new RegExp(`^${name}\\(\\) may only be called`));
    });
  }
});

describe("migration hooks", () => {
  const context = {
    id: new GrainId("Mig", "a"),
    runtime: {} as GrainRuntime,
  };

  it("make the activation a participant without putting the hooks on the call surface", () => {
    const Mig = defineGrain("MigSurface", () => {
      useOnDehydrate(() => {});
      useOnRehydrate(() => {});
      return { ping: async () => "pong" };
    }).grain;
    const instance = new Mig();
    instance.setContext(context);

    expect(isMigrationParticipant(instance)).toBe(true);
    expect(isRemotelyCallable(instance, "ping")).toBe(true);
    expect(isRemotelyCallable(instance, "onDehydrate")).toBe(false);
    expect(isRemotelyCallable(instance, "onRehydrate")).toBe(false);
  });

  it("leave a grain that registers none outside the participant set", () => {
    const Plain = defineGrain("MigNone", () => ({ ping: async () => "pong" })).grain;
    const instance = new Plain();
    instance.setContext(context);
    expect(isMigrationParticipant(instance)).toBe(false);
  });

  it("run every registered hook in registration order", () => {
    const seen: string[] = [];
    const Mig = defineGrain("MigOrder", () => {
      useOnDehydrate((bag) => {
        seen.push("d1");
        bag.set("a", 1);
      });
      useOnDehydrate(() => seen.push("d2"));
      useOnRehydrate((bag) => seen.push(`r:${String(bag.get("a"))}`));
      return {};
    }).grain;
    const instance = new Mig();
    instance.setContext(context);
    const bag = new Map<string, unknown>();
    const participant = instance as unknown as {
      onDehydrate(c: { set(k: string, v: unknown): void }): void;
      onRehydrate(c: { get(k: string): unknown }): void;
    };
    participant.onDehydrate({ set: (k, v) => bag.set(k, v) });
    participant.onRehydrate({ get: (k) => bag.get(k) });
    expect(seen).toEqual(["d1", "d2", "r:1"]);
  });

  it("refuse a migration hook returned in the message surface", () => {
    const Bad = defineGrain("MigReturned", () => ({ onDehydrate: () => {} })).grain;
    expect(() => new Bad().setContext(context)).toThrow(/useOnDehydrate \/ useOnRehydrate/);
  });
});
