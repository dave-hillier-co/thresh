import { inspect } from "node:util";
import { describe, expect, it } from "vitest";
import { grain } from "@thresh/core/decorators";
import { defineGrain, useOnActivate, useOnDeactivate } from "@thresh/core/define-grain";
import { Grain } from "@thresh/core/grain";
import { defineGrainInterface } from "@thresh/core/grain-interface";
import type { GrainWithStringKey } from "@thresh/core/key-kinds";
import { JournaledGrain } from "@thresh/core/journaled-grain";
import { TestCluster } from "@thresh/testing/test-cluster";

interface ICounter extends GrainWithStringKey {
  inc(): Promise<number>;
}

const log: string[] = [];

const FunctionalCounter = defineGrain<ICounter>("SurfaceFunctionalCounter", () => {
  let n = 0;
  useOnActivate(() => {
    log.push("functional-activate");
  });
  useOnDeactivate(() => {
    log.push("functional-deactivate");
  });
  return { inc: async () => ++n };
});

@grain({ name: "SurfaceClassCounter" })
class ClassCounter extends Grain implements ICounter {
  private n = 0;
  async inc(): Promise<number> {
    return ++this.n;
  }
  override async onActivate(): Promise<void> {
    log.push("class-activate");
  }
  override async onDeactivate(): Promise<void> {
    log.push("class-deactivate");
  }
}
const IClassCounter = defineGrainInterface<ICounter>("SurfaceClassCounter");

abstract class UserBase extends Grain {
  async fromBase(): Promise<string> {
    return "base";
  }
}

@grain({ name: "SurfaceInherit" })
class InheritGrain extends UserBase {
  readonly arrow = async (): Promise<string> => "arrow";
  async own(): Promise<string> {
    return "own";
  }
}
interface IInherit extends GrainWithStringKey {
  fromBase(): Promise<string>;
  arrow(): Promise<string>;
  own(): Promise<string>;
}
const IInherit = defineGrainInterface<IInherit>("SurfaceInherit");

@grain({ name: "SurfaceJournal" })
class JournalGrain extends JournaledGrain<number, number> {
  initialState(): number {
    return 0;
  }
  transitionState(state: number, event: number): number {
    return state + event;
  }
  async add(n: number): Promise<number> {
    this.raiseEvent(n);
    await this.confirmEvents();
    return this.state;
  }
}
interface IJournal extends GrainWithStringKey {
  add(n: number): Promise<number>;
}
const IJournal = defineGrainInterface<IJournal>("SurfaceJournal");

type Loose = Record<string, (...args: unknown[]) => Promise<unknown>>;

const BLOCKED = [
  "onDeactivate",
  "onActivate",
  "setContext",
  "onDehydrate",
  "onRehydrate",
  "hasOwnProperty",
  "isPrototypeOf",
  "__defineGetter__",
] as const;

async function expectNoMethod(call: Promise<unknown>): Promise<void> {
  await expect(call).rejects.toThrow(/has no method/);
}

describe("remote method surface", () => {
  const grains = [
    FunctionalCounter,
    { ctor: ClassCounter, interfaces: [IClassCounter] },
    { ctor: InheritGrain, interfaces: [IInherit] },
    { ctor: JournalGrain, interfaces: [IJournal] },
  ];

  for (const [label, def, firstCall] of [
    ["functional grain", FunctionalCounter, "functional"],
    ["class grain", IClassCounter, "class"],
  ] as const) {
    it(`rejects runtime hooks and Object.prototype names on a ${label}, silo-side and from a gateway client`, async () => {
      log.length = 0;
      const cluster = await TestCluster.start({ initialSilos: 2, grains });
      try {
        const client = await cluster.client;
        const sides = [
          cluster.getGrain(def, "k") as unknown as Loose,
          client.getGrain(def, "k2") as unknown as Loose,
        ];
        for (const side of sides) {
          expect(await (side as unknown as ICounter).inc()).toBe(1);
          const activations = log.length;
          for (const name of BLOCKED) {
            await expectNoMethod(side[name]!("x"));
          }
          // The hooks did not run and state was not reset.
          expect(log.length).toBe(activations);
          expect(await (side as unknown as ICounter).inc()).toBe(2);
        }
        expect(firstCall).toBeDefined();
      } finally {
        await cluster.dispose();
      }
    });
  }

  it("keeps methods from a user base class and class-field arrow functions callable", async () => {
    const cluster = await TestCluster.start({ initialSilos: 1, grains });
    try {
      const client = await cluster.client;
      for (const ref of [cluster.getGrain(IInherit, "a"), client.getGrain(IInherit, "b")]) {
        expect(await ref.fromBase()).toBe("base");
        expect(await ref.arrow()).toBe("arrow");
        expect(await ref.own()).toBe("own");
      }
    } finally {
      await cluster.dispose();
    }
  });

  it("does not expose JournaledGrain members, but keeps the subclass's own methods", async () => {
    const cluster = await TestCluster.start({ initialSilos: 1, grains });
    try {
      const client = await cluster.client;
      for (const ref of [cluster.getGrain(IJournal, "a"), client.getGrain(IJournal, "b")]) {
        expect(await ref.add(2)).toBe(2);
        const loose = ref as unknown as Loose;
        for (const name of [
          "raiseEvent",
          "clearLog",
          "confirmEvents",
          "installLogViewAdaptor",
          "retrieveConfirmedEvents",
        ]) {
          await expectNoMethod(loose[name]!(1));
        }
        expect(await ref.add(3)).toBe(5);
      }
    } finally {
      await cluster.dispose();
    }
  });

  it("refuses an Object.prototype method on a client-hosted observer", async () => {
    interface IObserver extends GrainWithStringKey {
      ping(): Promise<string>;
    }
    const IObserver = defineGrainInterface<IObserver>("SurfaceObserver");
    const cluster = await TestCluster.start({ initialSilos: 1, grains });
    try {
      const client = await cluster.client;
      const ref = client.createObjectReference(IObserver, { ping: async () => "pong" });
      expect(await ref.ping()).toBe("pong");
      await expectNoMethod((ref as unknown as Loose)["hasOwnProperty"]!("ping"));
    } finally {
      await cluster.dispose();
    }
  });
});

describe("grain reference protocol names", () => {
  it("never turns toJSON, toString, valueOf or inspect into a remote call", async () => {
    const unhandled: unknown[] = [];
    const onUnhandled = (reason: unknown): void => {
      unhandled.push(reason);
    };
    process.on("unhandledRejection", onUnhandled);
    const cluster = await TestCluster.start({ initialSilos: 1, grains: [FunctionalCounter] });
    try {
      const ref = cluster.getGrain(FunctionalCounter, "a");
      const loose = ref as unknown as Record<string, unknown>;
      expect(JSON.stringify({ grain: ref })).toBe('{"grain":{}}');
      expect(String(ref)).toContain("SurfaceFunctionalCounter");
      expect(String(ref)).toContain("a");
      expect(`${ref as unknown as string}`).toContain("SurfaceFunctionalCounter");
      expect(typeof inspect(ref)).toBe("string");
      expect(loose["valueOf"]).toBeUndefined();
      expect(loose["toJSON"]).toBeUndefined();
      expect(loose["asymmetricMatch"]).toBeUndefined();
      expect(loose["nodeType"]).toBeUndefined();
      expect(loose["$$typeof"]).toBeUndefined();
      expect("toJSON" in ref).toBe(false);
      expect("inc" in ref).toBe(true);
      await new Promise((r) => setTimeout(r, 50));
      expect(unhandled).toEqual([]);
    } finally {
      process.off("unhandledRejection", onUnhandled);
      await cluster.dispose();
    }
  });
});
