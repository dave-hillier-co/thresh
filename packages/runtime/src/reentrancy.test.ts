import { beforeEach, describe, expect, it } from "vitest";
import { grain } from "@tsva/core/decorators";
import { defineGrainInterface } from "@tsva/core/grain-interface";
import { Grain } from "@tsva/core/grain";
import type { GrainWithStringKey } from "@tsva/core/key-kinds";
import { Silo } from "@tsva/runtime/silo";

interface IGate extends GrainWithStringKey {
  enter(tag: string): Promise<void>;
}
interface IReentrantA extends GrainWithStringKey {
  run(): Promise<string>;
  ping(): Promise<string>;
}
interface IReentrantB extends GrainWithStringKey {
  callViaA(aKey: string): Promise<string>;
}

const IGate = defineGrainInterface<IGate>("IGate");
const IReentrantA = defineGrainInterface<IReentrantA>("IReentrantA");
const IReentrantB = defineGrainInterface<IReentrantB>("IReentrantB");

let events: string[] = [];
let gate: { promise: Promise<void>; resolve: () => void } | undefined;

@grain()
class GateGrain extends Grain implements IGate {
  async enter(tag: string): Promise<void> {
    events.push(`enter:${tag}`);
    if (gate !== undefined) await gate.promise;
    events.push(`exit:${tag}`);
  }
}

@grain()
class ReentrantAGrain extends Grain implements IReentrantA {
  async run(): Promise<string> {
    const b = this.getGrain(IReentrantB, String(this.id.key));
    const echo = await b.callViaA(String(this.id.key));
    return `run->${echo}`;
  }
  async ping(): Promise<string> {
    return "pong";
  }
}

@grain()
class ReentrantBGrain extends Grain implements IReentrantB {
  async callViaA(aKey: string): Promise<string> {
    // Re-enters grain A while A.run is still awaiting this call: only the shared
    // call-chain reentrancy id keeps this from deadlocking.
    return await this.getGrain(IReentrantA, aKey).ping();
  }
}

describe("call-chain reentrancy", () => {
  let silo: Silo;

  beforeEach(() => {
    events = [];
    gate = undefined;
    silo = new Silo();
    silo
      .registerGrain(GateGrain, { interfaces: [IGate] })
      .registerGrain(ReentrantAGrain, { interfaces: [IReentrantA] })
      .registerGrain(ReentrantBGrain, { interfaces: [IReentrantB] });
    silo.start();
  });

  it("admits a re-entrant call back into a grain on the same call chain", async () => {
    const result = await silo.getGrain(IReentrantA, "x").run();
    expect(result).toBe("run->pong");
  });

  it("serializes two independent call chains to the same grain (default queues)", async () => {
    let resolveGate!: () => void;
    gate = { promise: new Promise<void>((r) => (resolveGate = r)), resolve: () => resolveGate() };

    const g = silo.getGrain(IGate, "k");
    const first = g.enter("1");
    const second = g.enter("2");

    await new Promise((r) => setTimeout(r, 0));
    // Second call is queued behind the running exclusive turn.
    expect(events).toEqual(["enter:1"]);

    resolveGate();
    await Promise.all([first, second]);
    expect(events).toEqual(["enter:1", "exit:1", "enter:2", "exit:2"]);
  });
});
