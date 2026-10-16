import { beforeEach, describe, expect, it, vi } from "vitest";
import { grain, reentrant } from "@thresh/core/decorators";
import { GrainCallTimeoutError } from "@thresh/core/errors";
import { defineGrainInterface } from "@thresh/core/grain-interface";
import { Grain } from "@thresh/core/grain";
import type { GrainKey } from "@thresh/core/key-kinds";
import { RequestContext } from "@thresh/core/request-context";
import { Silo } from "@thresh/runtime/silo";

/**
 * Call-chain reentrancy (Orleans `RequestContext.AllowCallChainReentrancy`).
 * It is OFF by default, as in Orleans: a non-reentrant grain that calls back
 * into itself — directly, or through another grain — deadlocks until the
 * call's response timeout (`DisabledCallChainReentrancyTestRunner.NonReentrantGrain`).
 * A grain opts a call chain in by issuing its calls inside
 * `RequestContext.allowCallChainReentrancy`; only the grain that opted in
 * admits the callback (`CallChainReentrancyTestHelper.CallChainReentrancy_WithSuppression`).
 *
 * The response timeout on the re-entering methods is deliberately short: the
 * deadlock IS the behaviour under test, and the timeout is how it surfaces.
 */
const SHORT = { responseTimeout: { ms: 150 } } as const;

interface IChainA extends GrainKey<string> {
  /** Calls itself, plainly. */
  selfCall(): Promise<string>;
  /** Calls itself inside an allow section. */
  selfCallAllowing(): Promise<string>;
  /** A -> B -> A, plainly. */
  viaB(): Promise<string>;
  /** A -> B -> A, inside an allow section on A. */
  viaBAllowing(): Promise<string>;
  /** A -> B -> A -> B, allowing on A only: the second entry into B is not admitted. */
  viaBTwiceAllowingOnAOnly(): Promise<string>;
  /** A -> B -> A, allowing, but suppressing on the hop to B. */
  viaBAllowingThenSuppressing(): Promise<string>;
  /** Allows for one self-call, then self-calls again after the section closed. */
  selfCallAfterSectionClosed(): Promise<string>;
  /** Re-entered from B; optionally calls B back again. */
  ping(callBackToB: boolean): Promise<string>;
}
interface IChainB extends GrainKey<string> {
  callA(aKey: string, callBackToB: boolean): Promise<string>;
  pong(): Promise<string>;
}

const IChainA = defineGrainInterface<IChainA>("IChainA.reentrancy", {
  options: { ping: SHORT, selfCall: SHORT },
});
const IChainB = defineGrainInterface<IChainB>("IChainB.reentrancy", {
  options: { pong: SHORT },
});

let events: string[] = [];

@grain()
class ChainAGrain extends Grain implements IChainA {
  private get self(): IChainA {
    return this.getGrain(IChainA, String(this.id.key));
  }
  private get b(): IChainB {
    return this.getGrain(IChainB, String(this.id.key));
  }
  async selfCall(): Promise<string> {
    events.push("selfCall:enter");
    try {
      return `selfCall->${await this.self.ping(false)}`;
    } finally {
      events.push("selfCall:exit");
    }
  }
  async selfCallAllowing(): Promise<string> {
    const inner = await RequestContext.allowCallChainReentrancy(() => this.self.ping(false));
    return `selfCallAllowing->${inner}`;
  }
  async viaB(): Promise<string> {
    events.push("viaB:enter");
    try {
      return `viaB->${await this.b.callA(String(this.id.key), false)}`;
    } finally {
      events.push("viaB:exit");
    }
  }
  async viaBAllowing(): Promise<string> {
    const inner = await RequestContext.allowCallChainReentrancy(() =>
      this.b.callA(String(this.id.key), false),
    );
    return `viaBAllowing->${inner}`;
  }
  async viaBTwiceAllowingOnAOnly(): Promise<string> {
    return RequestContext.allowCallChainReentrancy(() => this.b.callA(String(this.id.key), true));
  }
  async viaBAllowingThenSuppressing(): Promise<string> {
    return RequestContext.allowCallChainReentrancy(() =>
      RequestContext.suppressCallChainReentrancy(() => this.b.callA(String(this.id.key), false)),
    );
  }
  async selfCallAfterSectionClosed(): Promise<string> {
    const first = await RequestContext.allowCallChainReentrancy(() => this.self.ping(false));
    const second = await this.self.ping(false);
    return `${first},${second}`;
  }
  async ping(callBackToB: boolean): Promise<string> {
    events.push("ping");
    if (callBackToB) return `ping->${await this.b.pong()}`;
    return "ping";
  }
}

@grain()
class ChainBGrain extends Grain implements IChainB {
  async callA(aKey: string, callBackToB: boolean): Promise<string> {
    return `callA->${await this.getGrain(IChainA, aKey).ping(callBackToB)}`;
  }
  async pong(): Promise<string> {
    return "pong";
  }
}

interface IReentrantSelf extends GrainKey<string> {
  selfCall(): Promise<string>;
  inner(): Promise<string>;
}
const IReentrantSelf = defineGrainInterface<IReentrantSelf>("IReentrantSelf.reentrancy", {
  options: { inner: SHORT },
});

@grain()
@reentrant()
class ReentrantSelfGrain extends Grain implements IReentrantSelf {
  async selfCall(): Promise<string> {
    return `selfCall->${await this.getGrain(IReentrantSelf, String(this.id.key)).inner()}`;
  }
  async inner(): Promise<string> {
    return "inner";
  }
}

interface IInterleaveSelf extends GrainKey<string> {
  selfCall(): Promise<string>;
  read(): Promise<string>;
}
const IInterleaveSelf = defineGrainInterface<IInterleaveSelf>("IInterleaveSelf.reentrancy", {
  options: { read: { alwaysInterleave: true, ...SHORT } },
});

@grain()
class InterleaveSelfGrain extends Grain implements IInterleaveSelf {
  async selfCall(): Promise<string> {
    return `selfCall->${await this.getGrain(IInterleaveSelf, String(this.id.key)).read()}`;
  }
  async read(): Promise<string> {
    return "read";
  }
}

interface IGate extends GrainKey<string> {
  enter(tag: string): Promise<void>;
}
const IGate = defineGrainInterface<IGate>("IGate.reentrancy");
let gate: { promise: Promise<void>; resolve: () => void } | undefined;

@grain()
class GateGrain extends Grain implements IGate {
  async enter(tag: string): Promise<void> {
    events.push(`enter:${tag}`);
    if (gate !== undefined) await gate.promise;
    events.push(`exit:${tag}`);
  }
}

describe("call-chain reentrancy", () => {
  let silo: Silo;

  beforeEach(() => {
    events = [];
    gate = undefined;
    silo = new Silo();
    silo
      .registerGrain(ChainAGrain, { interfaces: [IChainA] })
      .registerGrain(ChainBGrain, { interfaces: [IChainB] })
      .registerGrain(ReentrantSelfGrain, { interfaces: [IReentrantSelf] })
      .registerGrain(InterleaveSelfGrain, { interfaces: [IInterleaveSelf] })
      .registerGrain(GateGrain, { interfaces: [IGate] });
    silo.start();
  });

  describe("off by default (Orleans DisabledCallChainReentrancyTestRunner)", () => {
    it("a non-reentrant grain's self-call does not interleave: it deadlocks until its response timeout", async () => {
      await expect(silo.getGrain(IChainA, "self").selfCall()).rejects.toBeInstanceOf(
        GrainCallTimeoutError,
      );
      // The self-call never ran inside the outer turn: it was queued behind
      // it, and runs only once the outer turn has ended.
      await vi.waitFor(() => expect(events).toContain("ping"));
      expect(events).toEqual(["selfCall:enter", "selfCall:exit", "ping"]);
    });

    it("A -> B -> A does not interleave on A: the callback deadlocks until its response timeout", async () => {
      await expect(silo.getGrain(IChainA, "aba").viaB()).rejects.toBeInstanceOf(
        GrainCallTimeoutError,
      );
      await vi.waitFor(() => expect(events).toContain("ping"));
      expect(events).toEqual(["viaB:enter", "viaB:exit", "ping"]);
    });
  });

  describe("RequestContext.allowCallChainReentrancy (Orleans AllowCallChainReentrancy)", () => {
    it("admits a self-call issued inside the section", async () => {
      await expect(silo.getGrain(IChainA, "self-allow").selfCallAllowing()).resolves.toBe(
        "selfCallAllowing->ping",
      );
    });

    it("admits A -> B -> A when A opted the chain in", async () => {
      await expect(silo.getGrain(IChainA, "aba-allow").viaBAllowing()).resolves.toBe(
        "viaBAllowing->callA->ping",
      );
    });

    it("is scoped to the grain that opted in: A -> B -> A -> B still deadlocks on B", async () => {
      await expect(
        silo.getGrain(IChainA, "abab").viaBTwiceAllowingOnAOnly(),
      ).rejects.toBeInstanceOf(GrainCallTimeoutError);
    });

    it("is scoped to the callback: a self-call after the section closed deadlocks", async () => {
      await expect(
        silo.getGrain(IChainA, "closed").selfCallAfterSectionClosed(),
      ).rejects.toBeInstanceOf(GrainCallTimeoutError);
    });

    it("exposes the chain's id while the section is open, and none outside it", async () => {
      expect(RequestContext.reentrancyId).toBeUndefined();
      const inside = await RequestContext.allowCallChainReentrancy(async () => {
        const id = RequestContext.reentrancyId;
        // A nested allow keeps the existing chain id (Orleans reuses a non-empty one).
        const nested = await RequestContext.allowCallChainReentrancy(async () =>
          Promise.resolve(RequestContext.reentrancyId),
        );
        return { id, nested };
      });
      expect(inside.id).toBeTypeOf("string");
      expect(inside.nested).toBe(inside.id);
      expect(RequestContext.reentrancyId).toBeUndefined();
    });
  });

  describe("RequestContext.suppressCallChainReentrancy (Orleans SuppressCallChainReentrancy)", () => {
    it("drops the chain id for calls inside it, so the callback into A deadlocks again", async () => {
      await expect(
        silo.getGrain(IChainA, "suppress").viaBAllowingThenSuppressing(),
      ).rejects.toBeInstanceOf(GrainCallTimeoutError);
    });

    it("clears the ambient id inside the callback only", async () => {
      await RequestContext.allowCallChainReentrancy(async () => {
        const id = RequestContext.reentrancyId;
        await RequestContext.suppressCallChainReentrancy(async () => {
          expect(RequestContext.reentrancyId).toBeUndefined();
          return Promise.resolve();
        });
        expect(RequestContext.reentrancyId).toBe(id);
      });
    });
  });

  describe("unchanged by the default", () => {
    it("a [Reentrant] grain's self-call interleaves without opting in", async () => {
      await expect(silo.getGrain(IReentrantSelf, "r").selfCall()).resolves.toBe("selfCall->inner");
    });

    it("an alwaysInterleave method interleaves with a self-call without opting in", async () => {
      await expect(silo.getGrain(IInterleaveSelf, "i").selfCall()).resolves.toBe("selfCall->read");
    });

    it("serializes two independent call chains to the same grain", async () => {
      let resolveGate!: () => void;
      gate = { promise: new Promise<void>((r) => (resolveGate = r)), resolve: () => resolveGate() };

      const g = silo.getGrain(IGate, "k");
      const first = g.enter("1");
      const second = g.enter("2");

      await new Promise((r) => setTimeout(r, 0));
      expect(events).toEqual(["enter:1"]);

      resolveGate();
      await Promise.all([first, second]);
      expect(events).toEqual(["enter:1", "exit:1", "enter:2", "exit:2"]);
    });
  });
});
