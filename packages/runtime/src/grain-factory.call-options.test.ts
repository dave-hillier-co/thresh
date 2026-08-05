import { describe, expect, it } from "vitest";
import { defineGrainInterface } from "@thresh/core/grain-interface";
import type { GrainKey } from "@thresh/core/key-kinds";
import type { InvocationRequest } from "@thresh/core/request";
import type { Dispatcher, InvokeCallOptions } from "@thresh/runtime/dispatcher";
import { GrainFactory } from "@thresh/runtime/grain-factory";
import { invocationContext, withCallOptions } from "@thresh/runtime/invocation-context";

// Ambient cancellation remainder (issue #18 follow-up): `withCallOptions` is
// the friendlier per-call deadline/signal API a caller (grain code or
// top-level client code) uses to set a NEW deadline/signal for a proxy call —
// `InvokeCallOptions.deadlineMs`/`signal` alone only reaches internal
// `Dispatcher.invoke` callers, never `GrainFactory`'s public proxy surface.

interface IPingable extends GrainKey<string> {
  ping(): Promise<string>;
}

const IPingable = defineGrainInterface<IPingable>("IPingable.call-options-test");

function buildFactory(invoke: Dispatcher["invoke"]): GrainFactory {
  const factory = new GrainFactory(() => "IPingable");
  factory.setDispatcher({ invoke });
  return factory;
}

describe("withCallOptions", () => {
  it("stamps a fresh req.deadline on a call made from top-level (non-grain) code", async () => {
    let seen: InvocationRequest | undefined;
    const factory = buildFactory((req) => {
      seen = req;
      return Promise.resolve("pong");
    });
    const grainRef = factory.getGrain(IPingable, "a");

    const before = Date.now();
    await withCallOptions({ deadlineMs: 5_000 }, () => grainRef.ping());

    expect(seen?.deadline).toBeDefined();
    expect(seen!.deadline!).toBeGreaterThanOrEqual(before + 5_000);
    expect(seen!.deadline!).toBeLessThan(before + 5_000 + 1_000);
  });

  it("passes an opts.signal through to the local call for a top-level caller", async () => {
    let seenOpts: InvokeCallOptions | undefined;
    const factory = buildFactory((_req, opts) => {
      seenOpts = opts;
      return Promise.resolve("pong");
    });
    const grainRef = factory.getGrain(IPingable, "a");
    const controller = new AbortController();

    await withCallOptions({ signal: controller.signal }, () => grainRef.ping());

    expect(seenOpts?.signal).toBe(controller.signal);
  });

  it("an explicit deadlineMs overrides an already-ambient deadline for calls made inside fn", async () => {
    let seen: InvocationRequest | undefined;
    const factory = buildFactory((req) => {
      seen = req;
      return Promise.resolve("pong");
    });
    const grainRef = factory.getGrain(IPingable, "a");

    const outerDeadline = Date.now() + 1_000;
    await invocationContext.run(
      {
        senderId: undefined,
        ownerId: undefined,
        reentrancyId: "r1",
        deadline: outerDeadline,
      },
      () => withCallOptions({ deadlineMs: 60_000 }, () => grainRef.ping()),
    );

    expect(seen?.deadline).toBeGreaterThan(outerDeadline);
  });

  it("composes signal with an already-ambient one (either firing aborts the composed signal)", async () => {
    let seenOpts: InvokeCallOptions | undefined;
    const factory = buildFactory((_req, opts) => {
      seenOpts = opts;
      return Promise.resolve("pong");
    });
    const grainRef = factory.getGrain(IPingable, "a");
    const outer = new AbortController();
    const inner = new AbortController();

    await invocationContext.run(
      { senderId: undefined, ownerId: undefined, reentrancyId: "r1", signal: outer.signal },
      () => withCallOptions({ signal: inner.signal }, () => grainRef.ping()),
    );

    expect(seenOpts?.signal?.aborted).toBe(false);
    outer.abort();
    expect(seenOpts?.signal?.aborted).toBe(true);
  });

  it("propagates the deadline it sets to a second, nested downstream call", async () => {
    const seenDeadlines: (number | undefined)[] = [];
    const factory = buildFactory((req) => {
      seenDeadlines.push(req.deadline);
      // Simulate the callee turning around and making another outgoing call
      // with whatever ambient context this call arrived with — exactly what
      // `ActivationData.invoke` -> a nested `GrainFactory` call would do.
      return Promise.resolve("pong");
    });
    const grainRef = factory.getGrain(IPingable, "a");

    await withCallOptions({ deadlineMs: 2_000 }, async () => {
      await grainRef.ping();
      await grainRef.ping(); // still inside the same withCallOptions scope
    });

    expect(seenDeadlines).toHaveLength(2);
    expect(seenDeadlines[0]).toBe(seenDeadlines[1]);
  });
});
