import { describe, expect, it } from "vitest";
import { mapCancellationValues } from "@thresh/core/cancellation-value-walk";
import { CancellationTokenPlaceholder } from "@thresh/core/grain-cancellation-token";

const replaceWithMarker = (found: unknown): unknown =>
  found instanceof AbortSignal ? "replaced" : found;

describe("mapCancellationValues", () => {
  it("returns a subtree containing no cancellation value BY IDENTITY", () => {
    // The no-signal case is every argument of every grain call: it must not
    // copy, or a same-silo callee would stop receiving the caller's own object.
    const request = { resource: { type: "document", id: "1" }, tags: new Set(["a"]) };
    expect(mapCancellationValues(request, replaceWithMarker)).toBe(request);
  });

  it("rebuilds only the containers on the path to a replaced value", () => {
    const signal = new AbortController().signal;
    const untouched = { deep: { deeper: 1 } };
    const request = { untouched, carrier: { signal } };
    const result = mapCancellationValues(request, replaceWithMarker) as typeof request;

    expect(result).not.toBe(request);
    expect(result.carrier.signal).toBe("replaced" as never);
    // The caller's own record is intact — it is a copy that was rewritten.
    expect(request.carrier.signal).toBe(signal);
    // Everything off the path is shared, not cloned.
    expect(result.untouched).toBe(untouched);
  });

  it("reaches a value under an array, a Map and a Set", () => {
    const signal = new AbortController().signal;
    const result = mapCancellationValues(
      { steps: [{ by: new Map([["a", new Set([signal])]]) }] },
      replaceWithMarker,
    ) as { steps: { by: Map<string, Set<unknown>> }[] };
    expect([...result.steps[0]!.by.get("a")!]).toEqual(["replaced"]);
  });

  it("does not walk into a class instance", () => {
    // Rebuilding one would hand a same-silo callee a plain object where its
    // signature declares the class — a worse fault than the one being fixed.
    class Envelope {
      constructor(readonly signal: AbortSignal) {}
    }
    const envelope = new Envelope(new AbortController().signal);
    const result = mapCancellationValues({ envelope }, replaceWithMarker) as { envelope: Envelope };
    expect(result.envelope).toBe(envelope);
  });

  it("terminates on a cyclic graph, which a same-silo call is allowed to pass", () => {
    const signal = new AbortController().signal;
    const request: Record<string, unknown> = { signal };
    request.self = request;
    const result = mapCancellationValues(request, replaceWithMarker) as Record<string, unknown>;
    expect(result.signal).toBe("replaced");
  });

  it("finds a token placeholder as well as a signal", () => {
    const placeholder = new CancellationTokenPlaceholder("t-1", false, true);
    const seen: unknown[] = [];
    mapCancellationValues({ inner: { placeholder } }, (found) => {
      seen.push(found);
      return found;
    });
    expect(seen).toEqual([placeholder]);
  });
});

describe("mapCancellationValues cost", () => {
  /**
   * The `seen` set is a CYCLE guard, and a cycle guard alone is not enough: released on the way
   * out, a node reachable by k distinct paths is walked k times, so a diamond-shaped argument
   * graph costs 2^depth. This walk runs on every argument of every grain call - a path that did no
   * walking at all before - so a shared subtree must be visited once and its result reused.
   *
   * Depth 16 is ~65k paths over 17 distinct nodes: fast either way, but the visit count separates
   * the two implementations unambiguously. (Deeper hangs the process outright, because the walk is
   * synchronous and no test timeout can interrupt it.)
   */
  it("visits a shared subtree once, not once per path", () => {
    const signal = new AbortController().signal;
    let node: Record<string, unknown> = { signal };
    for (let i = 0; i < 16; i++) node = { a: node, b: node };

    let visits = 0;
    mapCancellationValues(node, (found) => {
      visits += 1;
      return found;
    });

    expect(visits).toBe(1);
  });
});
