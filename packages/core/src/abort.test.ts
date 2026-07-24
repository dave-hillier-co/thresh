import { describe, expect, it } from "vitest";
import { combineSignals, raceSignal } from "@tsva/core/abort";
import { GrainCallAbortedError } from "@tsva/core/errors";

describe("combineSignals", () => {
  it("returns undefined given no signals", () => {
    expect(combineSignals([])).toBeUndefined();
    expect(combineSignals([undefined, undefined])).toBeUndefined();
  });

  it("unwraps a single signal without wrapping it in AbortSignal.any", () => {
    const controller = new AbortController();
    expect(combineSignals([controller.signal])).toBe(controller.signal);
    expect(combineSignals([undefined, controller.signal, undefined])).toBe(controller.signal);
  });

  it("fires when any of several combined signals fires", () => {
    const a = new AbortController();
    const b = new AbortController();
    const combined = combineSignals([a.signal, b.signal])!;
    expect(combined.aborted).toBe(false);

    b.abort();
    expect(combined.aborted).toBe(true);
  });

  it("is already aborted if any input signal was aborted upfront", () => {
    const a = new AbortController();
    a.abort();
    const b = new AbortController();
    expect(combineSignals([a.signal, b.signal])!.aborted).toBe(true);
  });
});

describe("raceSignal", () => {
  it("passes an unsignalled call straight through", async () => {
    await expect(raceSignal(Promise.resolve(42), undefined)).resolves.toBe(42);
  });

  it("resolves with the promise's value when it settles before the signal fires", async () => {
    const controller = new AbortController();
    await expect(raceSignal(Promise.resolve("done"), controller.signal)).resolves.toBe("done");
  });

  it("propagates the promise's own rejection when it settles before the signal fires", async () => {
    const controller = new AbortController();
    await expect(raceSignal(Promise.reject(new Error("boom")), controller.signal)).rejects.toThrow(
      "boom",
    );
  });

  it("rejects with GrainCallAbortedError immediately for an already-aborted signal", async () => {
    const controller = new AbortController();
    controller.abort();
    const never = new Promise<void>(() => {}); // never settles
    await expect(raceSignal(never, controller.signal)).rejects.toBeInstanceOf(
      GrainCallAbortedError,
    );
  });

  it("rejects with GrainCallAbortedError once the signal fires, without waiting for the promise", async () => {
    const controller = new AbortController();
    const never = new Promise<void>(() => {});
    const raced = raceSignal(never, controller.signal);
    controller.abort();
    await expect(raced).rejects.toBeInstanceOf(GrainCallAbortedError);
  });
});
