import { describe, expect, it } from "vitest";

import { ABORTED, raceAbort, raceSignal } from "./abort";
import { GrainCallAbortedError } from "./errors";

describe("raceAbort", () => {
  it("passes a settled value through when no signal is given", async () => {
    await expect(raceAbort(Promise.resolve(7), undefined)).resolves.toBe(7);
  });

  it("passes a settled value through when the signal never fires", async () => {
    const controller = new AbortController();
    await expect(raceAbort(Promise.resolve(7), controller.signal)).resolves.toBe(7);
  });

  it("resolves to ABORTED rather than rejecting when the signal fires first", async () => {
    const controller = new AbortController();
    const race = raceAbort(new Promise<number>(() => {}), controller.signal);
    controller.abort();

    await expect(race).resolves.toBe(ABORTED);
  });

  it("resolves to ABORTED when the signal is already aborted", async () => {
    await expect(raceAbort(new Promise<number>(() => {}), AbortSignal.abort())).resolves.toBe(
      ABORTED,
    );
  });

  it("still propagates a rejection from the promise itself", async () => {
    const controller = new AbortController();
    const boom = new Error("boom");

    await expect(raceAbort(Promise.reject(boom), controller.signal)).rejects.toBe(boom);
  });

  it("is the settle-with-sentinel counterpart of raceSignal, which rejects", async () => {
    const controller = new AbortController();
    const rejecting = raceSignal(new Promise<number>(() => {}), controller.signal);
    const settling = raceAbort(new Promise<number>(() => {}), controller.signal);
    controller.abort();

    await expect(rejecting).rejects.toThrow(GrainCallAbortedError);
    await expect(settling).resolves.toBe(ABORTED);
  });
});
