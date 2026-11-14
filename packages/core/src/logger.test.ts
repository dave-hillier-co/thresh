import { afterEach, describe, expect, it, vi } from "vitest";
import { defaultLogger, noopLogger } from "@thresh/core/logger";

describe("defaultLogger", () => {
  afterEach(() => vi.restoreAllMocks());

  it("writes warn and error to the console", () => {
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    const error = vi.spyOn(console, "error").mockImplementation(() => {});
    defaultLogger.warn("watch failed", { attempt: 2 });
    defaultLogger.error("hook failed");
    expect(warn).toHaveBeenCalledTimes(1);
    expect(String(warn.mock.calls[0]![0])).toContain("watch failed");
    expect(error).toHaveBeenCalledTimes(1);
    expect(String(error.mock.calls[0]![0])).toContain("hook failed");
  });

  it("discards debug and info", () => {
    const spies = (["debug", "info", "log", "warn", "error"] as const).map((m) =>
      vi.spyOn(console, m).mockImplementation(() => {}),
    );
    defaultLogger.debug("d");
    defaultLogger.info("i");
    expect(spies.every((s) => s.mock.calls.length === 0)).toBe(true);
  });

  it("noopLogger stays silent at every level", () => {
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    const error = vi.spyOn(console, "error").mockImplementation(() => {});
    noopLogger.warn("w");
    noopLogger.error("e");
    expect(warn).not.toHaveBeenCalled();
    expect(error).not.toHaveBeenCalled();
  });
});
