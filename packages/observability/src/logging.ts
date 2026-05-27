import type { Logger } from "@tsva/core/logger";
import type { IncomingGrainCallFilter } from "@tsva/core/grain-call-filter";

/**
 * A grain call filter that logs each incoming call with structured fields
 * (grain, interface, method, duration, status) — successes at `info`, failures
 * at `error` with the message. Logging on the call-filter seam keeps it
 * consistent with tracing/metrics and means each log line sits inside the
 * request context.
 */
export function loggingFilter(logger: Logger): IncomingGrainCallFilter {
  return async (ctx) => {
    const start = Date.now();
    const fields = {
      grain: ctx.target.toString(),
      interface: ctx.interfaceName,
      method: ctx.methodName,
    };
    try {
      await ctx.invoke();
      logger.info("grain call", { ...fields, durationMs: Date.now() - start, status: "ok" });
    } catch (err) {
      logger.error("grain call failed", {
        ...fields,
        durationMs: Date.now() - start,
        status: "error",
        error: err instanceof Error ? err.message : String(err),
      });
      throw err;
    }
  };
}

/** A simple structured logger writing one JSON line per record to the console. */
export function consoleLogger(): Logger {
  const write =
    (level: string) =>
    (message: string, fields?: Record<string, unknown>): void => {
      console.log(JSON.stringify({ level, message, ...fields }));
    };
  return { debug: write("debug"), info: write("info"), warn: write("warn"), error: write("error") };
}
