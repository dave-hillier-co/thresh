/**
 * A structured, leveled logger (the analogue of Orleans' `ILogger`). Each call
 * takes a message and optional structured fields; the host plugs in an
 * implementation (console, pino, an OpenTelemetry logs bridge, …). When none is
 * configured the runtime uses {@link defaultLogger}: warnings and errors reach the
 * console, debug and info are discarded.
 */
export type LogFields = Record<string, unknown>;

export interface Logger {
  debug(message: string, fields?: LogFields): void;
  info(message: string, fields?: LogFields): void;
  warn(message: string, fields?: LogFields): void;
  error(message: string, fields?: LogFields): void;
}

/** A logger that discards everything; pass it explicitly to silence a silo (tests do). */
export const noopLogger: Logger = {
  debug() {},
  info() {},
  warn() {},
  error() {},
};

/**
 * The logger used when none is configured: `warn` and `error` go to the console (a swallowed
 * failure must never be invisible), `debug` and `info` are discarded.
 */
export const defaultLogger: Logger = {
  debug() {},
  info() {},
  warn(message, fields) {
    if (fields === undefined) console.warn(`[thresh] ${message}`);
    else console.warn(`[thresh] ${message}`, fields);
  },
  error(message, fields) {
    if (fields === undefined) console.error(`[thresh] ${message}`);
    else console.error(`[thresh] ${message}`, fields);
  },
};
