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

const wrapped = new WeakSet<Logger>();

/**
 * Wrap `logger` so an exception thrown by it (a broken sink, a closed transport) is swallowed.
 * Diagnostics are best effort: a failing logger must never abort the work it was reporting on,
 * such as the Kubernetes watch's reconnect or the self-probe's readiness flip, or replace the
 * original error a hook threw. Wrapping an already-safe logger returns it unchanged.
 */
export function safeLogger(logger: Logger): Logger {
  if (wrapped.has(logger)) return logger;
  const guard =
    (level: keyof Logger) =>
    (message: string, fields?: LogFields): void => {
      try {
        logger[level](message, fields);
      } catch {
        // best effort
      }
    };
  const safe: Logger = {
    debug: guard("debug"),
    info: guard("info"),
    warn: guard("warn"),
    error: guard("error"),
  };
  wrapped.add(safe);
  return safe;
}
