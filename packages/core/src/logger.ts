/**
 * A structured, leveled logger (the analogue of Orleans' `ILogger`). Each call
 * takes a message and optional structured fields; the host plugs in an
 * implementation (console, pino, an OpenTelemetry logs bridge, …). Defaults to a
 * no-op so the runtime never logs unless a logger is configured.
 */
export type LogFields = Record<string, unknown>;

export interface Logger {
  debug(message: string, fields?: LogFields): void;
  info(message: string, fields?: LogFields): void;
  warn(message: string, fields?: LogFields): void;
  error(message: string, fields?: LogFields): void;
}

/** A logger that discards everything; the default when none is configured. */
export const noopLogger: Logger = {
  debug() {},
  info() {},
  warn() {},
  error() {},
};
