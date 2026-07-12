/**
 * Tunables for the activation repartitioner, ported from Orleans'
 * `ActivationRepartitionerOptions`
 * (`src/Orleans.Runtime/Configuration/Options/ActivationRepartitionerOptions.cs`). `TimeSpan`
 * fields become plain millisecond numbers (`*Ms`), matching this codebase's existing convention
 * (see `packages/runtime/src/grain-timer-impl.ts`).
 */
export interface ActivationRepartitionerOptions {
  /**
   * The maximum number of edges to retain in-memory during a repartitioning round. An edge
   * represents how many calls were made from one grain to another.
   */
  maxEdgeCount: number;

  /** The minimum time between initiating a repartitioning round. */
  minRoundPeriodMs: number;

  /** The maximum time between initiating a repartitioning round. */
  maxRoundPeriodMs: number;

  /**
   * The minimum time needed for a silo to recover from a previous repartitioning round. Until
   * this time has elapsed, this silo will not take part in any repartitioning attempt from
   * another silo.
   */
  recoveryPeriodMs: number;

  /**
   * The maximum number of unprocessed edges to buffer. If this number is exceeded, the oldest
   * edges will be discarded.
   */
  maxUnprocessedEdges: number;

  /**
   * Whether to enable the local vertex (anchoring) filter. Enabled by default.
   */
  anchoringFilterEnabled: boolean;

  /**
   * The maximum allowed error rate when `anchoringFilterEnabled` is `true`, otherwise this does
   * not apply. Allowed range: `[0.001, 0.01]` (0.1% - 1%).
   */
  probabilisticFilteringMaxAllowedErrorRate: number;

  /** How long anchoring history is retained, in repartitioning-round generations. */
  anchoringFilterGenerations: number;
}

export const DEFAULT_MAX_EDGE_COUNT = 10_000;
export const DEFAULT_MINUMUM_ROUND_PERIOD_MS = 60_000; // TimeSpan.FromMinutes(1)
export const DEFAULT_MAXIMUM_ROUND_PERIOD_MS = 120_000; // TimeSpan.FromMinutes(2)
export const DEFAULT_RECOVERY_PERIOD_MS = 60_000; // TimeSpan.FromMinutes(1)
export const DEFAULT_MAX_UNPROCESSED_EDGES = 100_000;
export const DEFAULT_ANCHORING_FILTER_ENABLED = true;
export const DEFAULT_PROBABILISTIC_FILTERING_MAX_ALLOWED_ERROR = 0.01;
export const DEFAULT_ANCHORING_FILTER_GENERATIONS = 3;

/** `ActivationRepartitionerOptions` populated with the upstream defaults. */
export const defaultActivationRepartitionerOptions = (): ActivationRepartitionerOptions => ({
  maxEdgeCount: DEFAULT_MAX_EDGE_COUNT,
  minRoundPeriodMs: DEFAULT_MINUMUM_ROUND_PERIOD_MS,
  maxRoundPeriodMs: DEFAULT_MAXIMUM_ROUND_PERIOD_MS,
  recoveryPeriodMs: DEFAULT_RECOVERY_PERIOD_MS,
  maxUnprocessedEdges: DEFAULT_MAX_UNPROCESSED_EDGES,
  anchoringFilterEnabled: DEFAULT_ANCHORING_FILTER_ENABLED,
  probabilisticFilteringMaxAllowedErrorRate: DEFAULT_PROBABILISTIC_FILTERING_MAX_ALLOWED_ERROR,
  anchoringFilterGenerations: DEFAULT_ANCHORING_FILTER_GENERATIONS,
});

/** Raised when an `ActivationRepartitionerOptions` value is out of range. */
export class ActivationRepartitionerConfigurationError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "ActivationRepartitionerConfigurationError";
  }
}

/**
 * Throws `ActivationRepartitionerConfigurationError` if `options` is invalid (Orleans
 * `ActivationRepartitionerOptionsValidator.ValidateConfiguration`).
 */
export function validateActivationRepartitionerOptions(options: ActivationRepartitionerOptions): void {
  if (options.maxEdgeCount <= 0) {
    throwMustBeGreaterThanZero("maxEdgeCount");
  }

  if (options.maxUnprocessedEdges <= 0) {
    throwMustBeGreaterThanZero("maxUnprocessedEdges");
  }

  if (options.minRoundPeriodMs < 0) {
    throwMustBeGreaterThanOrEqualToZero("minRoundPeriodMs");
  }

  if (options.maxRoundPeriodMs <= 0) {
    throwMustBeGreaterThanZero("maxRoundPeriodMs");
  }

  if (options.recoveryPeriodMs < 0) {
    throwMustBeGreaterThanOrEqualToZero("recoveryPeriodMs");
  }

  if (options.maxRoundPeriodMs < options.minRoundPeriodMs) {
    throwMustBeGreaterThanOrEqualTo("maxRoundPeriodMs", "minRoundPeriodMs");
  }

  if (options.minRoundPeriodMs < options.recoveryPeriodMs) {
    throwMustBeGreaterThanOrEqualTo("minRoundPeriodMs", "recoveryPeriodMs");
  }

  if (
    options.probabilisticFilteringMaxAllowedErrorRate < 0.001 ||
    options.probabilisticFilteringMaxAllowedErrorRate > 0.01
  ) {
    throw new ActivationRepartitionerConfigurationError(
      "probabilisticFilteringMaxAllowedErrorRate must be inclusive between [0.001 - 0.01](0.1% - 1%)",
    );
  }

  if (options.anchoringFilterGenerations <= 0) {
    throwMustBeGreaterThanZero("anchoringFilterGenerations");
  }
}

function throwMustBeGreaterThanOrEqualToZero(propertyName: string): never {
  throw new ActivationRepartitionerConfigurationError(`${propertyName} must be greater than or equal to 0.`);
}

function throwMustBeGreaterThanZero(propertyName: string): never {
  throw new ActivationRepartitionerConfigurationError(`${propertyName} must be greater than 0.`);
}

function throwMustBeGreaterThanOrEqualTo(name1: string, name2: string): never {
  throw new ActivationRepartitionerConfigurationError(`${name1} must be greater than or equal to ${name2}.`);
}

/** Object form of `validateActivationRepartitionerOptions`, mirroring Orleans' `IConfigurationValidator`. */
export class ActivationRepartitionerOptionsValidator {
  constructor(private readonly options: ActivationRepartitionerOptions) {}

  validateConfiguration(): void {
    validateActivationRepartitionerOptions(this.options);
  }
}
