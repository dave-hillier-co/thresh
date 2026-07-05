/**
 * Tunable weights for `ResourceOptimizedPlacement` (Orleans
 * `ResourceOptimizedPlacementOptions`). All weight properties are relative to
 * each other and must lie within [0, 100]; use
 * `validateResourceOptimizedPlacementOptions` (or
 * `ResourceOptimizedPlacementOptionsValidator`) to check a candidate set
 * before applying it.
 */
export interface ResourceOptimizedPlacementOptions {
  /**
   * The importance of the CPU usage by the silo. A higher value favours
   * silos with lower CPU usage. Valid range is [0, 100].
   */
  readonly cpuUsageWeight: number;

  /**
   * The importance of the memory usage by the silo. A higher value favours
   * silos with lower memory usage. Valid range is [0, 100].
   */
  readonly memoryUsageWeight: number;

  /**
   * The importance of the available memory to the silo. A higher value
   * favours silos with higher available memory. Valid range is [0, 100].
   */
  readonly availableMemoryWeight: number;

  /**
   * The importance of the maximum available memory to the silo. A higher
   * value favours silos with higher maximum available memory. Valid range
   * is [0, 100].
   */
  readonly maxAvailableMemoryWeight: number;

  /**
   * The importance of the current activation count to the silo. A higher
   * value favours silos with lower activation count. Valid range is
   * [0, 100].
   */
  readonly activationCountWeight: number;

  /**
   * The margin within which the local silo's utilization score is treated
   * as equal to a remote silo's, so ties favour the local silo. 0 always
   * favours the lower-utilization silo; 100 always favours the local silo.
   * Valid range is [0, 100].
   */
  readonly localSiloPreferenceMargin: number;
}

export const DEFAULT_CPU_USAGE_WEIGHT = 40;
export const DEFAULT_MEMORY_USAGE_WEIGHT = 20;
export const DEFAULT_AVAILABLE_MEMORY_WEIGHT = 20;
export const DEFAULT_MAX_AVAILABLE_MEMORY_WEIGHT = 5;
export const DEFAULT_ACTIVATION_COUNT_WEIGHT = 15;
export const DEFAULT_LOCAL_SILO_PREFERENCE_MARGIN = 5;

/** `ResourceOptimizedPlacementOptions` populated with the upstream defaults. */
export const defaultResourceOptimizedPlacementOptions = (): ResourceOptimizedPlacementOptions => ({
  cpuUsageWeight: DEFAULT_CPU_USAGE_WEIGHT,
  memoryUsageWeight: DEFAULT_MEMORY_USAGE_WEIGHT,
  availableMemoryWeight: DEFAULT_AVAILABLE_MEMORY_WEIGHT,
  maxAvailableMemoryWeight: DEFAULT_MAX_AVAILABLE_MEMORY_WEIGHT,
  activationCountWeight: DEFAULT_ACTIVATION_COUNT_WEIGHT,
  localSiloPreferenceMargin: DEFAULT_LOCAL_SILO_PREFERENCE_MARGIN,
});

/** Raised when a `ResourceOptimizedPlacementOptions` weight is out of range. */
export class PlacementConfigurationError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "PlacementConfigurationError";
  }
}

const WEIGHT_FIELDS: ReadonlyArray<keyof ResourceOptimizedPlacementOptions> = [
  "cpuUsageWeight",
  "memoryUsageWeight",
  "availableMemoryWeight",
  "maxAvailableMemoryWeight",
  "activationCountWeight",
  "localSiloPreferenceMargin",
];

/**
 * Throws `PlacementConfigurationError` if any weight in `options` is
 * outside the inclusive [0, 100] range (Orleans
 * `ResourceOptimizedPlacementOptionsValidator.ValidateConfiguration`).
 */
export function validateResourceOptimizedPlacementOptions(
  options: ResourceOptimizedPlacementOptions,
): void {
  for (const field of WEIGHT_FIELDS) {
    const value = options[field];
    if (value < 0 || value > 100) {
      throw new PlacementConfigurationError(`${field} must be inclusive between [0-100]`);
    }
  }
}

/** Object form of `validateResourceOptimizedPlacementOptions`, mirroring Orleans' `IConfigurationValidator`. */
export class ResourceOptimizedPlacementOptionsValidator {
  constructor(private readonly options: ResourceOptimizedPlacementOptions) {}

  validateConfiguration(): void {
    validateResourceOptimizedPlacementOptions(this.options);
  }
}
