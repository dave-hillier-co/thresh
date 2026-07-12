/**
 * The public, host-facing tunables for the activation rebalancer — a faithful
 * port of Orleans' `ActivationRebalancerOptions`
 * (`src/Orleans.Runtime/Configuration/Options/ActivationRebalancerOptions.cs`) plus its
 * validator. This is a superset of `rebalancer-model`'s `RebalancerOptions` (the pure
 * entropy-model tunables): it adds the two scheduling knobs the model has no opinion on —
 * the startup due-time and the inter-cycle period — and is the shape a host author
 * configures end-to-end (mirroring `SiloBuilder.useActivationRebalancing`'s
 * `sessionCyclePeriodSeconds` option, but as one validated bag). `TimeSpan` fields become
 * plain millisecond numbers (`*Ms`), matching this codebase's existing convention (see
 * `packages/runtime/src/grain-timer-impl.ts`).
 */
import type { RebalancerOptions } from "@tsva/runtime/placement/rebalancing/rebalancer-model";
import {
  DEFAULT_ACTIVATION_MIGRATION_COUNT_LIMIT,
  DEFAULT_ALLOWED_ENTROPY_DEVIATION,
  DEFAULT_CYCLE_NUMBER_WEIGHT,
  DEFAULT_ENTROPY_QUANTUM,
  DEFAULT_MAX_STAGNANT_CYCLES,
  DEFAULT_SCALE_ALLOWED_ENTROPY_DEVIATION,
  DEFAULT_SCALED_ENTROPY_DEVIATION_ACTIVATION_THRESHOLD,
  DEFAULT_SILO_NUMBER_WEIGHT,
  MAX_SCALED_ENTROPY_DEVIATION,
} from "@tsva/runtime/placement/rebalancing/rebalancer-model";

export { MAX_SCALED_ENTROPY_DEVIATION };

/** The rebalancer's full public configuration: the model's tunables plus scheduling. */
export interface ActivationRebalancerOptions extends RebalancerOptions {
  /** The due time for the rebalancer to start its very first session, in ms. */
  rebalancerDueTimeMs: number;
  /** The time between two consecutive rebalancing cycles within a session, in ms. */
  sessionCyclePeriodMs: number;
}

export const DEFAULT_REBALANCER_DUE_TIME_MS = 60_000; // TimeSpan.FromSeconds(60)
export const DEFAULT_SESSION_CYCLE_PERIOD_MS = 15_000; // TimeSpan.FromSeconds(15)

/** `ActivationRebalancerOptions` populated with the upstream defaults. */
export const defaultActivationRebalancerOptions = (): ActivationRebalancerOptions => ({
  rebalancerDueTimeMs: DEFAULT_REBALANCER_DUE_TIME_MS,
  sessionCyclePeriodMs: DEFAULT_SESSION_CYCLE_PERIOD_MS,
  maxStagnantCycles: DEFAULT_MAX_STAGNANT_CYCLES,
  entropyQuantum: DEFAULT_ENTROPY_QUANTUM,
  allowedEntropyDeviation: DEFAULT_ALLOWED_ENTROPY_DEVIATION,
  scaleAllowedEntropyDeviation: DEFAULT_SCALE_ALLOWED_ENTROPY_DEVIATION,
  scaledEntropyDeviationActivationThreshold: DEFAULT_SCALED_ENTROPY_DEVIATION_ACTIVATION_THRESHOLD,
  cycleNumberWeight: DEFAULT_CYCLE_NUMBER_WEIGHT,
  siloNumberWeight: DEFAULT_SILO_NUMBER_WEIGHT,
  activationMigrationCountLimit: DEFAULT_ACTIVATION_MIGRATION_COUNT_LIMIT,
});

/**
 * The cross-cutting knob `ActivationRebalancerOptionsValidator` checks
 * `sessionCyclePeriodMs` against (Orleans `DeploymentLoadPublisherOptions`):
 * the interval on which per-silo load statistics are refreshed and published
 * cluster-wide. A rebalancing session must run at least two publisher
 * refreshes apart, or it would plan cycles against stale load.
 */
export interface DeploymentLoadPublisherOptions {
  /** How often deployment (per-silo load) statistics are published, in ms. */
  deploymentLoadPublisherRefreshTimeMs: number;
}

export const DEFAULT_DEPLOYMENT_LOAD_PUBLISHER_REFRESH_TIME_MS = 1_000; // TimeSpan.FromSeconds(1)

export const defaultDeploymentLoadPublisherOptions = (): DeploymentLoadPublisherOptions => ({
  deploymentLoadPublisherRefreshTimeMs: DEFAULT_DEPLOYMENT_LOAD_PUBLISHER_REFRESH_TIME_MS,
});

/** Raised when an `ActivationRebalancerOptions` value is out of range. */
export class ActivationRebalancerConfigurationError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "ActivationRebalancerConfigurationError";
  }
}

/**
 * Throws `ActivationRebalancerConfigurationError` if `options` is invalid, checked in the
 * same order as Orleans' `ActivationRebalancerOptionsValidator.ValidateConfiguration` (so the
 * same combination of bad fields throws for the same reason).
 */
export function validateActivationRebalancerOptions(
  options: ActivationRebalancerOptions,
  publisherOptions: DeploymentLoadPublisherOptions = defaultDeploymentLoadPublisherOptions(),
): void {
  if (options.sessionCyclePeriodMs < 2 * publisherOptions.deploymentLoadPublisherRefreshTimeMs) {
    throw new ActivationRebalancerConfigurationError(
      "sessionCyclePeriodMs must be at least 2 x deploymentLoadPublisherRefreshTimeMs",
    );
  }

  if (options.maxStagnantCycles <= 0) {
    throw new ActivationRebalancerConfigurationError("maxStagnantCycles must be greater than 0");
  }

  if (options.entropyQuantum <= 0 || options.entropyQuantum > 0.1) {
    throw new ActivationRebalancerConfigurationError(
      "entropyQuantum must be greater than 0, and less or equal 0.1",
    );
  }

  if (options.allowedEntropyDeviation <= 0 || options.allowedEntropyDeviation > 0.1) {
    throw new ActivationRebalancerConfigurationError(
      "allowedEntropyDeviation must be greater than 0, and less or equal 0.1",
    );
  }

  if (options.cycleNumberWeight <= 0 || options.cycleNumberWeight > 1) {
    throw new ActivationRebalancerConfigurationError(
      "cycleNumberWeight must be greater than 0, and less or equal to 1",
    );
  }

  if (options.siloNumberWeight < 0 || options.siloNumberWeight > 1) {
    throw new ActivationRebalancerConfigurationError(
      "siloNumberWeight must be greater than or equal to 0, and less or equal to 1",
    );
  }

  if (options.activationMigrationCountLimit < 1) {
    throw new ActivationRebalancerConfigurationError(
      "activationMigrationCountLimit must be greater than 0",
    );
  }

  if (options.scaledEntropyDeviationActivationThreshold < 1_000) {
    throw new ActivationRebalancerConfigurationError(
      "scaledEntropyDeviationActivationThreshold must be greater than or equal to 1000",
    );
  }
}

/** Object form of `validateActivationRebalancerOptions`, mirroring Orleans' `IConfigurationValidator`. */
export class ActivationRebalancerOptionsValidator {
  constructor(
    private readonly options: ActivationRebalancerOptions,
    private readonly publisherOptions: DeploymentLoadPublisherOptions = defaultDeploymentLoadPublisherOptions(),
  ) {}

  validateConfiguration(): void {
    validateActivationRebalancerOptions(this.options, this.publisherOptions);
  }
}
