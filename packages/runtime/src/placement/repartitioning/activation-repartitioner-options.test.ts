import { describe, expect, it } from "vitest";
import {
  ActivationRepartitionerConfigurationError,
  ActivationRepartitionerOptionsValidator,
  DEFAULT_ANCHORING_FILTER_ENABLED,
  DEFAULT_MAX_EDGE_COUNT,
  DEFAULT_MAXIMUM_ROUND_PERIOD_MS,
  DEFAULT_MINUMUM_ROUND_PERIOD_MS,
  DEFAULT_PROBABILISTIC_FILTERING_MAX_ALLOWED_ERROR,
  DEFAULT_RECOVERY_PERIOD_MS,
  defaultActivationRepartitionerOptions,
} from "./activation-repartitioner-options";

describe("defaultActivationRepartitionerOptions", () => {
  it("matches the pinned upstream defaults", () => {
    expect(DEFAULT_ANCHORING_FILTER_ENABLED).toBe(true);
    expect(DEFAULT_PROBABILISTIC_FILTERING_MAX_ALLOWED_ERROR).toBe(0.01);
    expect(DEFAULT_MAX_EDGE_COUNT).toBe(10_000);
    expect(DEFAULT_MINUMUM_ROUND_PERIOD_MS).toBe(60_000);
    expect(DEFAULT_MAXIMUM_ROUND_PERIOD_MS).toBe(120_000);
    expect(DEFAULT_RECOVERY_PERIOD_MS).toBe(60_000);
  });

  it("passes its own validator", () => {
    expect(() =>
      new ActivationRepartitionerOptionsValidator(defaultActivationRepartitionerOptions()).validateConfiguration(),
    ).not.toThrow();
  });
});

describe("ActivationRepartitionerOptionsValidator", () => {
  it("rejects a non-positive maxEdgeCount", () => {
    const options = { ...defaultActivationRepartitionerOptions(), maxEdgeCount: 0 };
    expect(() => new ActivationRepartitionerOptionsValidator(options).validateConfiguration()).toThrow(
      ActivationRepartitionerConfigurationError,
    );
  });

  it("rejects maxRoundPeriodMs < minRoundPeriodMs", () => {
    const options = {
      ...defaultActivationRepartitionerOptions(),
      minRoundPeriodMs: 120_000,
      maxRoundPeriodMs: 60_000,
    };
    expect(() => new ActivationRepartitionerOptionsValidator(options).validateConfiguration()).toThrow(
      ActivationRepartitionerConfigurationError,
    );
  });

  it("rejects an out-of-range error rate", () => {
    const options = { ...defaultActivationRepartitionerOptions(), probabilisticFilteringMaxAllowedErrorRate: 0.5 };
    expect(() => new ActivationRepartitionerOptionsValidator(options).validateConfiguration()).toThrow(
      ActivationRepartitionerConfigurationError,
    );
  });
});
