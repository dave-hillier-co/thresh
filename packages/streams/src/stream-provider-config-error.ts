/**
 * A stream provider failed to load from its configuration (Orleans' notion of
 * a provider that never came up because its options were invalid — distinct
 * from a runtime *delivery* failure, which is per-event and reported through
 * `StreamFailureHandler` well after the provider is already running). Thrown
 * synchronously from a provider's constructor or a builder's config-parsing
 * path, before any queue, connection, or agent exists — so a bad config fails
 * silo startup fast instead of surfacing later as a mysterious delivery gap.
 */
export class StreamProviderConfigurationError extends Error {
  constructor(
    readonly providerName: string,
    message: string,
  ) {
    super(`stream provider "${providerName}" configuration error: ${message}`);
    this.name = "StreamProviderConfigurationError";
  }
}
