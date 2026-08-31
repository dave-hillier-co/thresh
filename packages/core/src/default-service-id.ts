/**
 * Orleans' `ClusterOptions.DefaultServiceId`
 * (`src/Orleans.Core/Configuration/Options/ClusterOptions.cs:19`).
 *
 * Shared rather than redeclared per module because THREE independent places have
 * to agree on it or durable state is silently stranded: the value a silo
 * advertises when its config names no `serviceId`, the value each storage
 * provider falls back to, and the value `PostgresGrainStorage`'s migration
 * backfills pre-service rows to. A silo that defaulted to something else would
 * stamp existing rows with this literal and then read them back under the other
 * one, matching nothing — the grain activates empty and its next write orphans
 * the original row.
 *
 * Orleans keeps `ServiceId` independent of `ClusterId` deliberately: it "should
 * survive deployment and redeployment, where as ClusterId might not"
 * (ClusterOptions.cs:36). Defaulting it to a cluster id would destroy exactly
 * that property.
 */
export const DEFAULT_SERVICE_ID = "default";
