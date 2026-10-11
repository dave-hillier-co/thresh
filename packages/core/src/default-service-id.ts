/**
 * Orleans' `ClusterOptions.DefaultServiceId`
 * (`src/Orleans.Core/Configuration/Options/ClusterOptions.cs:19`).
 *
 * Shared rather than redeclared per module because two independent places have
 * to agree on it or durable state is silently stranded: the value a silo
 * advertises when its config names no `serviceId`, and the value each storage
 * provider falls back to. A provider built directly (with no silo) and one built
 * by a silo with no `serviceId` must read the same rows.
 *
 * Orleans keeps `ServiceId` independent of `ClusterId` deliberately: it "should
 * survive deployment and redeployment, where as ClusterId might not"
 * (ClusterOptions.cs:36). Defaulting it to a cluster id would destroy exactly
 * that property.
 */
export const DEFAULT_SERVICE_ID = "default";
