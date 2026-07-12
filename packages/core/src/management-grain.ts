import type { GrainAddress } from "./grain-address";
import { defineGrainInterface } from "./grain-interface";
import type { GrainWithIntegerKey } from "./key-kinds";
import type { SiloMember, SiloStatus } from "./membership";
import type { SiloAddress } from "./silo-address";

/**
 * Orleans' `Orleans.Runtime.SimpleGrainStatistic`: a per-grain-type activation
 * count on one silo. `GetSimpleGrainStatistics` amalgamates one of these per
 * (grain type, silo) pair across the whole cluster.
 */
export interface SimpleGrainStatistic {
  grainType: string;
  silo: SiloAddress;
  activationCount: number;
}

/**
 * Orleans' `IManagementGrain`: a built-in system grain (reached via
 * `getGrain(IManagementGrain, 0n)`) that surfaces cluster topology and
 * per-grain-type activation statistics. Only the subset of the upstream
 * surface the ported parity tests exercise is modelled here (GetHosts,
 * GetDetailedHosts, GetSimpleGrainStatistics, GetActivationAddress); the rest
 * of upstream's surface (ForceGarbageCollection, GetRuntimeStatistics,
 * SendControlCommandToProvider, ...) has no equivalent yet.
 */
export interface IManagementGrain extends GrainWithIntegerKey {
  /**
   * The silo hosts and statuses currently known in this cluster (Orleans
   * `GetHosts`). `onlyActive` (default `false`) restricts the result to
   * silos currently `active`.
   */
  getHosts(onlyActive?: boolean): Promise<Map<SiloAddress, SiloStatus>>;
  /**
   * The silo membership entries currently known in this cluster (Orleans
   * `GetDetailedHosts`). `onlyActive` (default `false`) restricts the result
   * to silos currently `active`.
   */
  getDetailedHosts(onlyActive?: boolean): Promise<SiloMember[]>;
  /**
   * Grain activation statistics amalgamated across every active silo (Orleans
   * `GetSimpleGrainStatistics`). `hostsIds` is accepted for signature parity
   * with upstream but not consulted — every ported test queries the whole
   * cluster.
   */
  getSimpleGrainStatistics(hostsIds?: SiloAddress[] | null): Promise<SimpleGrainStatistic[]>;
  /**
   * The activation address of a grain, or `null` if it has none (Orleans
   * `GetActivationAddress`, `ValueTask<SiloAddress>`, `null` when
   * unactivated). Throws if `reference` addresses a `StatelessWorker` grain,
   * which has no single activation to report (Orleans:
   * `InvalidOperationException`).
   */
  getActivationAddress(reference: unknown): Promise<GrainAddress | null>;
}

export const IManagementGrain = defineGrainInterface<IManagementGrain>(
  "Orleans.Runtime.IManagementGrain",
);
