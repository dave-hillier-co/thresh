import type { Duration } from "./duration";
import type { GrainAddress } from "./grain-address";
import { defineGrainInterface } from "./grain-interface";
import type { GrainId } from "./grain-id";
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
 * Orleans' `Orleans.Runtime.DetailedGrainStatistic`: one live activation's
 * identity and host, unlike `SimpleGrainStatistic` which only amalgamates a
 * per-(type, silo) count. `GetDetailedGrainStatistics` returns one of these
 * per live activation across the cluster — e.g. the activation-rebalancing
 * parity tests use it to count per-silo activations of one grain type
 * directly, rather than reading the aggregate count.
 */
export interface DetailedGrainStatistic {
  grainType: string;
  silo: SiloAddress;
  grainId: GrainId;
}

/**
 * Orleans' `IManagementGrain`: a built-in system grain (reached via
 * `getGrain(IManagementGrain, 0n)`) that surfaces cluster topology and
 * per-grain-type activation statistics. Only the subset of the upstream
 * surface the ported parity tests exercise is modelled here (GetHosts,
 * GetDetailedHosts, GetSimpleGrainStatistics, GetActivationAddress,
 * SendControlCommandToProvider); the rest of upstream's surface
 * (ForceGarbageCollection, GetRuntimeStatistics, ...) has no equivalent yet.
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
   * Detailed, per-activation grain statistics amalgamated across every active
   * silo (Orleans `GetDetailedGrainStatistics`). `types`, given, restricts the
   * result to those grain types; `hostsIds` is accepted for signature parity
   * with upstream but not consulted — every ported test queries the whole
   * cluster.
   */
  getDetailedGrainStatistics(
    types?: string[] | null,
    hostsIds?: SiloAddress[] | null,
  ): Promise<DetailedGrainStatistic[]>;
  /**
   * The activation address of a grain, or `null` if it has none (Orleans
   * `GetActivationAddress`, `ValueTask<SiloAddress>`, `null` when
   * unactivated). Throws if `reference` addresses a `StatelessWorker` grain,
   * which has no single activation to report (Orleans:
   * `InvalidOperationException`).
   */
  getActivationAddress(reference: unknown): Promise<GrainAddress | null>;
  /**
   * The number of live activations of the same grain as `grainReference`,
   * amalgamated across every active silo (Orleans `GetGrainActivationCount`).
   * Unlike `getActivationAddress`, this works for `StatelessWorker` grains
   * too — it is in fact the only way to observe how many local activations
   * one has scaled up to, since they have no single address.
   */
  getGrainActivationCount(grainReference: unknown): Promise<number>;
  /**
   * Forces an immediate activation-collection sweep on every active silo,
   * treating any activation idle for at least `ageLimit` as collectible
   * regardless of the silo's normal collection age (Orleans
   * `ForceActivationCollection(TimeSpan)`).
   */
  forceActivationCollection(ageLimit: Duration): Promise<void>;
  /**
   * Orleans `IManagementGrain.SendControlCommandToProvider`: invoke an
   * `IControllable` control command on the named provider on EVERY active
   * silo, returning one result per silo. `providerTypeName` is accepted for
   * signature parity with upstream (which routes by provider type + name)
   * but this port resolves providers by name only.
   */
  sendControlCommandToProvider(
    providerTypeName: string,
    providerName: string,
    command: number,
    arg?: unknown,
  ): Promise<unknown[]>;
}

export const IManagementGrain = defineGrainInterface<IManagementGrain>(
  "Orleans.Runtime.IManagementGrain",
);
