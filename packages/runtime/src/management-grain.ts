import { durationToMs, type Duration } from "@thresh/core/duration";
import type { GrainAddress } from "@thresh/core/grain-address";
import { Grain } from "@thresh/core/grain";
import type { GrainId } from "@thresh/core/grain-id";
import { setGrainOptions } from "@thresh/core/grain-metadata";
import { grainReferenceIdentity } from "@thresh/core/grain-reference";
import type { GrainType } from "@thresh/core/grain-type";
import {
  IManagementGrain,
  type DetailedGrainStatistic,
  type SimpleGrainStatistic,
} from "@thresh/core/management-grain";
import type { MembershipSnapshot, SiloMember, SiloStatus } from "@thresh/core/membership";
import type { SiloAddress } from "@thresh/core/silo-address";

export { IManagementGrain };

/** The grain type the built-in management grain registers under on every silo. */
export const MANAGEMENT_GRAIN_TYPE: GrainType = "Orleans.Runtime.ManagementGrain";

/**
 * The cluster-node hooks the built-in management grain needs, injected per
 * silo — see `ClusterNode`, which builds one of these closing over its own
 * membership view / directory / cross-silo stats gathering and registers the
 * resulting grain type on itself, so `getGrain(IManagementGrain, 0n)`
 * resolves correctly regardless which silo ends up hosting the activation.
 */
export interface ManagementContext {
  /** This silo's current view of cluster membership (Orleans `ISiloStatusOracle`/`IMembershipManager`). */
  membershipSnapshot(): MembershipSnapshot;
  /** Per-grain-type activation counts amalgamated across every active silo. */
  gatherGrainStats(): Promise<SimpleGrainStatistic[]>;
  /**
   * One entry per live activation amalgamated across every active silo,
   * optionally restricted to `types` (Orleans `GetDetailedGrainStatistics`).
   */
  gatherDetailedGrainStats(types?: string[]): Promise<DetailedGrainStatistic[]>;
  /** The grain directory's current activation address for `id`, if any — no side effect (doesn't activate). */
  lookupActivation(id: GrainId): Promise<GrainAddress | undefined>;
  /** Whether `type` is registered with `[StatelessWorker]`-equivalent placement. */
  isStatelessWorker(type: GrainType): boolean;
  /**
   * Live activation count for `id`, amalgamated across every active silo —
   * works for a `[StatelessWorker]` id too (0..maxLocalWorkers on whichever
   * silos have activated it), unlike `lookupActivation`.
   */
  activationCountFor(id: GrainId): Promise<number>;
  /** Force an immediate idle-activation sweep, with `ageLimitMs` in place of each activation's own age, on every active silo. */
  forceActivationCollection(ageLimitMs: number): Promise<void>;
  /**
   * Invoke an `IControllable` control command on the named stream provider on
   * every active silo, returning one result per silo (Orleans
   * `IManagementGrain.SendControlCommandToProvider`).
   */
  sendControlCommandToProvider(
    providerName: string,
    command: number,
    arg?: unknown,
  ): Promise<unknown[]>;
}

/**
 * Builds the concrete management-grain class for one silo, closing over its
 * `ManagementContext`. A fresh class per silo (rather than one shared class)
 * because the silo constructs this grain through the default `new ctor()` path
 * (no `GrainActivator` is involved for a built-in system grain, so there is
 * nowhere to pass a context) — the per-silo context is captured in the closure
 * instead, and
 * `setGrainOptions` is called directly (bypassing the `@grain()` decorator,
 * which only runs at module-evaluation time on a single static class) so the
 * catalog resolves this dynamically-built class the same as any other
 * registered grain type.
 */
export function createManagementGrainType(ctx: ManagementContext): new () => Grain {
  class ManagementGrain extends Grain implements IManagementGrain {
    async getHosts(onlyActive = false): Promise<Map<SiloAddress, SiloStatus>> {
      const result = new Map<SiloAddress, SiloStatus>();
      for (const member of ctx.membershipSnapshot().silos) {
        if (onlyActive && member.status !== "active") continue;
        result.set(member.address, member.status);
      }
      return result;
    }

    async getDetailedHosts(onlyActive = false): Promise<SiloMember[]> {
      const silos = ctx.membershipSnapshot().silos;
      return onlyActive ? silos.filter((m) => m.status === "active") : [...silos];
    }

    async getSimpleGrainStatistics(
      _hostsIds?: SiloAddress[] | null,
    ): Promise<SimpleGrainStatistic[]> {
      return ctx.gatherGrainStats();
    }

    async getDetailedGrainStatistics(
      types?: string[] | null,
      _hostsIds?: SiloAddress[] | null,
    ): Promise<DetailedGrainStatistic[]> {
      return ctx.gatherDetailedGrainStats(types ?? undefined);
    }

    async getActivationAddress(reference: unknown): Promise<GrainAddress | null> {
      const identity = grainReferenceIdentity(reference);
      if (identity === undefined) {
        throw new TypeError("getActivationAddress: reference is not a grain reference");
      }
      if (ctx.isStatelessWorker(identity.grainId.type)) {
        throw new Error(
          `Grain '${identity.grainId.toString()}' is a Stateless Worker. This type of grain can't be looked up by this method`,
        );
      }
      const addr = await ctx.lookupActivation(identity.grainId);
      return addr ?? null;
    }

    async getGrainActivationCount(grainReference: unknown): Promise<number> {
      const identity = grainReferenceIdentity(grainReference);
      if (identity === undefined) {
        throw new TypeError("getGrainActivationCount: reference is not a grain reference");
      }
      return ctx.activationCountFor(identity.grainId);
    }

    async forceActivationCollection(ageLimit: Duration): Promise<void> {
      await ctx.forceActivationCollection(durationToMs(ageLimit));
    }

    async sendControlCommandToProvider(
      _providerTypeName: string,
      providerName: string,
      command: number,
      arg?: unknown,
    ): Promise<unknown[]> {
      return ctx.sendControlCommandToProvider(providerName, command, arg);
    }
  }
  setGrainOptions(ManagementGrain, MANAGEMENT_GRAIN_TYPE, {});
  return ManagementGrain;
}
