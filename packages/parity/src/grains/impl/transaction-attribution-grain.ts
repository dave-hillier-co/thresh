// Ported from dotnet/orleans src/Orleans.Transactions.TestKit.Base/Grains/
// TransactionAttributionGrain.cs @ v10.1.0 (MIT).
import { grain } from "@tsva/core/decorators";
import { Grain } from "@tsva/core/grain";
import type { GrainRuntime } from "@tsva/core/grain-runtime";
import {
  attributionGrainInterfaceFor,
  NoAttributionGrainInterface,
  SuppressAttributionGrainInterface,
  CreateOrJoinAttributionGrainInterface,
  CreateAttributionGrainInterface,
  JoinAttributionGrainInterface,
  SupportedAttributionGrainInterface,
  NotAllowedAttributionGrainInterface,
  type AttributionTierEntry,
  type TransactionAttributionGrain,
} from "@tsva/parity/grains/interfaces/transaction-attribution-grain-interfaces";

export {
  NoAttributionGrainInterface,
  SuppressAttributionGrainInterface,
  CreateOrJoinAttributionGrainInterface,
  CreateAttributionGrainInterface,
  JoinAttributionGrainInterface,
  SupportedAttributionGrainInterface,
  NotAllowedAttributionGrainInterface,
};

/**
 * Shared body for every attribution grain (upstream's static
 * `AttributionGrain.GetNestedTransactionIds`): read this turn's ambient
 * transaction id via the newly-exposed `GrainRuntime.getTransactionId()`,
 * then recurse one tier down, merging each child's per-tier results into this
 * call's own.
 */
async function nestedTransactionIds(
  runtime: GrainRuntime,
  tier: number,
  tiers: readonly AttributionTierEntry[][],
): Promise<(string | undefined)[][]> {
  const results: (string | undefined)[][] = [];
  results[tier] = [runtime.getTransactionId()];

  if (tiers.length === 0) return results;

  const [nextTier, ...nextTiers] = tiers;
  const tierResults = await Promise.all(
    nextTier!.map((entry) =>
      runtime
        .getGrain(attributionGrainInterfaceFor(entry.option), entry.key)
        .getNestedTransactionIds(tier + 1, nextTiers),
    ),
  );
  for (const result of tierResults) {
    for (let i = tier + 1; i < result.length; i++) {
      const existing = results[i];
      results[i] = existing !== undefined ? [...existing, ...result[i]!] : result[i]!;
    }
  }
  return results;
}

@grain({ name: "Orleans.Transactions.TestKit.NoAttributionGrain" })
export class NoAttributionGrain extends Grain implements TransactionAttributionGrain {
  getNestedTransactionIds(
    tier: number,
    tiers: readonly AttributionTierEntry[][],
  ): Promise<(string | undefined)[][]> {
    return nestedTransactionIds(this.runtime, tier, tiers);
  }
}

@grain({ name: "Orleans.Transactions.TestKit.SuppressAttributionGrain" })
export class SuppressAttributionGrain extends Grain implements TransactionAttributionGrain {
  getNestedTransactionIds(
    tier: number,
    tiers: readonly AttributionTierEntry[][],
  ): Promise<(string | undefined)[][]> {
    return nestedTransactionIds(this.runtime, tier, tiers);
  }
}

@grain({ name: "Orleans.Transactions.TestKit.CreateOrJoinAttributionGrain" })
export class CreateOrJoinAttributionGrain extends Grain implements TransactionAttributionGrain {
  getNestedTransactionIds(
    tier: number,
    tiers: readonly AttributionTierEntry[][],
  ): Promise<(string | undefined)[][]> {
    return nestedTransactionIds(this.runtime, tier, tiers);
  }
}

@grain({ name: "Orleans.Transactions.TestKit.CreateAttributionGrain" })
export class CreateAttributionGrain extends Grain implements TransactionAttributionGrain {
  getNestedTransactionIds(
    tier: number,
    tiers: readonly AttributionTierEntry[][],
  ): Promise<(string | undefined)[][]> {
    return nestedTransactionIds(this.runtime, tier, tiers);
  }
}

@grain({ name: "Orleans.Transactions.TestKit.JoinAttributionGrain" })
export class JoinAttributionGrain extends Grain implements TransactionAttributionGrain {
  getNestedTransactionIds(
    tier: number,
    tiers: readonly AttributionTierEntry[][],
  ): Promise<(string | undefined)[][]> {
    return nestedTransactionIds(this.runtime, tier, tiers);
  }
}

@grain({ name: "Orleans.Transactions.TestKit.SupportedAttributionGrain" })
export class SupportedAttributionGrain extends Grain implements TransactionAttributionGrain {
  getNestedTransactionIds(
    tier: number,
    tiers: readonly AttributionTierEntry[][],
  ): Promise<(string | undefined)[][]> {
    return nestedTransactionIds(this.runtime, tier, tiers);
  }
}

@grain({ name: "Orleans.Transactions.TestKit.NotAllowedAttributionGrain" })
export class NotAllowedAttributionGrain extends Grain implements TransactionAttributionGrain {
  getNestedTransactionIds(
    tier: number,
    tiers: readonly AttributionTierEntry[][],
  ): Promise<(string | undefined)[][]> {
    return nestedTransactionIds(this.runtime, tier, tiers);
  }
}
