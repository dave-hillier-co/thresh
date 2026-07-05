// Ported from dotnet/orleans src/Orleans.Transactions.TestKit.Base/Grains/
// ITransactionAttributionGrain.cs @ v10.1.0 (MIT).
//
// Upstream models each `TransactionOption` as its own tiny grain interface
// carrying a single `[Transaction(option)]`-attributed method, plus a
// "no attribute" variant, then a `GetTransactionAttributionGrain(id, option)`
// factory extension that picks the right one. This framework attaches
// `InvokeMethodOptions.transaction` per method on `defineGrainInterface`
// rather than via a reflected attribute, so the same shape falls out
// naturally: one `defineGrainInterface` per option, all implementing the
// same `TransactionAttributionGrain` TS shape.
import { defineGrainInterface, type GrainInterface } from "@tsva/core/grain-interface";
import type { GrainWithStringKey } from "@tsva/core/key-kinds";
import type { TransactionOption } from "@tsva/core/transaction-info";

/** A `TransactionOption`, or `"none"` for a method with no `[Transaction]` attribute at all. */
export type AttributionOption = TransactionOption | "none";

/** One entry in a call tree tier: which grain to call, and under which option. */
export interface AttributionTierEntry {
  readonly key: string;
  readonly option: AttributionOption;
}

/**
 * Recursive call-tree probe (Orleans `ITransactionAttributionGrain`):
 * `getNestedTransactionIds(tier, tiers)` records the ambient transaction id
 * (or `undefined`) for its own tier, then — for each entry in `tiers[0]` —
 * calls the corresponding grain's `getNestedTransactionIds(tier + 1,
 * tiers.slice(1))` and merges the results. The returned array is indexed by
 * tier: `result[0]` is always this call's own `[id]`; deeper indices collect
 * every descendant's id at that depth, in call order.
 */
export interface TransactionAttributionGrain extends GrainWithStringKey {
  getNestedTransactionIds(
    tier: number,
    tiers: readonly AttributionTierEntry[][],
  ): Promise<(string | undefined)[][]>;
}

function attributionInterface(
  name: string,
  option?: TransactionOption,
): GrainInterface<TransactionAttributionGrain> {
  return defineGrainInterface<TransactionAttributionGrain>(name, {
    options: option === undefined ? {} : { getNestedTransactionIds: { transaction: option } },
  });
}

export const NoAttributionGrainInterface = attributionInterface(
  "Orleans.Transactions.TestKit.INoAttributionGrain",
);
export const SuppressAttributionGrainInterface = attributionInterface(
  "Orleans.Transactions.TestKit.ISuppressAttributionGrain",
  "suppress",
);
export const CreateOrJoinAttributionGrainInterface = attributionInterface(
  "Orleans.Transactions.TestKit.ICreateOrJoinAttributionGrain",
  "createOrJoin",
);
export const CreateAttributionGrainInterface = attributionInterface(
  "Orleans.Transactions.TestKit.ICreateAttributionGrain",
  "create",
);
export const JoinAttributionGrainInterface = attributionInterface(
  "Orleans.Transactions.TestKit.IJoinAttributionGrain",
  "join",
);
export const SupportedAttributionGrainInterface = attributionInterface(
  "Orleans.Transactions.TestKit.ISupportedAttributionGrain",
  "supported",
);
export const NotAllowedAttributionGrainInterface = attributionInterface(
  "Orleans.Transactions.TestKit.INotAllowedAttributionGrain",
  "notAllowed",
);

/** Mirrors upstream's `TransactionAttributionGrainExtensions.GetTransactionAttributionGrain`. */
export function attributionGrainInterfaceFor(
  option: AttributionOption,
): GrainInterface<TransactionAttributionGrain> {
  switch (option) {
    case "none":
      return NoAttributionGrainInterface;
    case "suppress":
      return SuppressAttributionGrainInterface;
    case "createOrJoin":
      return CreateOrJoinAttributionGrainInterface;
    case "create":
      return CreateAttributionGrainInterface;
    case "join":
      return JoinAttributionGrainInterface;
    case "supported":
      return SupportedAttributionGrainInterface;
    case "notAllowed":
      return NotAllowedAttributionGrainInterface;
  }
}
