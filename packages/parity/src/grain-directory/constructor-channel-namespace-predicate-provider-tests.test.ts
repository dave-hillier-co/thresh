// Ported from dotnet/orleans test/Orleans.BroadcastChannel.Tests/ConstructorChannelNamespacePredicateProviderTests.cs @ v10.1.0 (MIT).
import { describe, it } from "vitest";
import { orleansTest } from "@tsva/testing/orleans-test";

// `ConstructorChannelNamespacePredicateProvider` resolves an
// `IChannelNamespacePredicate` from a `"ctor:<TypeName>[,<arg>]"` pattern string —
// looking up a registered predicate type by name (optionally invoking a
// single-string-arg constructor, e.g. `RegexChannelNamespacePredicate`) and
// throwing `InvalidOperationException` for an unregistered arbitrary type.
// `[ImplicitChannelSubscription(typeof(...))]` / `[RegexImplicitChannelSubscription(...)]`
// attributes on a grain class drive this pattern generation.
//
// This framework's broadcast-channel implicit subscription
// (`@tsva/core/broadcast-channel`, `BROADCAST_CHANNEL_OBSERVER`,
// `implicitChannelSubscriptions` in `define-grain.ts`) matches a channel by
// namespace string directly — there is no `IChannelNamespacePredicate`
// abstraction, no pattern-string encoding/decoding, and no provider registry
// that maps a predicate type name back to a constructed predicate instance
// (with or without a constructor argument). The whole predicate-provider
// mechanism this file exercises does not exist here.

describe("UnitTests.ConstructorChannelNamespacePredicateProviderTests", () => {
  const NS = "UnitTests.ConstructorChannelNamespacePredicateProviderTests";

  orleansTest.gap("GAP-CHANNEL-NAMESPACE-PREDICATE", `${NS}.RegisteredPredicateType_Succeeds`);
  orleansTest.gap(
    "GAP-CHANNEL-NAMESPACE-PREDICATE",
    `${NS}.RegisteredPredicateTypeWithArg_Succeeds`,
  );
  orleansTest.gap("GAP-CHANNEL-NAMESPACE-PREDICATE", `${NS}.ArbitraryType_Throws`);
  orleansTest.gap("GAP-CHANNEL-NAMESPACE-PREDICATE", `${NS}.NonMatchingPrefix_ReturnsFalse`);

  // vitest requires at least one runtime test per file; all upstream Facts are
  // gapped above, so this placeholder keeps the file a valid suite.
  it.skip("(all tests in this file are orleansTest.gap — see above)", () => undefined);
});
