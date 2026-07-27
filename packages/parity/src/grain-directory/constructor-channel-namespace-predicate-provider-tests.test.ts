// Ported from dotnet/orleans test/Orleans.BroadcastChannel.Tests/ConstructorChannelNamespacePredicateProviderTests.cs @ v10.1.0 (MIT).
import { describe, expect } from "vitest";
import { orleansTest } from "@thresh/testing/orleans-test";
import {
  ConstructorChannelNamespacePredicateProvider,
  RegexChannelNamespacePredicate,
  registerChannelNamespacePredicateType,
  type IChannelNamespacePredicate,
} from "@thresh/core/channel-namespace-predicate";

// `ConstructorChannelNamespacePredicateProvider` resolves an
// `IChannelNamespacePredicate` from a `"ctor:<TypeName>[:<arg>]"` pattern string —
// looking up a registered predicate type by name (optionally invoking a
// single-string-arg constructor, e.g. `RegexChannelNamespacePredicate`) and
// throwing for an unregistered arbitrary type name. Orleans resolves the type
// via CLR reflection (`Type.GetType`) and throws `InvalidOperationException`
// only when the resolved type doesn't implement `IChannelNamespacePredicate`;
// without reflection over arbitrary classes, this framework requires a
// predicate type to be registered by name first (`registerChannelNamespacePredicateType`),
// so an unregistered name plays the same role as upstream's "not a valid
// predicate" type.
class TestChannelPredicate implements IChannelNamespacePredicate {
  isMatch(_namespace: string): boolean {
    return true;
  }
  get patternString(): string {
    return ConstructorChannelNamespacePredicateProvider.formatPattern("TestChannelPredicate");
  }
}
registerChannelNamespacePredicateType("TestChannelPredicate", TestChannelPredicate);

describe("UnitTests.ConstructorChannelNamespacePredicateProviderTests", () => {
  const createProvider = () => new ConstructorChannelNamespacePredicateProvider();

  orleansTest(
    "UnitTests.ConstructorChannelNamespacePredicateProviderTests.RegisteredPredicateType_Succeeds",
    () => {
      const provider = createProvider();
      const pattern =
        ConstructorChannelNamespacePredicateProvider.formatPattern("TestChannelPredicate");

      const result = provider.tryGetPredicate(pattern);

      expect(result.matched).toBe(true);
      expect(result.matched && result.predicate).toBeInstanceOf(TestChannelPredicate);
    },
  );

  orleansTest(
    "UnitTests.ConstructorChannelNamespacePredicateProviderTests.RegisteredPredicateTypeWithArg_Succeeds",
    () => {
      const provider = createProvider();
      const pattern = ConstructorChannelNamespacePredicateProvider.formatPattern(
        "RegexChannelNamespacePredicate",
        "str-[a-zA-Z]+",
      );

      const result = provider.tryGetPredicate(pattern);

      expect(result.matched).toBe(true);
      expect(result.matched && result.predicate).toBeInstanceOf(RegexChannelNamespacePredicate);
      expect(result.matched && result.predicate.isMatch("str-JumboJet")).toBe(true);
    },
  );

  orleansTest(
    "UnitTests.ConstructorChannelNamespacePredicateProviderTests.ArbitraryType_Throws",
    () => {
      const provider = createProvider();
      const pattern = `ctor:UnregisteredArbitraryType`;

      expect(() => provider.tryGetPredicate(pattern)).toThrow();
    },
  );

  orleansTest(
    "UnitTests.ConstructorChannelNamespacePredicateProviderTests.NonMatchingPrefix_ReturnsFalse",
    () => {
      const provider = createProvider();

      const result = provider.tryGetPredicate("namespace:test");

      expect(result.matched).toBe(false);
    },
  );
});
