import { describe, expect, it } from "vitest";
import {
  ConstructorChannelNamespacePredicateProvider,
  ExactMatchChannelNamespacePredicate,
  RegexChannelNamespacePredicate,
  registerChannelNamespacePredicateType,
  unregisterChannelNamespacePredicateType,
  type IChannelNamespacePredicate,
} from "./channel-namespace-predicate";
import { regexImplicitChannelSubscription } from "./decorators";
import { getGrainMetadata, setGrainOptions, type GrainConstructor } from "./grain-metadata";

describe("ExactMatchChannelNamespacePredicate", () => {
  it("matches only the exact namespace", () => {
    const predicate = new ExactMatchChannelNamespacePredicate("alerts");
    expect(predicate.isMatch("alerts")).toBe(true);
    expect(predicate.isMatch("alerts.other")).toBe(false);
  });

  it("encodes to a namespace: pattern string", () => {
    const predicate = new ExactMatchChannelNamespacePredicate("alerts");
    expect(predicate.patternString).toBe("namespace:alerts");
  });
});

describe("RegexChannelNamespacePredicate", () => {
  it("matches namespaces satisfying the regular expression", () => {
    const predicate = new RegexChannelNamespacePredicate("str-[a-zA-Z]+");
    expect(predicate.isMatch("str-JumboJet")).toBe(true);
    expect(predicate.isMatch("other")).toBe(false);
  });

  it("encodes to a regex: pattern string", () => {
    const predicate = new RegexChannelNamespacePredicate("str-[a-zA-Z]+");
    expect(predicate.patternString).toBe("regex:str-[a-zA-Z]+");
  });
});

describe("ConstructorChannelNamespacePredicateProvider", () => {
  it("resolves a registered predicate type with no constructor argument", () => {
    class NoArgPredicate implements IChannelNamespacePredicate {
      isMatch(): boolean {
        return true;
      }
      get patternString(): string {
        return ConstructorChannelNamespacePredicateProvider.formatPattern("NoArgPredicate");
      }
    }
    registerChannelNamespacePredicateType("NoArgPredicate", NoArgPredicate);
    try {
      const provider = new ConstructorChannelNamespacePredicateProvider();
      const pattern = ConstructorChannelNamespacePredicateProvider.formatPattern("NoArgPredicate");

      const result = provider.tryGetPredicate(pattern);

      expect(result.matched).toBe(true);
      expect(result.matched && result.predicate).toBeInstanceOf(NoArgPredicate);
    } finally {
      unregisterChannelNamespacePredicateType("NoArgPredicate");
    }
  });

  it("resolves a registered predicate type with a constructor argument", () => {
    const provider = new ConstructorChannelNamespacePredicateProvider();
    const pattern = ConstructorChannelNamespacePredicateProvider.formatPattern(
      "RegexChannelNamespacePredicate",
      "str-[a-zA-Z]+",
    );

    const result = provider.tryGetPredicate(pattern);

    expect(result.matched).toBe(true);
    expect(result.matched && result.predicate).toBeInstanceOf(RegexChannelNamespacePredicate);
    expect(result.matched && result.predicate.isMatch("str-JumboJet")).toBe(true);
  });

  it("throws for an unregistered arbitrary type name", () => {
    const provider = new ConstructorChannelNamespacePredicateProvider();
    const pattern = "ctor:SomeUnregisteredType";

    expect(() => provider.tryGetPredicate(pattern)).toThrow();
  });

  it("returns unmatched for a pattern with a different prefix", () => {
    const provider = new ConstructorChannelNamespacePredicateProvider();

    const result = provider.tryGetPredicate("namespace:test");

    expect(result.matched).toBe(false);
  });
});

describe("regexImplicitChannelSubscription", () => {
  it("records a ctor: pattern string a provider can resolve back to a RegexChannelNamespacePredicate", () => {
    @regexImplicitChannelSubscription("str-[a-zA-Z]+")
    class RegexSubscribedGrain {}
    setGrainOptions(RegexSubscribedGrain as GrainConstructor, "RegexSubscribed", {});

    const metadata = getGrainMetadata(RegexSubscribedGrain as GrainConstructor);
    expect(metadata?.broadcastSubscriptions).toHaveLength(1);

    const provider = new ConstructorChannelNamespacePredicateProvider();
    const result = provider.tryGetPredicate(metadata!.broadcastSubscriptions[0]!);
    expect(result.matched).toBe(true);
    expect(result.matched && result.predicate).toBeInstanceOf(RegexChannelNamespacePredicate);
    expect(result.matched && result.predicate.isMatch("str-JumboJet")).toBe(true);
  });
});
