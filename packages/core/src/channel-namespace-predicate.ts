/**
 * Channel-namespace predicates: a generalization of the exact-namespace match
 * `@implicitChannelSubscription` uses, mirroring Orleans'
 * `IChannelNamespacePredicate` / `[ImplicitChannelSubscription(typeof(...))]` /
 * `[RegexImplicitChannelSubscription]`
 * (`src/Orleans.BroadcastChannel/SubscriberTable/Predicates/*.cs`).
 *
 * A predicate decides whether a grain subscribes to a published channel's
 * namespace and encodes itself as a `patternString` — the same string that
 * flows through `markBroadcastSubscription` / `GrainMetadata.broadcastSubscriptions`
 * so the mark-time and match-time representations stay a plain string.
 */
export interface IChannelNamespacePredicate {
  /** Whether a grain declaring this predicate should subscribe to `namespace`. */
  isMatch(namespace: string): boolean;
  /** Encodes this predicate so a provider can reconstruct it elsewhere. */
  readonly patternString: string;
}

/**
 * Matches a channel namespace by exact string equality — what
 * `@implicitChannelSubscription(namespace)` has always done. Orleans'
 * `ExactMatchChannelNamespacePredicate`.
 */
export class ExactMatchChannelNamespacePredicate implements IChannelNamespacePredicate {
  static readonly prefix = "namespace:";

  constructor(private readonly namespace: string) {}

  isMatch(namespace: string): boolean {
    return namespace === this.namespace;
  }

  get patternString(): string {
    return `${ExactMatchChannelNamespacePredicate.prefix}${this.namespace}`;
  }
}

/**
 * Matches a channel namespace by regular expression. Orleans'
 * `RegexChannelNamespacePredicate` (`[RegexImplicitChannelSubscription]`).
 */
export class RegexChannelNamespacePredicate implements IChannelNamespacePredicate {
  static readonly prefix = "regex:";

  private readonly regex: RegExp;

  constructor(pattern: string) {
    if (pattern === undefined || pattern === null) {
      throw new Error("RegexChannelNamespacePredicate requires a pattern");
    }
    this.regex = new RegExp(pattern);
  }

  isMatch(namespace: string): boolean {
    return this.regex.test(namespace);
  }

  get patternString(): string {
    return `${RegexChannelNamespacePredicate.prefix}${this.regex.source}`;
  }
}

/**
 * A predicate class constructible with zero args or a single string argument.
 * Predicate constructors that require the argument (e.g.
 * `RegexChannelNamespacePredicate`) are registered as this type too — the
 * provider only ever omits the argument when the pattern string carries none,
 * which callers control by choosing whether to encode one.
 */
export type ChannelNamespacePredicateConstructor = new (arg?: string) => IChannelNamespacePredicate;

/**
 * Maps a registered predicate type name to its constructor. Orleans resolves
 * `[ImplicitChannelSubscription(typeof(Foo))]` via CLR reflection
 * (`Type.GetType`); without reflection over arbitrary classes, this framework
 * requires a predicate type to be registered by name before a `"ctor:..."`
 * pattern naming it can be resolved. Module-level by default — register once
 * at startup (or from a test) with `registerChannelNamespacePredicateType`.
 */
const predicateTypeRegistry = new Map<string, ChannelNamespacePredicateConstructor>();

/** Register a predicate type so `ConstructorChannelNamespacePredicateProvider` can construct it by name. */
export function registerChannelNamespacePredicateType(
  name: string,
  ctor: ChannelNamespacePredicateConstructor,
): void {
  predicateTypeRegistry.set(name, ctor);
}

/** Remove a previously registered predicate type (mainly for test isolation). */
export function unregisterChannelNamespacePredicateType(name: string): void {
  predicateTypeRegistry.delete(name);
}

registerChannelNamespacePredicateType(
  "RegexChannelNamespacePredicate",
  RegexChannelNamespacePredicate as ChannelNamespacePredicateConstructor,
);
registerChannelNamespacePredicateType(
  "ExactMatchChannelNamespacePredicate",
  ExactMatchChannelNamespacePredicate as ChannelNamespacePredicateConstructor,
);

/** The outcome of resolving a pattern string through a predicate provider. */
export type ChannelNamespacePredicateLookup =
  | { matched: true; predicate: IChannelNamespacePredicate }
  | { matched: false };

/**
 * Resolves an `IChannelNamespacePredicate` from a `"ctor:<TypeName>[:<arg>]"`
 * pattern string: looks up `TypeName` in the predicate type registry,
 * optionally invoking its single-string-argument constructor, and throws for a
 * name that isn't registered. Orleans'
 * `ConstructorChannelNamespacePredicateProvider` (`DefaultStreamNamespacePredicateProvider.cs`) —
 * there, `TryGetPredicate` resolves the type via CLR reflection and throws
 * `InvalidOperationException` only when the resolved type doesn't implement
 * `IChannelNamespacePredicate`; here, an unregistered name plays that same
 * role since there is no reflection fallback.
 */
export class ConstructorChannelNamespacePredicateProvider {
  static readonly prefix = "ctor";

  /** Formats the pattern string a registered predicate type (optionally with a constructor argument) encodes to. */
  static formatPattern(typeName: string, constructorArgument?: string): string {
    const { prefix } = ConstructorChannelNamespacePredicateProvider;
    return constructorArgument === undefined
      ? `${prefix}:${typeName}`
      : `${prefix}:${typeName}:${constructorArgument}`;
  }

  /**
   * Resolves `pattern`. Returns `{ matched: false }` when `pattern` doesn't
   * carry the `"ctor:"` prefix this provider owns; throws when it does but
   * names a type that isn't registered.
   */
  tryGetPredicate(pattern: string): ChannelNamespacePredicateLookup {
    const { prefix } = ConstructorChannelNamespacePredicateProvider;
    if (!pattern.startsWith(`${prefix}:`)) return { matched: false };

    const start = prefix.length + 1;
    const separator = pattern.indexOf(":", start);
    const typeName = separator < 0 ? pattern.slice(start) : pattern.slice(start, separator);
    const arg = separator < 0 ? undefined : pattern.slice(separator + 1);

    const ctor = predicateTypeRegistry.get(typeName);
    if (ctor === undefined) {
      throw new Error(
        `Type "${typeName}" is not a registered channel namespace predicate. Register it with ` +
          `registerChannelNamespacePredicateType before it can be resolved from a "ctor:" pattern.`,
      );
    }

    const predicate = arg === undefined ? new ctor() : new ctor(arg);
    return { matched: true, predicate };
  }
}

const constructorPredicateProvider = new ConstructorChannelNamespacePredicateProvider();

/**
 * Whether a grain's recorded broadcast-subscription pattern string matches a
 * published channel's `namespace` — the single chokepoint `ClusterNode.
 * broadcastGrainTypes` uses for BOTH the plain exact-namespace strings
 * `@implicitChannelSubscription` records (mark-time and match-time both plain
 * strings, so compared directly) and the `"ctor:...".`-prefixed patterns
 * `@regexImplicitChannelSubscription` records (resolved through the
 * `ConstructorChannelNamespacePredicateProvider` registry and matched via
 * `IChannelNamespacePredicate.isMatch`).
 */
export function matchesChannelNamespace(pattern: string, namespace: string): boolean {
  const lookup = constructorPredicateProvider.tryGetPredicate(pattern);
  if (lookup.matched) return lookup.predicate.isMatch(namespace);
  return pattern === namespace;
}
