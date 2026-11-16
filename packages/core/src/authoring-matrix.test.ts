import { describe, expect, it } from "vitest";
import * as decorators from "@thresh/core/decorators";
import * as defineGrainModule from "@thresh/core/define-grain";

/**
 * The authoring feature matrix: a grain is written either as a class (decorators)
 * or as a factory (`defineGrain` and its hooks), over one shared substrate. Every
 * authoring feature is listed here with, for each path, either what the path
 * uses for it or the reason it is deliberately single-path.
 *
 * The matrix is checked against what the two modules actually export, so a
 * decorator or hook added to one module without an entry (and an answer for the
 * other path) fails `checkMatrix` below. See docs/guide/grains.md.
 *
 * Entries in `uses`:
 *  - a bare name is an export of that path's module and is verified to exist;
 *  - `option:<name>` is a `defineGrain` option, verified against
 *    `DEFINE_GRAIN_OPTION_NAMES` (functional path only);
 *  - `api:<description>` names a surface outside both modules (a base-class
 *    member, a symbol-keyed member, a builder method) and is not verified.
 */
type Path = { uses: readonly string[] } | { singlePath: string };

interface Feature {
  name: string;
  class: Path;
  functional: Path;
}

const matrix: readonly Feature[] = [
  {
    name: "registering a grain",
    class: { uses: ["grain"] },
    functional: { uses: ["defineGrain"] },
  },
  {
    name: "grain options (name, placement, collection age, ...)",
    class: { uses: ["api:@grain(options)"] },
    functional: {
      uses: [
        "option:name",
        "option:placement",
        "option:strategy",
        "option:role",
        "option:placementFilters",
        "option:stateless",
        "option:maxLocalWorkers",
        "option:collectionAgeSeconds",
        "option:immovable",
      ],
    },
  },
  {
    name: "interface contract (key kind, version, per-method options)",
    class: { uses: ["api:defineGrainInterface"] },
    functional: {
      uses: [
        "option:key",
        "option:interfaceOptions",
        "option:interfaceName",
        "option:version",
        "option:extension",
      ],
    },
  },
  {
    name: "full reentrancy",
    class: { uses: ["reentrant"] },
    functional: { uses: ["option:reentrant"] },
  },
  {
    name: "may-interleave admission predicate",
    class: { uses: ["mayInterleave"] },
    functional: { uses: ["useMayInterleave"] },
  },
  {
    name: "implicit stream subscription",
    class: { uses: ["implicitStreamSubscription"] },
    functional: { uses: ["option:implicitSubscriptions"] },
  },
  {
    name: "implicit broadcast-channel subscription",
    class: { uses: ["implicitChannelSubscription"] },
    functional: { uses: ["option:implicitChannelSubscriptions"] },
  },
  {
    name: "regular-expression implicit channel subscription",
    class: { uses: ["regexImplicitChannelSubscription"] },
    functional: { uses: ["option:regexImplicitChannelSubscriptions"] },
  },
  {
    name: "stream and channel observers",
    class: { uses: ["api:STREAM_SUBSCRIPTION_OBSERVER / BROADCAST_CHANNEL_OBSERVER members"] },
    functional: { uses: ["api:the same symbol-keyed members, returned from the factory"] },
  },
  {
    name: "incoming call filter on the grain itself",
    class: { uses: ["api:INCOMING_CALL_FILTER member"] },
    functional: { uses: ["api:INCOMING_CALL_FILTER member, returned from the factory"] },
  },
  {
    name: "persistent state facet",
    class: { uses: ["persistentState"] },
    functional: { uses: ["usePersistentState"] },
  },
  {
    name: "reducer state facet",
    class: { uses: ["reducerState"] },
    functional: { uses: ["useReducerState"] },
  },
  {
    name: "transactional state facet",
    class: { uses: ["transactionalState"] },
    functional: { uses: ["useTransactionalState"] },
  },
  {
    name: "durable value",
    class: { uses: ["durableState"] },
    functional: { uses: ["useDurableState"] },
  },
  {
    name: "durable dictionary",
    class: { uses: ["durableDictionary"] },
    functional: { uses: ["useDurableDictionary"] },
  },
  {
    name: "durable list",
    class: { uses: ["durableList"] },
    functional: { uses: ["useDurableList"] },
  },
  {
    name: "durable queue",
    class: { uses: ["durableQueue"] },
    functional: { uses: ["useDurableQueue"] },
  },
  {
    name: "durable set",
    class: { uses: ["durableSet"] },
    functional: { uses: ["useDurableSet"] },
  },
  {
    name: "write durable state (the one flush of every buffered durable mutation)",
    class: { uses: ["api:Grain.writeDurableState() on the class base"] },
    functional: { uses: ["useWriteDurableState"] },
  },
  {
    name: "third-party facets",
    class: { uses: ["grainFacet"] },
    functional: { uses: ["useGrainFacet"] },
  },
  {
    name: "activation and deactivation lifecycle",
    class: { uses: ["api:Grain.onActivate / onDeactivate overrides"] },
    functional: { uses: ["useOnActivate", "useOnDeactivate"] },
  },
  {
    name: "migration participation (dehydrate and rehydrate)",
    class: { uses: ["api:IGrainMigrationParticipant methods"] },
    functional: { uses: ["useOnDehydrate", "useOnRehydrate"] },
  },
  {
    name: "durable job handler",
    class: { uses: ["api:DURABLE_JOB_HANDLER member"] },
    functional: { uses: ["useDurableJobHandler"] },
  },
  {
    name: "per-silo dependencies",
    class: { uses: ["api:SiloBuilder.useGrainActivator (constructor arguments)"] },
    functional: { uses: ["useDependency"] },
  },
  {
    name: "identity and runtime access",
    class: { uses: ["api:Grain.id / runtime / getGrain via this"] },
    functional: { uses: ["useContext"] },
  },
  {
    name: "journaled grain base class",
    class: { uses: ["api:JournaledGrain"] },
    functional: {
      singlePath:
        "JournaledGrain is a base class: the application overrides its transition and log-view hooks. " +
        "A factory has no base class to override; it journals with useDurable* or folds events with useReducerState.",
    },
  },
  {
    name: "custom log-view storage",
    class: { uses: ["api:JournaledGrain custom storage hooks"] },
    functional: {
      singlePath:
        "Custom storage is a JournaledGrain extension point (readStateFromStorage, applyUpdatesToStorage, " +
        "clearStoredState); it follows JournaledGrain.",
    },
  },
  {
    name: "reducer grain (a whole grain that is one reducer)",
    class: {
      singlePath:
        "A whole-grain shortcut over useReducerState; a class grain gets the same behaviour from a reducerState field.",
    },
    functional: { uses: ["api:defineReducerGrain"] },
  },
];

/** Exports that are scaffolding, not authoring features. */
const NOT_FEATURES: Readonly<Record<string, string>> = {
  setupDepth: "runtime assertion that no factory is mid-setup; not something a grain author calls",
};

interface Surface {
  classExports: readonly string[];
  functionalExports: readonly string[];
  functionalOptions: readonly string[];
}

function functionNames(module: object): string[] {
  return Object.entries(module)
    .filter(([, value]) => typeof value === "function")
    .map(([name]) => name);
}

function checkMatrix(features: readonly Feature[], surface: Surface): string[] {
  const problems: string[] = [];
  const claimed = { class: new Set<string>(), functional: new Set<string>() };

  for (const feature of features) {
    for (const side of ["class", "functional"] as const) {
      const path = feature[side] as Path | undefined;
      if (path === undefined) {
        problems.push(`"${feature.name}" has no ${side} entry`);
        continue;
      }
      if ("singlePath" in path) {
        if (path.singlePath.trim() === "") {
          problems.push(`"${feature.name}" is single-path on the ${side} side with no reason`);
        }
        continue;
      }
      if (path.uses.length === 0) problems.push(`"${feature.name}" names nothing for ${side}`);
      for (const use of path.uses) {
        if (use.startsWith("api:")) continue;
        if (use.startsWith("option:")) {
          const option = use.slice("option:".length);
          if (side === "class") problems.push(`"${feature.name}" lists ${use} on the class side`);
          else if (!surface.functionalOptions.includes(option)) {
            problems.push(`"${feature.name}" names unknown defineGrain option ${option}`);
          }
          claimed.functional.add(use);
          continue;
        }
        const exported = side === "class" ? surface.classExports : surface.functionalExports;
        if (!exported.includes(use)) problems.push(`"${feature.name}" names missing export ${use}`);
        claimed[side].add(use);
      }
    }
    if ("singlePath" in (feature.class ?? {}) && "singlePath" in (feature.functional ?? {})) {
      problems.push(`"${feature.name}" is single-path on both sides`);
    }
  }

  for (const name of surface.classExports) {
    if (!claimed.class.has(name)) problems.push(`decorators export ${name} has no matrix entry`);
  }
  for (const name of surface.functionalExports) {
    if (name in NOT_FEATURES) continue;
    if (!claimed.functional.has(name)) {
      problems.push(`define-grain export ${name} has no matrix entry`);
    }
  }
  for (const option of surface.functionalOptions) {
    if (!claimed.functional.has(`option:${option}`)) {
      problems.push(`defineGrain option ${option} has no matrix entry`);
    }
  }
  return problems;
}

const surface: Surface = {
  classExports: functionNames(decorators),
  functionalExports: functionNames(defineGrainModule),
  functionalOptions: Object.keys(defineGrainModule.DEFINE_GRAIN_OPTION_NAMES),
};

describe("authoring feature matrix", () => {
  it("accounts for every export of the decorator and defineGrain modules", () => {
    expect(checkMatrix(matrix, surface)).toEqual([]);
  });

  it("derives its surface from the modules, not from this file", () => {
    expect(surface.classExports).toContain("mayInterleave");
    expect(surface.functionalExports).toContain("useMayInterleave");
    expect(surface.functionalOptions).toContain("regexImplicitChannelSubscriptions");
  });

  it("names a reason for every deliberately single-path feature", () => {
    const single = matrix.filter((f) => "singlePath" in f.class || "singlePath" in f.functional);
    expect(single.map((f) => f.name)).toEqual([
      "journaled grain base class",
      "custom log-view storage",
      "reducer grain (a whole grain that is one reducer)",
    ]);
  });

  for (const side of ["class", "functional"] as const) {
    it(`fails when a feature loses its ${side} entry`, () => {
      for (const feature of matrix) {
        const withoutEntry = matrix.map((f) => {
          if (f !== feature) return f;
          const copy: Partial<Feature> = { ...f };
          delete copy[side];
          return copy as Feature;
        });
        expect(checkMatrix(withoutEntry, surface), feature.name).not.toEqual([]);
      }
    });

    it(`fails when a ${side}-path export is no longer claimed by any feature`, () => {
      const exports = side === "class" ? surface.classExports : surface.functionalExports;
      for (const name of exports) {
        if (name in NOT_FEATURES) continue;
        const stripped = matrix.map((f) => {
          const path = f[side];
          if ("singlePath" in path) return f;
          return { ...f, [side]: { uses: path.uses.filter((u) => u !== name) } } as Feature;
        });
        // A feature left empty is itself a problem, so look for the specific one.
        expect(checkMatrix(stripped, surface).join("\n"), name).toMatch(
          /no matrix entry|names nothing/,
        );
      }
    });
  }

  it("fails when a new export appears in one module with no entry for it", () => {
    const grown: Surface = {
      ...surface,
      functionalExports: [...surface.functionalExports, "useSomethingNew"],
    };
    expect(checkMatrix(matrix, grown)).toEqual([
      "define-grain export useSomethingNew has no matrix entry",
    ]);
    const grownClass: Surface = { ...surface, classExports: [...surface.classExports, "newDeco"] };
    expect(checkMatrix(matrix, grownClass)).toEqual([
      "decorators export newDeco has no matrix entry",
    ]);
  });
});
