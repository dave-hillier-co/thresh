import type { GrainId } from "@tsva/core/grain-id";
import { getGrainFacetFields } from "@tsva/core/grain-facet-metadata";
import type { GrainFacetRegistry } from "@tsva/persistence/grain-facet-registry";

/**
 * Inject a third-party facet into each `@grainFacet` field of a grain
 * instance, before `onActivate`. Wired into the catalog by the hosting layer
 * alongside `bindPersistentStates`/`bindReducerStates`/`bindDurableStates` —
 * the general-purpose sibling of those built-in facet activators, for facet
 * families a third party registers itself.
 */
export async function bindGrainFacets(
  instance: object,
  grainId: GrainId,
  registry: GrainFacetRegistry,
): Promise<void> {
  for (const field of getGrainFacetFields(instance)) {
    const factory = registry.resolve(field.kind, field.providerName);
    const facet = await factory(field.config, grainId);
    (instance as Record<string, unknown>)[field.fieldName] = facet;
  }
}
