import type { GrainType } from "@thresh/core/grain-type";
import type { SiloAddress } from "@thresh/core/silo-address";
import type { PlacementFilter } from "@thresh/runtime/placement/placement-filter";
import type { PlacementContext } from "@thresh/runtime/placement/placement-strategy";

/**
 * Reads a silo's advertised metadata from the placement context, defaulting to
 * an empty map when the context carries no `siloMetadata` accessor or the silo
 * advertises none (mirrors upstream's `SiloMetadata.Empty`/`GetValueOrDefault`).
 */
function metadataOf(
  context: PlacementContext,
  silo: SiloAddress,
): Readonly<Record<string, string>> {
  return context.siloMetadata?.(silo) ?? {};
}

/**
 * Keeps only candidates whose advertised metadata matches the *local* (calling)
 * silo's own metadata for every key in `keys`, with no fallback — an empty
 * result is valid (Orleans `RequiredMatchSiloMetadataPlacementFilterDirector`).
 * Distinct from `MetadataMatchFilter`, which matches against a fixed,
 * statically-configured value map rather than the local silo's live metadata.
 */
export class RequiredMatchSiloMetadataPlacementFilterDirector implements PlacementFilter {
  constructor(private readonly keys: readonly string[]) {}

  filter(
    _grainType: GrainType,
    candidates: readonly SiloAddress[],
    context: PlacementContext,
  ): readonly SiloAddress[] {
    if (this.keys.length === 0) return candidates;
    const local = metadataOf(context, context.localSilo);
    return candidates.filter((silo) => {
      const remote = metadataOf(context, silo);
      return this.keys.every((key) => local[key] === remote[key]);
    });
  }
}

/**
 * Preferentially filters candidates down to those whose metadata best matches
 * the *local* silo's own metadata across `orderedMetadataKeys` (earlier keys
 * are least important; the last key is most important), but only so long as
 * doing so leaves at least `minCandidates` silos — otherwise it relaxes the
 * match (fewer keys) or, failing that, falls back to every candidate
 * unfiltered (Orleans `PreferredMatchSiloMetadataPlacementFilterDirector`).
 */
export class PreferredMatchSiloMetadataPlacementFilterDirector implements PlacementFilter {
  constructor(
    private readonly orderedMetadataKeys: readonly string[],
    private readonly minCandidates: number = 2,
  ) {}

  filter(
    _grainType: GrainType,
    candidates: readonly SiloAddress[],
    context: PlacementContext,
  ): readonly SiloAddress[] {
    const local = metadataOf(context, context.localSilo);
    if (Object.keys(local).length === 0) return candidates;

    const siloList = [...candidates];
    if (siloList.length <= this.minCandidates) return siloList;

    const keys = this.orderedMetadataKeys;
    let maxScore = 0;
    const siloScores = new Array<number>(siloList.length).fill(0);
    const scoreCounts = new Array<number>(keys.length + 1).fill(0);
    for (let i = 0; i < siloList.length; i++) {
      const remote = metadataOf(context, siloList[i]!);
      let score = 0;
      for (let j = keys.length - 1; j >= 0; j--) {
        const key = keys[j]!;
        if (key in remote && key in local && remote[key] === local[key]) {
          score += 1;
        } else {
          break;
        }
      }
      siloScores[i] = score;
      maxScore = Math.max(maxScore, score);
      scoreCounts[score] = (scoreCounts[score] ?? 0) + 1;
    }

    if (maxScore === 0) return siloList;

    let candidateCount = 0;
    let scoreCutOff = keys.length;
    for (let i = scoreCounts.length - 1; i >= 0; i--) {
      candidateCount += scoreCounts[i] ?? 0;
      if (candidateCount >= this.minCandidates) {
        scoreCutOff = i;
        break;
      }
    }

    return siloList.filter((_, i) => (siloScores[i] ?? 0) >= scoreCutOff);
  }
}
