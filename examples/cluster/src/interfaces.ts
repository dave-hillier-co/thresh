import { defineGrainInterface } from "@tsva/core/grain-interface";
import type { GrainWithStringKey } from "@tsva/core/key-kinds";

export interface ScoreEntry {
  player: string;
  score: number;
}

/**
 * A single, cluster-wide leaderboard. Game servers on any silo record scores
 * against it; because a grain has exactly one activation, every `record` — no
 * matter which silo it originates on — lands on the same in-memory tally.
 */
export interface ILeaderboard extends GrainWithStringKey {
  record(player: string, score: number): Promise<void>;
  top(limit?: number): Promise<ScoreEntry[]>;
}

export const ILeaderboard = defineGrainInterface<ILeaderboard>("example.ILeaderboard", {
  options: { top: { readOnly: true } },
});
