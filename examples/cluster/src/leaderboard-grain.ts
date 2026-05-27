import { defineGrain } from "@tsva/core/define-grain";
import type { ILeaderboard, ScoreEntry } from "@tsva/example-cluster/interfaces";

/**
 * Keeps each player's best score. Holds no locks: calls arriving from every silo
 * in the cluster are routed to this one activation and run as serialized turns,
 * so the `Map` is only ever touched by one turn at a time.
 */
export const LeaderboardGrain = defineGrain<ILeaderboard>("Leaderboard", () => {
  const best = new Map<string, number>();

  return {
    record: async (player: string, score: number): Promise<void> => {
      const current = best.get(player);
      if (current === undefined || score > current) best.set(player, score);
    },

    top: async (limit = 10): Promise<ScoreEntry[]> =>
      [...best.entries()]
        .map(([player, score]) => ({ player, score }))
        .sort((a, b) => b.score - a.score)
        .slice(0, limit),
  };
});
