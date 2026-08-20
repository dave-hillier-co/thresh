import { defineGrain } from "@thresh/core/define-grain";

export interface ScoreEntry {
  player: string;
  score: number;
}

interface LeaderboardMethods {
  record(player: string, score: number): Promise<void>;
  top(limit?: number): Promise<ScoreEntry[]>;
}

/**
 * A single, cluster-wide leaderboard. Game servers on any silo record scores
 * against it; because a grain has exactly one activation, every `record` — no
 * matter which silo it originates on — lands on the same in-memory tally.
 *
 * Keeps each player's best score. Holds no locks: calls arriving from every silo
 * in the cluster are routed to this one activation and run as serialized turns,
 * so the `Map` is only ever touched by one turn at a time.
 */
export const Leaderboard = defineGrain<LeaderboardMethods>(
  "Leaderboard",
  () => {
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
  },
  { interfaceOptions: { top: { readOnly: true } } },
);
