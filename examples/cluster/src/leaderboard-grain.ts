import { grain } from "@tsva/core/decorators";
import { Grain } from "@tsva/core/grain";
import type { ILeaderboard, ScoreEntry } from "@tsva/example-cluster/interfaces";

/**
 * Keeps each player's best score. Holds no locks: calls arriving from every silo
 * in the cluster are routed to this one activation and run as serialized turns,
 * so the `Map` is only ever touched by one turn at a time.
 */
@grain()
export class LeaderboardGrain extends Grain implements ILeaderboard {
  private readonly best = new Map<string, number>();

  async record(player: string, score: number): Promise<void> {
    const current = this.best.get(player);
    if (current === undefined || score > current) this.best.set(player, score);
  }

  async top(limit = 10): Promise<ScoreEntry[]> {
    return [...this.best.entries()]
      .map(([player, score]) => ({ player, score }))
      .sort((a, b) => b.score - a.score)
      .slice(0, limit);
  }
}
