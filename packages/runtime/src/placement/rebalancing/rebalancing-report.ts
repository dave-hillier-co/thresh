/**
 * The rebalancer's externally observable status, a faithful port of
 * Orleans' `RebalancingReport` (`Orleans.Core/Placement/Rebalancing/RebalancingReport.cs`).
 * The elected worker tracks the latest report after each cycle; the host can
 * surface it for diagnostics.
 */

/** Whether the elected worker is actively running cycles or has been suspended. */
export type RebalancerStatus = "executing" | "suspended";

/**
 * A snapshot of the rebalancer's state after a cycle: which silo is the elected
 * host, whether it is executing or suspended, the current cluster imbalance
 * `∈ [0,1]`, and how many activations each silo dispersed (shed) or acquired
 * (received) in the latest cycle.
 */
export interface RebalancingReport {
  /** The ring key of the elected silo currently running the rebalancer. */
  host: string;
  /** Whether the worker is executing cycles or suspended. */
  status: RebalancerStatus;
  /** The cluster imbalance `(maxEntropy − entropy) / maxEntropy ∈ [0,1]` at the last cycle. */
  clusterImbalance: number;
  /** Per-silo count of activations shed in the latest cycle, keyed by ring key. */
  dispersed: ReadonlyMap<string, number>;
  /** Per-silo count of activations received in the latest cycle, keyed by ring key. */
  acquired: ReadonlyMap<string, number>;
}
