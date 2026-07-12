import { FrequentItemCollection } from "./frequent-item-collection";
import type { Edge } from "./edge";

/**
 * Tracks the most frequent communication edges (grain-pair call counts) behind the activation
 * repartitioner, ported from Orleans' `FrequentEdgeCounter`
 * (`src/Orleans.Runtime/Placement/Repartitioning/FrequentItemCollection.cs`).
 */
export class FrequentEdgeCounter extends FrequentItemCollection<Edge> {
  protected override getKey(element: Edge): bigint {
    const hi = BigInt(element.source.id.getUniformHashCode() >>> 0);
    const lo = BigInt(element.target.id.getUniformHashCode() >>> 0);
    return (hi << 32n) | lo;
  }

  clear(): void {
    this.clearCore();
  }

  remove(element: Edge): void {
    this.removeCore(this.getKey(element));
  }
}
