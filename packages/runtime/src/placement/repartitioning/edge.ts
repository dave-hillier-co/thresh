import type { GrainId } from "@tsva/core/grain-id";
import type { SiloAddress } from "@tsva/core/silo-address";

/**
 * One side of a communication `Edge`, ported from Orleans' `EdgeVertex`
 * (`src/Orleans.Core/Placement/Repartitioning/IActivationRepartitionerSystemTarget.cs`).
 */
export interface EdgeVertex {
  readonly id: GrainId;
  readonly silo: SiloAddress;
  readonly isMigratable: boolean;
}

export function edgeVertex(id: GrainId, silo: SiloAddress, isMigratable: boolean): EdgeVertex {
  return { id, silo, isMigratable };
}

/** A communication edge between a source and target grain, ported from Orleans' `Edge`. */
export interface Edge {
  readonly source: EdgeVertex;
  readonly target: EdgeVertex;
}

export function edge(source: EdgeVertex, target: EdgeVertex): Edge {
  return { source, target };
}

/** Returns a copy of `e` but with flipped source and target. */
export function flipEdge(e: Edge): Edge {
  return { source: e.target, target: e.source };
}
