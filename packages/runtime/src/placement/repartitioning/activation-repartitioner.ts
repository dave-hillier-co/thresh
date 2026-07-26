/**
 * The activation repartitioner's per-silo protocol — a scoped port of Orleans'
 * `ActivationRepartitioner`
 * (`src/Orleans.Runtime/Placement/Repartitioning/ActivationRepartitioner.cs`,
 * `.MessageSink.cs`) moving individual grain activations near their most
 * frequent communication partners, distinct from the activation *rebalancer*
 * (load-leveling, `@thresh/runtime/placement/rebalancing`).
 *
 * Orleans reaches this protocol via an `ISystemTarget` (a per-silo
 * grain-factory reference resolved through `IGrainFactory`); this framework
 * has no system-target substrate, so this is instead a plain, host-local
 * object one per silo (owned by `ClusterNode`, exactly how the rebalancer's
 * `ActivationRebalancerWorker` is host-local — see that file's doc), reached
 * over the wire via a `system: "repartition"` message
 * (`ClusterNode.handleRepartitionRequest`/`sendAcceptExchangeRequest`).
 *
 * Deliberate scope reductions from upstream, all inert for the ported tests
 * (`packages/parity/src/placement/default-tolerance-tests.test.ts`,
 * `custom-tolerance-tests.test.ts`), which trigger every exchange manually
 * with `MinRoundPeriod`/`MaxRoundPeriod` set so high the real timer would
 * "practically never fire" (the upstream fixtures' own words):
 *  - No background timer (`RegisterTimer`/`ILifecycleParticipant`): the
 *    protocol only runs when `triggerExchangeRequest()` is called, avoiding a
 *    timer that would otherwise need explicit silo-shutdown teardown.
 *  - No `AnchoredGrainsFilter` (probabilistic local-vertex anchoring): starts
 *    empty every round in the upstream tests too (`ResetCounters` before each
 *    test), so it never influences any of these scenarios; omitting it avoids
 *    porting a bloom-filter generation-rotation scheme with no observable
 *    effect here.
 *  - No `DeactivatedGrainQueue` (best-effort tracking of grains that
 *    deactivated mid-round, folded into the next round's affected-edge
 *    pruning): none of the ported scenarios deactivate a grain out from under
 *    a live round.
 *  - No mutual-exchange tie-break (`_currentExchangeSilo`): the ported tests
 *    never trigger two silos' protocols concurrently against each other.
 *  - `recoveryPeriodMs` is measured off the wall clock (`Date.now()`), not an
 *    injected `TimeProvider`: the ported tests configure it near zero and
 *    poll for the outcome rather than asserting on timing.
 */
import type { GrainId } from "@thresh/core/grain-id";
import type { MembershipService } from "@thresh/core/membership";
import { activeSilos } from "@thresh/core/membership";
import type { SiloAddress } from "@thresh/core/silo-address";
import type { HeapElement } from "@thresh/runtime/placement/repartitioning/max-heap";
import { MaxHeap } from "@thresh/runtime/placement/repartitioning/max-heap";
import type { Edge } from "@thresh/runtime/placement/repartitioning/edge";
import { edge, edgeVertex } from "@thresh/runtime/placement/repartitioning/edge";
import { FrequentEdgeCounter } from "@thresh/runtime/placement/repartitioning/frequent-edge-counter";
import type { ActivationRepartitionerOptions } from "@thresh/runtime/placement/repartitioning/activation-repartitioner-options";

/**
 * Controls the tolerable imbalance between a pair of exchanging silos, ported
 * from Orleans' `IImbalanceToleranceRule`.
 */
export interface ImbalanceToleranceRule {
  isSatisfiedBy(imbalance: number): boolean;
}

/**
 * The cluster-size-aware default tolerance rule, ported from Orleans'
 * `RebalancerCompatibleRule` minus its coexistence with the activation
 * rebalancer's live cluster-imbalance report (`clusterImbalance` always `0`
 * here, matching a cluster with no rebalancer registered — `toleranceFactor
 * === 1`).
 */
export function rebalancerCompatibleTolerance(
  membership: MembershipService,
): ImbalanceToleranceRule {
  return {
    isSatisfiedBy(imbalance: number): boolean {
      const activeCount = activeSilos(membership.current()).length;
      const percentageOfBaseline = Math.max(100 / (1 + Math.exp(0.07 * activeCount - 4.8)), 10);
      const pairwiseImbalance = Math.round(10.1 * percentageOfBaseline);
      return imbalance <= pairwiseImbalance;
    },
  };
}

/** A candidate grain offered for exchange, ported from Orleans' `CandidateVertex`. */
export interface CandidateVertex {
  readonly id: GrainId;
  readonly accumulatedTransferScore: number;
  readonly connectedVertices: ReadonlyArray<{
    readonly id: GrainId;
    readonly transferScore: number;
  }>;
}

/** Ported from Orleans' `AcceptExchangeRequest`. */
export interface AcceptExchangeRequest {
  readonly sendingSilo: SiloAddress;
  readonly exchangeSet: readonly CandidateVertex[];
  readonly activationCountSnapshot: number;
}

/** Ported from Orleans' `AcceptExchangeResponse`. */
export interface AcceptExchangeResponse {
  readonly type: "success" | "exchangedRecently" | "mutualExchangeAttempt";
  readonly acceptedGrainIds: readonly GrainId[];
  readonly givenGrainIds: readonly GrainId[];
}

export interface ActivationRepartitionerDeps {
  local: SiloAddress;
  membership: MembershipService;
  options: ActivationRepartitionerOptions;
  toleranceRule: ImbalanceToleranceRule;
  /** This silo's local live activation count (Orleans `ActivationDirectory.Count`). */
  activationCount: () => number;
  /** Whether `grainId`'s type may be moved by the repartitioner (Orleans `[Immovable(Repartitioner)]`). */
  isMigratable: (grainId: GrainId) => boolean;
  /** Migrate a locally-hosted grain to `target`; `false` if it isn't hosted here. */
  migrate: (grainId: GrainId, target: SiloAddress) => Promise<boolean>;
  /** Send an `AcceptExchangeRequest` to `target`'s repartitioner over the wire. */
  sendAcceptExchangeRequest: (
    target: SiloAddress,
    request: AcceptExchangeRequest,
  ) => Promise<AcceptExchangeResponse>;
}

type Direction = "localToLocal" | "localToRemote" | "remoteToLocal";

interface VertexEdge {
  readonly sourceId: GrainId;
  readonly targetId: GrainId;
  readonly isMigratable: boolean;
  readonly partnerSilo: SiloAddress;
  readonly direction: Direction;
  readonly weight: number;
}

class CandidateVertexHeapElement implements HeapElement<CandidateVertexHeapElement> {
  heapIndex = -1;
  location: "local" | "remote" | "unknown" = "unknown";
  accumulatedTransferScore: number;
  readonly connectedVertices: Array<[CandidateVertexHeapElement, number]> = [];

  constructor(
    readonly id: GrainId,
    accumulatedTransferScore: number,
  ) {
    this.accumulatedTransferScore = accumulatedTransferScore;
  }

  compareTo(other: CandidateVertexHeapElement): number {
    return this.accumulatedTransferScore - other.accumulatedTransferScore;
  }
}

/** The activation repartitioner's per-silo protocol object (see module doc). */
export class ActivationRepartitioner {
  private readonly edgeWeights: FrequentEdgeCounter;
  private activationCountOffset = 0;
  private lastExchangeAt = Number.NEGATIVE_INFINITY;

  constructor(private readonly deps: ActivationRepartitionerDeps) {
    this.edgeWeights = new FrequentEdgeCounter(deps.options.maxEdgeCount);
  }

  // ── message sink (edge recording) ──────────────────────────────────────

  /**
   * Record an edge from this (sending) silo's perspective, at the moment the
   * dispatcher resolves where a grain-to-grain call is headed. `undefined`
   * `sender` (a top-level, non-grain-originated call) records nothing, same
   * as a self-call (Orleans `RepartitionerMessageFilter`).
   */
  recordLocalEdge(sender: GrainId | undefined, target: GrainId, targetSilo: SiloAddress): void {
    if (sender === undefined || sender.equals(target)) return;
    const isSenderMigratable = this.deps.isMigratable(sender);
    const isTargetMigratable = this.deps.isMigratable(target);
    if (!isSenderMigratable && !isTargetMigratable) return;
    this.edgeWeights.add(
      edge(
        edgeVertex(sender, this.deps.local, isSenderMigratable),
        edgeVertex(target, targetSilo, isTargetMigratable),
      ),
    );
  }

  /** Record an edge from this (receiving) silo's perspective, for an inbound remote call. */
  recordRemoteEdge(sender: GrainId, senderSilo: SiloAddress, target: GrainId): void {
    if (sender.equals(target)) return;
    const isSenderMigratable = this.deps.isMigratable(sender);
    const isTargetMigratable = this.deps.isMigratable(target);
    if (!isSenderMigratable && !isTargetMigratable) return;
    this.edgeWeights.add(
      edge(
        edgeVertex(sender, senderSilo, isSenderMigratable),
        edgeVertex(target, this.deps.local, isTargetMigratable),
      ),
    );
  }

  // ── testing surface (Orleans `IActivationRepartitionerSystemTarget`) ───

  resetCounters(): void {
    this.edgeWeights.clear();
  }

  getActivationCount(): number {
    return this.deps.activationCount();
  }

  setActivationCountOffset(offset: number): void {
    this.activationCountOffset = offset;
  }

  getGrainCallFrequencies(): ReadonlyArray<{ edge: Edge; count: number }> {
    return this.edgeWeights.elements.map((e) => ({ edge: e.element, count: e.count }));
  }

  /** No-op: edges are recorded synchronously in this port (see module doc). */
  flushBuffers(): Promise<void> {
    return Promise.resolve();
  }

  private getLocalActivationCount(): number {
    return this.deps.activationCount() + this.activationCountOffset;
  }

  // ── protocol ─────────────────────────────────────────────────────────

  async triggerExchangeRequest(): Promise<void> {
    const silos = activeSilos(this.deps.membership.current()).filter(
      (s) => !s.equals(this.deps.local),
    );
    if (silos.length === 0) return;

    const migrationCandidates = this.getMigrationCandidates();
    const sets = this.createCandidateSets(migrationCandidates, silos);

    for (const { silo: candidateSilo, candidates: offeredGrains } of sets) {
      if (offeredGrains.length === 0) continue;
      try {
        const response = await this.deps.sendAcceptExchangeRequest(candidateSilo, {
          sendingSilo: this.deps.local,
          exchangeSet: offeredGrains,
          activationCountSnapshot: this.getLocalActivationCount(),
        });
        if (response.type === "success") {
          // `acceptedGrainIds` are this silo's own offered grains the peer
          // pulled in — this silo gives them away. `givenGrainIds` are the
          // peer's own grains it decided to give us; the peer migrates those
          // itself (its own `finalizeProtocol` call), so this silo only uses
          // them to prune stale edges.
          await this.finalizeProtocol(
            response.acceptedGrainIds,
            response.givenGrainIds,
            candidateSilo,
          );
          return;
        }
        if (response.type === "mutualExchangeAttempt") return;
        // "exchangedRecently": try the next best candidate.
      } catch {
        // Unreachable peer or transient failure: try the next best candidate.
      }
    }
  }

  async acceptExchangeRequest(request: AcceptExchangeRequest): Promise<AcceptExchangeResponse> {
    const now = Date.now();
    if (now - this.lastExchangeAt < this.deps.options.recoveryPeriodMs) {
      return { type: "exchangedRecently", acceptedGrainIds: [], givenGrainIds: [] };
    }

    const migrationCandidates = this.getMigrationCandidates();
    const localSet = this.getCandidatesForSilo(migrationCandidates, request.sendingSilo);
    const remoteSet = request.exchangeSet;

    let remoteActivations = request.activationCountSnapshot;
    let localActivations = this.getLocalActivationCount();
    let imbalance = Math.abs(localActivations - remoteActivations);

    const { localHeap, remoteHeap } = this.createCandidateHeaps(localSet, remoteSet);
    const giving: GrainId[] = [];
    const accepting: GrainId[] = [];

    const tryMigrateCore = (
      sourceHeap: MaxHeap<CandidateVertexHeapElement>,
      localDelta: number,
      remoteDelta: number,
    ): CandidateVertexHeapElement | undefined => {
      const anticipated = Math.abs(
        localActivations + localDelta - (remoteActivations + remoteDelta),
      );
      if (anticipated >= imbalance && !this.deps.toleranceRule.isSatisfiedBy(anticipated)) {
        return undefined;
      }
      const popped = sourceHeap.tryPop();
      if (!popped.found) return undefined;
      if (popped.value.accumulatedTransferScore <= 0) return undefined;

      localActivations += localDelta;
      remoteActivations += remoteDelta;
      imbalance = anticipated;
      return popped.value;
    };

    const tryLocalToRemote = (): boolean => {
      const chosen = tryMigrateCore(localHeap, -1, 1);
      if (chosen === undefined) return false;
      giving.push(chosen.id);
      for (const [connected, transferScore] of chosen.connectedVertices) {
        if (connected.location === "local") {
          connected.accumulatedTransferScore += transferScore;
          localHeap.onIncreaseElementPriority(connected);
        } else if (connected.location === "remote") {
          connected.accumulatedTransferScore -= transferScore;
          remoteHeap.onDecreaseElementPriority(connected);
        }
      }
      chosen.location = "remote";
      return true;
    };

    const tryRemoteToLocal = (): boolean => {
      const chosen = tryMigrateCore(remoteHeap, 1, -1);
      if (chosen === undefined) return false;
      accepting.push(chosen.id);
      for (const [connected, transferScore] of chosen.connectedVertices) {
        if (connected.location === "local") {
          connected.accumulatedTransferScore -= transferScore;
          localHeap.onDecreaseElementPriority(connected);
        } else if (connected.location === "remote") {
          connected.accumulatedTransferScore += transferScore;
          remoteHeap.onIncreaseElementPriority(connected);
        }
      }
      chosen.location = "local";
      return true;
    };

    for (;;) {
      const localScore = localHeap.firstOrDefault()?.accumulatedTransferScore ?? 0;
      const remoteScore = remoteHeap.firstOrDefault()?.accumulatedTransferScore ?? 0;
      const preferLocalFirst =
        localScore > remoteScore ||
        (localScore === remoteScore && localActivations > remoteActivations);
      const migrated = preferLocalFirst
        ? tryLocalToRemote() || tryRemoteToLocal()
        : tryRemoteToLocal() || tryLocalToRemote();
      if (!migrated) break;
    }

    await this.finalizeProtocol(giving, accepting, request.sendingSilo);
    this.lastExchangeAt = Date.now();

    return { type: "success", acceptedGrainIds: accepting, givenGrainIds: giving };
  }

  /** Migrate `giving` to `targetSilo` and prune stale edges touching either set. */
  private async finalizeProtocol(
    giving: readonly GrainId[],
    accepting: readonly GrainId[],
    targetSilo: SiloAddress,
  ): Promise<void> {
    for (const grainId of giving) {
      await this.deps.migrate(grainId, targetSilo);
    }

    const affected = new Set<string>();
    for (const id of giving) affected.add(id.toString());
    for (const id of accepting) affected.add(id.toString());
    if (affected.size === 0) return;

    const toRemove: Edge[] = [];
    for (const { element: e } of this.edgeWeights.elements) {
      if (affected.has(e.source.id.toString()) || affected.has(e.target.id.toString())) {
        toRemove.push(e);
      }
    }
    for (const e of toRemove) this.edgeWeights.remove(e);
  }

  // ── candidate-set construction (ported from Orleans, `Silo` = `deps.local`) ──

  private getMigrationCandidates(): Map<string, VertexEdge[]> {
    const result = new Map<string, VertexEdge[]>();
    for (const { element: e, count } of this.edgeWeights.elements) {
      if (count === 0) continue;
      const isSourceLocal = e.source.silo.equals(this.deps.local);
      const isTargetLocal = e.target.silo.equals(this.deps.local);

      const push = (v: VertexEdge): void => {
        if (!v.isMigratable) return;
        const key = v.sourceId.toString();
        const list = result.get(key);
        if (list === undefined) result.set(key, [v]);
        else list.push(v);
      };

      if (isSourceLocal && isTargetLocal) {
        push({
          sourceId: e.source.id,
          targetId: e.target.id,
          isMigratable: e.source.isMigratable,
          partnerSilo: this.deps.local,
          direction: "localToLocal",
          weight: count,
        });
        // Flip: a local-to-local edge links two local grains, and both take
        // part in the candidate set (Orleans' `CreateLocalVertexEdges`).
        push({
          sourceId: e.target.id,
          targetId: e.source.id,
          isMigratable: e.target.isMigratable,
          partnerSilo: this.deps.local,
          direction: "localToLocal",
          weight: count,
        });
      } else if (isSourceLocal) {
        push({
          sourceId: e.source.id,
          targetId: e.target.id,
          isMigratable: e.source.isMigratable,
          partnerSilo: e.target.silo,
          direction: "localToRemote",
          weight: count,
        });
      } else if (isTargetLocal) {
        push({
          sourceId: e.target.id,
          targetId: e.source.id,
          isMigratable: e.target.isMigratable,
          partnerSilo: e.source.silo,
          direction: "remoteToLocal",
          weight: count,
        });
      }
      // Neither side local: can occur if a message was re-routed via this
      // silo; not expected from this port's recording hooks, so ignored.
    }
    return result;
  }

  private getCandidatesForSilo(
    migrationCandidates: Map<string, VertexEdge[]>,
    otherSilo: SiloAddress,
  ): CandidateVertex[] {
    const result: CandidateVertex[] = [];
    for (const grainEdges of migrationCandidates.values()) {
      let accLocalScore = 0;
      let accRemoteScore = 0;
      for (const e of grainEdges) {
        if (e.direction === "localToLocal") accLocalScore += e.weight;
        else if (e.partnerSilo.equals(otherSilo)) accRemoteScore += e.weight;
      }
      if (accLocalScore >= accRemoteScore) continue;

      const totalAccScore = accRemoteScore - accLocalScore;
      const connVertices = grainEdges.map((e) => ({ id: e.targetId, transferScore: e.weight }));
      result.push({
        id: grainEdges[0]!.sourceId,
        accumulatedTransferScore: totalAccScore,
        connectedVertices: connVertices,
      });
    }
    return result;
  }

  private createCandidateSets(
    migrationCandidates: Map<string, VertexEdge[]>,
    silos: readonly SiloAddress[],
  ): Array<{ silo: SiloAddress; candidates: CandidateVertex[]; transferScore: number }> {
    const sets = silos.map((silo) => {
      const candidates = this.getCandidatesForSilo(migrationCandidates, silo);
      const transferScore = candidates.reduce((sum, c) => sum + c.accumulatedTransferScore, 0);
      return { silo, candidates, transferScore };
    });
    sets.sort((a, b) => b.transferScore - a.transferScore);
    return sets;
  }

  private createCandidateHeaps(
    local: readonly CandidateVertex[],
    remote: readonly CandidateVertex[],
  ): {
    localHeap: MaxHeap<CandidateVertexHeapElement>;
    remoteHeap: MaxHeap<CandidateVertexHeapElement>;
  } {
    const sourceIndex = new Map<string, CandidateVertex>();
    for (const c of local) sourceIndex.set(c.id.toString(), c);
    for (const c of remote) sourceIndex.set(c.id.toString(), c);

    const heapIndex = new Map<string, CandidateVertexHeapElement>();
    const getOrAdd = (c: CandidateVertex): CandidateVertexHeapElement => {
      const key = c.id.toString();
      let v = heapIndex.get(key);
      if (v === undefined) {
        v = new CandidateVertexHeapElement(c.id, c.accumulatedTransferScore);
        heapIndex.set(key, v);
      }
      return v;
    };
    const createVertex = (c: CandidateVertex): CandidateVertexHeapElement => {
      const vertex = getOrAdd(c);
      for (const connected of c.connectedVertices) {
        const source = sourceIndex.get(connected.id.toString());
        if (source !== undefined) {
          vertex.connectedVertices.push([getOrAdd(source), connected.transferScore]);
        }
        // Otherwise the connected vertex is not part of either candidate set: ignored.
      }
      return vertex;
    };

    const localVertexList: CandidateVertexHeapElement[] = [];
    for (const c of local) {
      const vertex = createVertex(c);
      vertex.location = "local";
      localVertexList.push(vertex);
    }

    const remoteVertexList: CandidateVertexHeapElement[] = [];
    for (const c of remote) {
      const vertex = createVertex(c);
      if (vertex.location !== "unknown") continue; // already local: assume local, ignore the remote copy
      vertex.location = "remote";
      remoteVertexList.push(vertex);
    }

    return {
      localHeap: new MaxHeap(localVertexList),
      remoteHeap: new MaxHeap(remoteVertexList),
    };
  }
}
