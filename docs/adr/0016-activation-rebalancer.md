# ADR 0016 — Activation rebalancer (adaptive, entropy-minimizing)

- Status: Accepted — implemented (slice 1 model + slice 2a mechanism + slice 2b elected worker +
  builder + `RebalancingReport` + convergence e2e all shipped)
- Context docs: [06 — Grain directory and placement](../06-grain-directory-and-placement.md),
  [13 — Roadmap](../13-roadmap-and-phases.md)

> Orleans references: `Orleans.Runtime/Placement/Rebalancing/ActivationRebalancerWorker.cs` (the
> session/cycle loop, entropy, adaptive scaling, silo pairing), `.../ActivationRebalancerMonitor.cs`,
> `Orleans.Core/Placement/Rebalancing/{IActivationRebalancer,RebalancingReport,IActivationRebalancerWorker}.cs`,
> `Orleans.Runtime/Configuration/Options/ActivationRebalancerOptions.cs`,
> `ISiloControl.MigrateRandomActivations`. Algorithm write-up:
> <https://www.ledjonbehluli.com/posts/orleans_adaptive_rebalancing/>.

## Context

Placement decides where a grain activates *when it has no activation*; once placed, an activation
stays put until it deactivates or migrates ([ADR on grain migration]; `IGrainMigrationParticipant` /
`MigrateOnIdle` shipped). Over time a cluster drifts out of balance — a long-lived silo accumulates
activations, a freshly joined silo sits nearly empty — because placement only ever acted on *new*
grains. Orleans 10 closes this with the **activation rebalancer**: a background process that
proactively migrates *live* activations from busier to quieter silos to even out load, without
waiting for them to go idle.

Orleans' rebalancer is **adaptive and entropy-minimizing** (Behluli's model), not a naive
"move from the busiest to the emptiest." It treats the per-silo load distribution as a probability
distribution and drives its **Shannon entropy** toward the maximum (`ln S`, the perfectly even
distribution), migrating a *scaled, bounded* number of activations each cycle and backing off when
further cycles stop improving. This avoids both under-correction (slow convergence) and
over-correction (thrashing activations between silos).

## Decision

Port the adaptive model faithfully, in vertical slices.

### Load metric — a single scalar (divergence)

Orleans weights load by **both** activation count and memory usage per silo
(`ResourceStatistics(MemoryUsage, ActivationCount)`), gossiped by `DeploymentLoadPublisher`. This port
does **not** gossip per-silo memory (the placement-filters work left remote `resourceStats` as
activation-count-only; see [06](../06-grain-directory-and-placement.md)), so the rebalancer models
each silo's load as a **single scalar = its activation count**, i.e. the uniform-memory case of the
Orleans model. Concretely:

- `p_i = load_i / Σ load` — the normalized load distribution.
- `entropy = −Σ p_i · ln p_i`; `maxEntropy = ln S` (S = number of silos with stats).
- `imbalance = (maxEntropy − entropy) / maxEntropy ∈ [0,1]` — the cluster imbalance Orleans reports as
  `ClusterImbalance`.
- `ideal_i = Σ load / S` — the even target (Orleans' `n_i = (N/S)·(M_m/m_i)` collapses to this when
  memory is uniform).

When per-silo memory reporting is added later, `load_i` becomes the memory-weighted quantity and the
rest of the model is unchanged — the scalar is the only seam.

### The cycle (faithful)

A **session** runs **cycles** spaced by `sessionCyclePeriod`. Each cycle, given the current per-silo
load snapshot and cycle state, decides one of:

- **noop** — fewer than 2 silos with stats: nothing to balance.
- **complete** — `imbalance < allowedDeviation`: the cluster is effectively even; end the session and
  cool down for one cycle period.
- **stagnate** — the normalized entropy change since the last cycle, `|Δentropy| / maxEntropy`, is
  below `entropyQuantum` for `maxStagnantCycles` consecutive cycles: the session is not improving;
  end it and back off (exponential, on `failedSessions`).
- **migrate** — otherwise, form silo **pairs** (sort by load; pair lowest with highest, next-lowest
  with next-highest, …) and for each pair move
  `delta = ⌊ alpha · scaling · (|load_high − load_low| / 2) ⌋`
  activations from the high silo to the low silo, where `alpha = entropy / maxEntropy` and
  `scaling = (1 − e^(−cycleWeight·cycle)) · 1/(1 + siloWeight·(S−1))` is Orleans' **adaptive scaling**
  (ramps the migration rate up over a session's cycles, damped by cluster size). `delta` is clamped to
  the high silo's count and to `activationMigrationCountLimit`.

The adaptive scaling and the entropy stop-conditions are the point of the model and are ported
exactly; only the load metric is simplified.

### Slices

- **Slice 1 (shipped)** — the model as a **pure function**
  `planCycle(snapshot, options, state) → { moves, nextState, stop? }`
  (`rebalancer-model.ts`), with `shannonEntropy` / `clusterImbalance` / `formSiloPairs` /
  `adaptiveScaling` as tested building blocks; no cluster, no I/O.
- **Slice 2a (shipped)** — the distributed mechanism on `ClusterNode`: a `system: "load"` RPC +
  `gatherClusterLoad`, `migrateRandomActivations(target, count)` (directed immediate migration reusing
  the `dehydrate` → `system: "migration"` path, reachable on a peer via a `system: "rebalance"` RPC),
  and `runRebalanceCycle(state)` that gathers load, runs `planCycle`, and executes the moves.
- **Slice 2b (shipped)** — an **elected singleton worker** (`ActivationRebalancerWorker`; one silo,
  elected deterministically as the lowest active ring key from `MembershipService`, Orleans'
  `[KeepAlive, Immovable]` system target) driving `runRebalanceCycle` on a self-rescheduling timer,
  threading `CycleState`, cooling down one period on a completed session and backing off exponentially
  on a stagnated one; `createSilo(...).useActivationRebalancing(options?)` wires and starts/stops it
  with the host; a `RebalancingReport` + `suspend`/`resume`; a multi-silo convergence e2e.

## Consequences

- The cluster self-levels: a newly joined silo is filled by migrating live activations to it, not only
  by future placements; a hot silo is drained gradually rather than abruptly.
- Migration cost is bounded per cycle (the scaling factor and the count limit), and sessions stop once
  balanced or stagnant, so the rebalancer does not thrash.
- The model is pure and independently tested, so the (harder) distributed wiring builds on a verified
  core.

## Scope boundary

- **Activation-count load only** (uniform memory) until per-silo memory reporting exists; documented
  divergence above.
- **No repartitioning** (Orleans' separate `Repartitioning/*` cograph optimizer that co-locates
  chatty grains) — out of scope; this ADR covers load rebalancing only.
- The rebalancer **never rejects placement** and never blocks calls; it only migrates idle-eligible
  live activations opportunistically, honouring the same `IGrainMigrationParticipant` state transfer.
