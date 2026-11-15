# Design — one view per version, and a directory that fails closed

This note settles two findings of the [2026-11-14 design review](project-review-2026-11-14.md) that
have to be decided together, because both define who owns a hash range:

- the open half of issue #72: a silo always counts itself `active` in its own view, so a joiner or a
  draining silo holds a different ring from its peers under the same view version;
- **M6**: the directory handoff is a per-grain map with a per-source gate that opens without the
  range's entries in several cases, and nothing detects or repairs the duplicate that follows.

It supersedes the "TS today" paragraph and the recommendation of section 8 of
[design-notes-parity-gaps.md](design-notes-parity-gaps.md).

## What Orleans relies on

Orleans' distributed grain directory (`src/Orleans.Runtime/GrainDirectory`) is correct because of
four properties of its membership, and the port needs each of them:

1. **The ring is the active silos only.** A joining or shutting-down silo owns no range
   (`DirectoryMembershipSnapshot.cs`), and a silo creates activations only while its own status is
   active (`Catalog.GetOrCreateActivation`).
2. **A view is the same everywhere at a given version.** The version is the membership table's.
3. **Requests carry the caller's version.** A callee that is behind catches up before it answers; a
   caller that is behind is told to refresh and retries (`GrainDirectoryPartition.Interface.cs`).
4. **A range is locked across a view change** until its contents have been transferred from the
   previous owner or rebuilt from the hosts (`AcquireRangeAsync`, `ReleaseRangeAsync`). Callers wait;
   they are not told the grain is unregistered.

Thresh has the fourth only in part, the third only when versions differ, and breaks the first two for
the local silo.

## Decision 1 — a silo follows the watch like any peer

**The view is a pure function of the EndpointSlices.** `KubernetesMembership` stops injecting the
local silo. Every listed endpoint, the local one included, gets its status from its conditions:

| Endpoint                         | Status     | In the ring | A member |
| -------------------------------- | ---------- | ----------- | -------- |
| `ready: true`                    | `active`   | yes         | yes      |
| not ready, `terminating: true`   | `draining` | no          | yes      |
| not ready, not terminating       | `joining`  | see below   | yes      |
| not listed                       | absent     | no          | no       |

Two silos that have observed the same slices now hold the same view, so "same version, same ring"
holds and the owner-side shortcut that trusts an equal version becomes sound.

**The ring, and with it the placement candidates, is the `active` members; when there are none it is
the `joining` members.** The fallback is what lets a cold cluster start: nobody is ready yet, every
silo sees the same listed set, so they agree on a ring and startup tasks can call grains. The first
silo to become ready shrinks the ring to itself and the others hand their ranges to it through the
protocol in decision 2. One helper computes this set and every consumer of "who may own or host"
uses it: the hash ring, placement, migration targets, the rebalancer and repartitioner, the gateway
list, stream, reminder and job ownership.

**A silo outside the ring creates no activation.** A call that would activate a grain on it is
refused before execution so the caller re-resolves (Orleans forwards). It keeps serving the
activations it already hosts, and their directory entries stay valid because it is still a member.
This closes the window in which a joiner and its peers each own the same range, and the one in which
a draining silo keeps placing on itself.

**Readiness stops depending on the ring.** `membershipHealthy` becomes "the watch has completed a
list and this silo is listed". Readiness is still published last, after startup tasks, and flipped
first on stop. The bootstrap cycle that self-injection existed to break is gone, because a joining
silo no longer needs to be in a ring to be healthy.

**The self-probe no longer goes through placement.** It is activated through the node's local
system-activation path: never placed, never registered in the directory, always answered by the silo
that asked. It then measures only this silo's dispatcher, which is its purpose, and it works on a
silo that is not a placement candidate.

**A silo that sees itself removed fences itself.** If a list or event omits a silo that was
previously listed, its peers have already dropped its entries. It fails liveness, refuses every call
and abandons its activations without unregistering them. This is Orleans' `KillMyselfLocally`.
Before its first listing a silo is simply not yet a member.

**The version stays the observed EndpointSlice revision,** with two changes that the protocol below
needs:

- A watch must report a revision. The local `+1` fallback is removed; a hand-driven test watch
  passes one.
- `MembershipService.refresh(version)` lets a silo catch up on demand, as
  `ClusterMembershipService.Refresh` does. Under Kubernetes it waits briefly for the watch and then
  re-lists; a list whose own `resourceVersion` is at least the requested version proves the local
  view is at least that new, so the silo adopts the requested version. That closes the documented
  case in which a silo that listed after a missed deletion sat behind a peer until the next write.
  Static membership has one view and returns at once.

### What changes for an operator

- A joining pod owns nothing and hosts nothing until Kubernetes reports it ready.
- A pod that is taken out of the Service while running restarts itself.
- A readiness flap moves the pod's ranges away and back. That is safe under decision 2 and costs a
  transfer each way; damping it is out of scope.

## Decision 2 — the directory partition is a state machine that fails closed

The protocol moves out of `cluster-node.ts` into `@thresh/directory` as a port of
`GrainDirectoryPartition`. The node keeps only the wiring.

### State

- **Entries**, each stamped with the owner's view version at registration.
- **Range locks**: `(ranges, version)`. A request for a hash under a lock at or below its version
  waits.
- **Snapshots** held for transfer: the entries of ranges this silo gave up, with the pair of view
  versions it moved between and the silos that may come for them.
- **The applied view**: the version, the ring and the member set.

A range is `(start, end]` on the hash circle, and one definition serves the directory, reminders and
streams. Today `rangesFor` and `ownerOf` disagree about which end is inclusive.

### On a view change

Everything up to and including taking the locks happens synchronously when the view is applied, so no
request can run between the ring changing and the range being locked.

1. **Members that left.** Delete every entry hosted on them and drop them from every snapshot's
   partner list.
2. **Ranges lost.** Lock them. Wait for any earlier lock on them. Move their entries into a snapshot
   keyed by `(previous version, current version)` with the new owners as partners. Unlock. From here
   a request for those hashes is told to refresh.
3. **Ranges gained.** Lock them. For each previous owner that is still a member, pull its snapshot
   for `(previous version, current version)`. If every pull succeeds, apply the entries and
   acknowledge. Otherwise recover. Unlock only when one of the two has completed.

Orleans decides whether the previous owner is known by checking that the version advanced by exactly
one. Slice revisions do not advance by one, and a silo may coalesce views, so the port asks for the
exact pair instead: a snapshot is served only to a silo that moved between the same two versions.
A silo that skipped a view gets nothing and recovers, which is what Orleans does for a
non-contiguous change.

### Recovery

The new owner asks **every member** for the registered, live activations it hosts in the range, and
applies the answers. The directory is rebuilt from the hosts, which is why nothing has to be declared
lost: a silo that is gone hosts nothing.

- A member that refuses the connection has no process and so hosts nothing: that is an answer.
- A member that does not answer is retried for as long as it is a member. The range stays locked.
- A member that leaves the view stops being asked.

This is the fail-closed choice, and its cost is specific: when a silo is unreachable but Kubernetes
still lists it, lookups that miss the location cache for its share of the hash space wait until
Kubernetes removes it. Without a failure detector of its own, the alternative is to let those grains
activate a second time while the first may still be serving. An optional bound
(`directoryRecoveryPatience`, off by default) lets an operator choose availability; when it fires
the range unlocks and the event is logged and counted.

A responder also drops the requester from its snapshots, and raises its **recovery version**. A silo
whose registration completed below its recovery version re-confirms it; if another activation now
holds the entry, the local one deactivates as a duplicate and forwards its queue. That is the only
place a duplicate can arise, and it is resolved there
(`DistributedGrainDirectory._recoveryMembershipVersion`).

### Requests

`lookup`, `register` and `unregister` carry the caller's view version.

- The callee waits until it has applied at least that version (`refresh`), then for every lock at or
  below it on the grain's hash.
- If it then does not own the hash it answers **refresh required** with its own version. It checks
  ownership every time, not only when the caller is behind.
- The caller refreshes to that version, re-resolves the owner and retries, with backoff.

Registration keeps its compare-and-swap, and a lookup keeps hiding an entry whose host is no longer a
member.

### Leaving

A stopping silo flips readiness, waits to see itself out of the ring, lets its partition release its
ranges, waits (within the stop budget) for the new owners to collect the snapshots, and only then
deactivates its activations. Their unregistrations go to the new owners.

### Integrity

`checkIntegrity`, a test hook as in Orleans: no partition holds an entry outside its range, and every
registered live activation in the cluster has exactly its own entry at its owner. `TestCluster`
exposes it, and the tests below call it after every step.

## What is removed

The per-grain handoff snapshot, its retention timer, the per-source recovery pass, the re-arm after
exhaustion, the unfiltered `serveRecover`, and the "fall back to lazy activation" path. Each was a
patch for a case the range lock covers.

## Slices

1. **The view.** Statuses from conditions for every silo, the ring helper with its cold-start
   fallback, the activation gate, readiness, the local self-probe, self-fencing, `refresh` and the
   mandatory revision. The existing handoff keeps working on top of it, on the path the
   static-membership tests already cover.
2. **The partition.** Range locks, snapshots by version pair, recovery, versioned requests, the
   recovery version, `checkIntegrity`; the old machinery deleted.
3. **Leaving.** Release on drain, wait for transfer, then deactivate; `TestCluster`'s static mode
   takes the same path.

## Tests that must exist

All on `TestCluster`'s Kubernetes membership, with calls issued without waiting for the cluster to
settle:

- a join under load, including grains first called through the joiner while it is not ready;
- a crash in two steps, with the dead silo refusing connections, and again with it unreachable: the
  range blocks, then unblocks when the pod is removed;
- a drain, with calls throughout;
- a view that adds one silo and removes another;
- a silo whose watch is held across two changes and released;
- a readiness flap;
- a cold start of several silos with a startup task that calls a grain;
- a bounded run that starts, stops and kills silos at random and checks integrity each round, after
  Orleans' `ElasticChaos`.

## Out of scope

Local suspicion of a dead peer stays placement-only, as section 9 of the design notes decides. Damping
readiness flaps, and a failure detector that could fence an unreachable silo, are not attempted.
