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
place a duplicate can arise, and it is resolved there (Orleans
`DistributedGrainDirectory._recoveryMembershipVersion`).

In the port the recovery version lives on the partition (`GrainDirectoryPartition.recoveryVersion`),
and two mechanisms apply the rule, each of which closes the window alone. The directory issues an
operation again when the recovery version rose while it was in flight. The claim of a new activation
and the acceptance of a migrated one then confirm the registration again while the recovery version
is above the entry's, which covers a rise after the directory's last look.

### Requests

`lookup`, `register` and `unregister` carry the caller's view version.

- The callee waits until it has applied at least that version (`refresh`), then for every lock at or
  below it on the grain's hash.
- If it then does not own the hash it answers **refresh required** with its own version. It checks
  ownership every time, not only when the caller is behind.
- The caller refreshes to that version, re-resolves the owner and retries, with backoff.

Registration keeps its compare-and-swap, and a lookup keeps hiding an entry whose host is no longer a
member.

### A dead process that is still listed

A pod whose process has died stays listed, not ready, until Kubernetes removes it or restarts its
container in place, and a restarted container is the same silo address. The ranges the dead silo
owned are rebuilt without it, but entries for its activations in ranges that did not change hands
are still in the directory, and a call that follows one fails for as long as the pod is listed. Two
repairs close that, and both rest on the host being the authority on what it hosts:

- **A refused dial is an answer on the call path too.** When the dial to an entry's host is refused
  outright there is no process at the address, so the activation is gone. The caller unregisters
  that exact entry (address and activation id) at its owner and resolves the grain again, which
  places it afresh. The request never left, so resending it is safe. A dial that times out, or a
  connection that closes during the handshake, says nothing about the process behind it and changes
  nothing.
- **A silo that does not host what the directory says it hosts removes the entry**, whether or not
  it may activate (Orleans unregisters with `UnregistrationCause.NonexistentActivation`). A silo in
  the ring already does this before it claims the grain. A silo outside the ring, which is what a
  restarted container is until it is ready, looks up the grain it was sent: an entry naming it for
  an activation that is neither here nor coming up is unregistered, and the call is refused before
  execution so the caller resolves again.

Neither is a failure detector: nothing is inferred about a silo that does not answer.

### Leaving

A leaving silo hands its ranges over while it is still serving, so nothing waits for its drain:

1. It flips readiness and waits out the grace period, as before. That is the time Kubernetes' own
   proxies need to stop routing to the pod, which the silo cannot observe.
2. It keeps following the watch through the stop. The ownership hooks (reminders, stream queues, job
   shards) stay halted from the moment the stop begins; only the node and its partition go on
   applying views. So it sees itself out of the ring, terminating or not ready, in the view its
   peers act on, and not only when a new owner's request makes it catch up.
3. Applying that view releases every range it owned into a snapshot for the new owners, as any view
   change does. When the grace has elapsed it waits, within what is left of the stop budget, until
   every snapshot has been collected, or its partners have left or started recovery. A silo that
   never saw itself leave (a held watch, a membership that is not told) holds no snapshot and waits
   for nothing; its ranges are recovered once it has gone.
4. Only then does it deactivate its activations. It keeps serving them until that point. Their
   unregistrations resolve the owner in the view it has applied, so they go to the new owners.
5. Then the partition stops and the listener closes.

A silo with nobody to hand over to (the last one, or a cluster stopping together) waits for nothing.
Static membership takes the same path: the view marks the silo `draining` before it stops and removes
it after, which is what `TestCluster` does for a graceful stop.

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
4. **A dead process that is still listed.** The refused dial and the unhosted entry each remove the
   stale entry.

## Tests that must exist

All on `TestCluster`'s Kubernetes membership, with calls issued without waiting for the cluster to
settle:

- a join under load, including grains first called through the joiner while it is not ready;
- a crash in two steps, with the dead silo refusing connections, and again with it unreachable: the
  range blocks, then unblocks when the pod is removed;
- a drain, with calls throughout: no call fails, the ranges move by snapshot and not by recovery,
  and a slow deactivation on the leaving silo delays no lookup;
- a crash whose pod stays listed and not ready: every grain it hosted answers again, from a fresh
  activation, before the pod is removed, and again when the container restarts in place;
- a view that adds one silo and removes another;
- a silo whose watch is held across two changes and released;
- a readiness flap;
- a cold start of several silos with a startup task that calls a grain;
- a bounded run that starts, stops and kills silos at random and checks integrity each round, after
  Orleans' `ElasticChaos`.

## Out of scope

Local suspicion of a dead peer stays placement-only, as section 9 of the design notes decides: a
refused dial removes the one entry that was followed, and nothing else is concluded about the silo.
Damping readiness flaps, and a failure detector that could fence an unreachable silo, are not
attempted.
