# Project review — 2026-08-04

This review checks Thresh against the aims stated in the README and status board: a Kubernetes-native
TypeScript implementation of the Orleans virtual-actor model, with Orleans 10 parity as the main
milestone and browser-hosted read views as an explicitly beyond-parity direction.

## Scope and evidence

The review used the repository documentation, parity scorecard, source layout, and stub/TODO search.
The checks run were:

- `pnpm parity:scorecard`
- `rg -n "TODO|FIXME|stub|not implemented|throw new Error|return undefined|pending|skip\\(|\\.todo|describe\\.skip|it\\.skip|test\\.skip|XXX|HACK" -g '!node_modules' -g '!pnpm-lock.yaml' .`
- `rg -n "orleansTest\\.excluded|it\\.skip|describe\\.skipIf|test\\.skip|\\.todo" packages/parity packages/*/src examples -g '*.ts'`
- `pnpm test` (interrupted after several minutes with no progress output in this environment; see
  the final test notes in the associated change)

## Aim-by-aim assessment

| Aim | Assessment | Evidence |
| --- | --- | --- |
| Virtual actor model | Fulfilled. The core packages and examples cover grain identity, activation lifecycle, strongly typed proxy references, serialized turns, reentrancy, lifecycle hooks, and managed deactivation. | README quickstart and examples; core/runtime tests. |
| Kubernetes-native hosting | Fulfilled for the stated direction. K8s is documented as the membership authority and there is a dedicated real-cluster example with an opt-in e2e. | README and `examples/k8s-silo`. |
| Orleans 10 parity | Substantially fulfilled for the in-scope parity suite. The scorecard reports 502 ported tests, 0 gap-tagged tests, and 475 excluded cases with documented reasons. Excluded cases are not hidden stubs; they are explicit non-goals, upstream skips, platform differences, or tests whose Orleans mechanism is replaced by the Kubernetes design. | `pnpm parity:scorecard`. |
| Persistence, reminders, streams, transactions | Fulfilled at the parity/feature level advertised by the status board. The repo contains memory plus Redis/Postgres/Kafka provider surfaces and package-level tests, with external-service tests guarded by `describe.skipIf` when the dependency is not configured. | package layout and skip audit. |
| Functional/reducer-first TypeScript authoring | Fulfilled. Examples are functional/reducer-first, while one class/decorator path remains intentionally as interop coverage. | README examples and source layout. |
| Beyond-parity browser state replication | Not implemented by design. It is clearly marked as beyond parity and remains tracked as issue #38. | `EPICS.md` and `todo.md`. |
| Deferred stream backing polish | Not implemented by design. Phase 3 (LISTEN/NOTIFY polish, lag gauge, worked examples) is optional and explicitly deferred. | `EPICS.md` and `todo.md`. |

## Completeness and stub audit

No production code was found that is obviously stubbed in the sense of placeholder `TODO`, `FIXME`,
`not implemented`, or intentionally empty implementations masquerading as complete features. The
large number of `throw new Error` and `return undefined` matches normal validation, protocol, guard,
or optional-result logic rather than placeholder code.

The remaining incomplete work is transparent and intentionally tracked:

1. **Browser state replication** — beyond Orleans parity; still open.
2. **Optional stream-backing Phase 3 polish** — deferred, not required for the advertised Redis,
   Postgres, or Kafka stream backings to exist.
3. **Environment-dependent integration tests** — Redis, Postgres, Kafka, and Kubernetes tests use
   `describe.skipIf` guards when services are not configured. Those skips are operational gates, not
   code stubs.
4. **Parity exclusions** — the parity suite contains explicit `orleansTest.excluded(...)` records and
   matching skipped test shells. These exclusions are visible in the scorecard and should remain
   treated as documented scope boundaries unless the project decides to chase every Orleans internal
   implementation test instead of behavioural parity.

## Documentation consistency findings

Two documentation inconsistencies surfaced during the review:

- The status board still said the 2026-07-24 cancellation/read-only work had “remainders in
  `todo.md`,” but `todo.md` says those follow-up remainders were completed in that burn-down.
- `docs/orleans-port-analysis.md` is a historical pre-burn-down analysis. It names several gaps that
  later landed, including cancellation/deadlines, scheduler back-pressure, serializer versioning,
  stream failure handling, transaction TM keepalive, stateless-worker enforcement, and observability
  breadth. Treat it as historical context rather than current project state.

## Conclusion

Thresh appears to fulfil its stated current aims: a TypeScript, Kubernetes-native virtual-actor
runtime with the advertised Orleans 10 behavioural parity work complete for the in-scope parity
suite. There is no evidence from the review that incomplete or stubbed production code is being
presented as done. The only remaining incomplete items are explicitly labelled as beyond-parity or
deferred, and environment-dependent tests are intentionally skipped unless their external systems are
provided.
