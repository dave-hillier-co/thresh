# 12 — Project structure and tooling

How the codebase is organised, built, and tested.

## Monorepo layout

A [pnpm workspace](https://pnpm.io/workspaces) of focused packages depending inward (abstractions at
the centre, hosting at the edge):

```
packages/
  core/            # abstractions: GrainId, interfaces, defineGrain/defineReducerGrain, decorators, value-codec
  messaging/       # Transport interface, WebSocket/in-process transport, Serializer
  runtime/         # silo, catalog, turn scheduler, dispatcher, placement (+ rebalancer model)
  directory/       # DHT grain directory + location cache + ring
  clustering-k8s/  # Kubernetes membership service
  persistence/     # GrainStorage interface + redis/postgres/memory providers
  reminders/       # reminder service + redis/postgres/memory tables
  streams/         # StreamProvider interface + redis-streams/memory providers
  transactions/    # TransactionalState + wait-die locks + memory/redis transactional storage (ADR 0008)
  observability/   # tracing/metrics/logging grain-call filters (ADR 0013)
  client/          # external client (createClient → getGrain via a gateway; discovery + failover)
  hosting/         # silo builder, health endpoints, graceful drain
examples/          # greeter, chat, cluster, bank, broadcast, migration, thermostat, k8s-silo
```

`core` has no internal dependencies; `runtime` depends on `core` / `messaging` / `directory`;
`clustering-k8s` / `persistence` / `reminders` / `streams` / `transactions` / `observability` depend
only on `core` and are composed by `hosting`; `client` depends on what an external caller needs. The
clock (`TimeProvider`) lives in `core`. Redis and Postgres providers reuse the tagged round-trip in
`@tsva/core/value-codec` (also used by the messaging serializers), so persisted state and stream events
preserve runtime value types (`Date`/`bigint`/`Guid`/`GrainId`). A Postgres stream backing and stream
partitioning over the ring are future work.

## Module conventions

- **No `index.ts` barrels** — import explicit module paths (`@tsva/core/grain-id`).
- **One primary export per file**, named for the file.
- `Proxy` (grain references) is the only real "magic"; the class decorators `defineGrain` wraps are the
  lone remaining decorator use.
- Package names are scoped (`@tsva/core`, …).

## TypeScript configuration

- `tsconfig.base.json` pins `strict`, `exactOptionalPropertyTypes`, `noUncheckedIndexedAccess`.
- **Standard TC39 decorators** (`experimentalDecorators: false`), **no `reflect-metadata`**: metadata
  is recorded in constructor `WeakMap`s and via the decorator's `addInitializer`.
- `moduleResolution: "bundler"`, ESM; each package's `exports` maps `./*` to `./src/*.ts`, so the dev
  loop runs straight from source with **no build step** (`tsc --noEmit` type-checks the workspace).
- Tests run under **Vitest** transpiling with **SWC** (`unplugin-swc`), since esbuild/Oxc don't yet
  support standard decorators.

## Testing strategy

Classic (Detroit-school) TDD with **sociable tests** — exercise real collaborators, fake only at true
boundaries (network, Redis/Postgres, the Kubernetes API, the clock); mockist isolation is avoided.

1. **Unit / sociable** — drive a grain through the real turn scheduler, catalog and in-memory
   providers; assert on observable behaviour.
2. **Single-silo integration** — a whole silo in one process (`useStaticMembership` + in-memory
   providers).
3. **Multi-silo integration** — several silos over the real WebSocket transport (or in-process when
   socket behaviour isn't under test); directory races, placement, cross-silo calls, rebalancing.
4. **Cluster (Kubernetes)** — deploy to kind; membership from real EndpointSlices, probe-driven failure
   detection, rolling updates, Redis-backed durability. Run on demand, not every commit.

Determinism aids: an **injectable clock** (`TimeProvider`; fake clock in tests), a **deterministic
placement** option, and the **in-process transport**. Redis/Postgres integration tests skip when the
store is down.

## Lint, format, CI

- **ESLint + Prettier** at the root (one flat config); `pnpm lint` / `pnpm format`. Markdown under
  `docs/` is hand-authored and excluded from Prettier.
- CI: `pnpm typecheck`, `pnpm lint`, `pnpm test` per PR; cluster tests on a schedule/label.
- Tooling is installed via the package manager (`pnpm add -D …`), never by hand-editing `package.json`.
