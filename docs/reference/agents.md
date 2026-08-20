# Agent reference

This project is ESM, strict TypeScript, Node 22+, pnpm 10, Vitest, ESLint, and Prettier. Preserve
subpath imports and do not invent package-root barrels.

## Reliable workflow

1. Read `README.md`, `EPICS.md`, a relevant example, package source, and adjacent tests.
2. Find symbols with `rg`; imports map to `packages/<name>/src/<subpath>.ts`.
3. Write or update a focused test before behavior changes.
4. Run `pnpm typecheck`, focused tests, `pnpm test`, and `pnpm lint`.
5. For parity work, find the matching `packages/parity` test and document deviations.

## Invariants

- Interface names and keys are persistent protocol identity; calls cross serialization boundaries.
- One activation owns a grain and normal turns are serialized.
- Closure fields are volatile; durable state needs a configured named provider.
- Registrations and compatible configuration must exist on every serving silo.
- Timers are local; reminder/job handlers must be idempotent; streams can redeliver.
- Transactions do not make arbitrary external side effects atomic.
- Memory providers are not production durability.

Use `/llms.txt` for the map or `/llms-full.txt` for consolidated context.
