# @thresh/parity — the Orleans parity suite

A 1:1 traceable port of Microsoft Orleans' functional test suite, run against
this framework. Passing tests are evidence of parity; skipped tests are the
parity backlog.

## How it works

- **Pin.** Tests are ported from the Orleans release tag recorded under
  `orleans.tag` in this package's `package.json`. Read upstream sources with
  `git -C <orleans-checkout> show <tag>:<path>` — never a working tree, which
  may be ahead of the tag.
- **Traceability.** Every test is declared with `orleansTest(id, fn)` from
  `@thresh/testing/orleans-test`, where `id` is the fully-qualified upstream
  xunit test id (`Namespace.Class.Method`). The id is the vitest test name, so
  reporter output traces straight back to the original test.
- **Gaps.** A test whose feature does not exist here yet is registered with
  `orleansTest.gap(tag, id, fn)` — a skip prefixed with a machine-readable
  `GAP-*` tag. Each tag has a matching entry in the repo's `todo.md`.
- **Exclusions.** A test deliberately not ported (a .NET-specific mechanism, or
  skipped upstream) is recorded with `orleansTest.excluded(reason, id)` so
  every test in an in-scope upstream file is accounted for.
- **Scorecard.** `pnpm parity:scorecard` (from the repo root) parses these
  declarations and prints per-suite ported/gap/excluded counts and a per-gap
  rollup; `--run` joins in actual pass/fail results. The output is regenerable
  and never committed.

## Layout

`src/grains/` mirrors Orleans `test/Grains` (interfaces and implementations,
translated on demand); the other `src/` folders each mirror one upstream test
project (e.g. `src/default-cluster` ⇄ `test/Orleans.DefaultCluster.Tests`).
Every translated file's header comment names its upstream source path.

## Running

From the repo root: `pnpm test:parity` (the parity vitest project), or
`pnpm test:all` for unit + parity together.

## Bumping the pin

Update `orleans.tag` in `package.json`, then re-diff ported files against the
new tag (`git -C <orleans-checkout> diff <old-tag> <new-tag> -- test/<path>`)
and reconcile.

## Attribution

Test logic in this package is derived from
[dotnet/orleans](https://github.com/dotnet/orleans), licensed under the MIT
License, Copyright (c) .NET Foundation. See the upstream
[LICENSE](https://github.com/dotnet/orleans/blob/main/LICENSE).
