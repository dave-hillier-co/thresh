# CLAUDE.md

Guidance for working in this repository. See [`README.md`](README.md) for what the project is and
[`docs/`](docs/) for the design.

## Workflow

- **Maintain `todo.md`.** Track outstanding work items in `todo.md` at the repo root. Add items as
  they surface, mark them done as they complete, and remove stale ones. Keep it current so it
  reflects the real state of the work; [`EPICS.md`](EPICS.md) is the status board.
- **Work test-first (TDD).** Write a failing test that describes the desired behaviour before
  writing the implementation, then make it pass, then refactor. Prefer classic/Detroit-school TDD
  with sociable tests over mockist isolation; fake only at true boundaries (network, Redis, the
  Kubernetes API, the clock). See the testing strategy in
  [`docs/deviations.md`](docs/deviations.md).
- **Work in vertical slices.** Deliver one thin end-to-end capability at a time — interface through
  runtime through provider — rather than building a layer at a time. Each slice should be
  demonstrable and tested before starting the next, and should map to an exit criterion in the
  roadmap.
