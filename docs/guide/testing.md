# Testing

Use `TestCluster.start()` from `@thresh/testing/test-cluster` for integration tests. It creates
multiple in-process silos with shared memory backends while retaining real routing, activation,
serialization, membership, and failure behavior.

Use `FakeTimeProvider` to advance collection, timeout, timer, and reminder behavior without sleeps.
Test activation loss and restart, duplicate delivery/idempotency, concurrent calls, remote routing,
provider failure, and graceful shutdown. Unit-test pure reducers separately. Run fast suites with
`pnpm test` and Orleans behavioral ports with `pnpm test:parity`.
